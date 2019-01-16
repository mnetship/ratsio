use crate::error::RatsioError;
use crate::ops::Op;
use futures::{
    future::{self, Either, Loop, loop_fn},
    prelude::*,
};
use parking_lot::RwLock;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    str::FromStr,
    sync::Arc,
};
use super::{ReconnectHandler};
use super::connection_inner::NatsConnectionInner;
use url::Url;

/// State of the raw connection
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NatsConnectionState {
    Connected,
    Reconnecting,
    Disconnected,
}

/// Represents a connection to a NATS server. Implements `Sink` and `Stream`
#[derive(Debug)]
pub struct NatsConnection {
    /// indicates if the connection is made over TLS
    pub(crate) is_tls: bool,
    /// Inner dual `Stream`/`Sink` of the TCP connection
    pub(crate) inner: Arc<RwLock<NatsConnectionInner>>,
    /// Current state of the connection, and connect version.
    /// Version only increments on a successful reconnect.
    pub(crate) state: Arc<RwLock<(NatsConnectionState, u64)>>,

    /// Reconnect trigger
    pub(crate) reconnect_handler: ReconnectHandler,

    pub(crate) init_hosts: Vec<String>,
    pub(crate) reconnect_hosts: RwLock<Vec<String>>,
    pub(crate) reconnect_timeout: u64,
}

pub struct NatsConnSinkStream {
   /// Inner dual `Stream`/`Sink` of the TCP connection
    pub(crate) inner: Arc<RwLock<NatsConnectionInner>>,
    /// Current state of the connection, and connect version.
    /// Version only increments on a successful reconnect.
    pub(crate) state: Arc<RwLock<(NatsConnectionState, u64)>>,

    /// Reconnect trigger
    pub(crate) reconnect_trigger: Box<Fn() -> () + Sync + Send>,
}


impl NatsConnection {
    /// Connect to a raw TCP socket
    fn connect(addr: SocketAddr) -> impl Future<Item=NatsConnectionInner, Error=RatsioError> {
        NatsConnectionInner::connect_tcp(&addr).map(move |socket| {
            socket.into()
        })
    }

    /// Connect to a TLS over TCP socket. Upgrade is performed automatically
    fn connect_tls(host: String, addr: SocketAddr) -> impl Future<Item=NatsConnectionInner, Error=RatsioError> {
        NatsConnectionInner::connect_tcp(&addr)
            .and_then(move |socket| {
                NatsConnectionInner::upgrade_tcp_to_tls(&host, socket)
            })
            .map(move |socket| {
                socket.into()
            })
    }

    /// Tries to reconnect once to the server; Only used internally. Blocks polling during reconnecting
    /// by forcing the object to return `Async::NotReady`/`AsyncSink::NotReady`
    pub(crate) fn trigger_reconnect(conn: Arc<Self>) {
        trace!(target:"ratsio", "Trigger reconnection ");
        let connect_version = conn.state.read().1;
        {
            let mut state_guard = conn.state.write();
            if state_guard.0 == NatsConnectionState::Reconnecting {
                // Another thread is busy reconnecting...
                trace!(target:"ratsio", "Already reconnection, nothing to do");
                return;
            } else if state_guard.0 == NatsConnectionState::Connected && state_guard.1 > connect_version {
                // Another thread has already reconnected ...
                trace!(target:"ratsio", "Another thread has reconnected, nothing to do");
                return;
            } else {
                let current_version = state_guard.1;
                *state_guard = (NatsConnectionState::Disconnected, current_version);
            }
        }
        NatsConnection::reconnect(conn);
    }

    fn reconnect(conn: Arc<Self>) {
        trace!(target:"ratsio", "Reconnecting");
        {
            let mut state_guard = conn.state.write();
            if state_guard.0 == NatsConnectionState::Disconnected {
                *state_guard = (NatsConnectionState::Reconnecting, state_guard.1);
            } else {
                return;
            }
        }

        let cluster_addrs: Vec<(String, SocketAddr)> = conn.reconnect_hosts.read().clone().into_iter().map(|cluster_uri| {
            if let Ok(sock_addr) = SocketAddr::from_str(&cluster_uri[..]) {
                vec!((cluster_uri.clone(), sock_addr))
            } else if let Ok(ips_iter) = cluster_uri.to_socket_addrs() {
                ips_iter.map(|x| (cluster_uri.clone(), x)).collect::<Vec<_>>()
            } else {
                vec!()
            }
        }).flatten().collect();
        trace!(target:"ratsio", "Retrying {:?}", &*conn.reconnect_hosts.read() );


        tokio::spawn(NatsConnection::get_conn_inner(cluster_addrs, conn.is_tls)
            .then(move |inner_result| {
                let connect_version = (*conn.state.read()).1;
                let retry_conn = conn.clone();
                match inner_result {
                    Ok(new_inner) => {
                        *conn.inner.write() = new_inner;
                        *conn.state.write() = (NatsConnectionState::Connected, connect_version + 1);
                        let _ = conn.reconnect_handler.unbounded_send(conn.clone());
                        debug!(target:"ratsio", "Got a connection");
                        Either::A(future::ok(()))
                    }
                    Err(err) => {
                        error!(target:"ratsio", "Error reconnecting :: {:?}", err);
                        *retry_conn.state.write() = (NatsConnectionState::Disconnected, connect_version);
                        //Rescedule another attempt
                        use tokio::timer::Delay;
                        use std::time::{Instant, Duration};

                        let when = Instant::now() + Duration::from_millis(retry_conn.reconnect_timeout);
                        let task = Delay::new(when)
                            .and_then(move |_| {
                                NatsConnection::trigger_reconnect(retry_conn);
                                Ok(())
                            })
                            .map_err(|err| error!(target:"ratsio", "Error scheduling reconnect attempt {:?}", err));
                        Either::B(task)
                    }
                }
            }));
    }

    pub fn create_connection(reconnect_handler: ReconnectHandler, reconnect_timeout: u64,
                             cluster_uris: Vec<String>, tls_required: bool) -> impl Future<Item=NatsConnection, Error=RatsioError> {
        let cluster_addrs: Vec<(String, SocketAddr)> = cluster_uris.clone().into_iter().map(|cluster_uri| {
            if let Ok(sock_addr) = SocketAddr::from_str(&cluster_uri[..]) {
                vec!((cluster_uri.clone(), sock_addr))
            } else if let Ok(ips_iter) = cluster_uri.to_socket_addrs() {
                ips_iter.map(|x| (cluster_uri.clone(), x)).collect::<Vec<_>>()
            } else {
                vec!()
            }
        }).flatten().collect();

        NatsConnection::get_conn_inner(cluster_addrs, tls_required)
            .map(move |inner| {
                NatsConnection {
                    is_tls: tls_required,
                    state: Arc::new(RwLock::new((NatsConnectionState::Connected, 0))),
                    inner: Arc::new(RwLock::new(inner)),
                    init_hosts: cluster_uris.clone(),
                    reconnect_hosts: RwLock::new(cluster_uris),
                    reconnect_handler,
                    reconnect_timeout,
                }
            })
    }


    fn get_conn_inner(cluster_addrs: Vec<(String, SocketAddr)>, tls_required: bool)
                      -> impl Future<Item=NatsConnectionInner, Error=RatsioError> {
        fn get_conn_step(cluster_addrs: &[(String, SocketAddr)], tls_required: bool)
                         -> impl Future<Item=NatsConnectionInner, Error=RatsioError> {
            if cluster_addrs.is_empty() {
                Either::A(future::err::<NatsConnectionInner, RatsioError>(RatsioError::NoRouteToHostError))
            } else {
                Either::B(future::ok(cluster_addrs[0].clone())
                    .and_then(move |(cluster_uri, cluster_sa)| {
                        if tls_required {
                            match Url::parse(&cluster_uri) {
                                Ok(url) => match url.host_str() {
                                    Some(host) => future::ok(Either::B(NatsConnection::connect_tls(host.to_string(), cluster_sa))),
                                    None => future::err(RatsioError::NoRouteToHostError),
                                },
                                Err(e) => future::err(e.into()),
                            }
                        } else {
                            future::ok(Either::A(NatsConnection::connect(cluster_sa)))
                        }
                    })
                    .flatten())
            }
        }

        loop_fn(cluster_addrs,
                move |cluster_addrs| {
                    let rem_addrs = Vec::from(&cluster_addrs[1..]).clone();
                    get_conn_step(&cluster_addrs[..], tls_required)
                        .and_then(move |inner| {
                            Ok(Loop::Break(inner))
                        })
                        .or_else(move |_err| {
                            if rem_addrs.is_empty() {
                                Err(RatsioError::NoRouteToHostError)
                            } else {
                                Ok(Loop::Continue(rem_addrs))
                            }
                        })
                })
    }
}

impl Sink for NatsConnSinkStream {
    type SinkError = RatsioError;
    type SinkItem = Op;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if match self.state.try_read() {
            Some(state) => (*state).0 != NatsConnectionState::Connected,
            _ => true,
        } {
            return Ok(AsyncSink::NotReady(item));
        }

        if let Some(mut inner) = self.inner.try_write() {
            match inner.start_send(item.clone()) {
                Err(RatsioError::ServerDisconnected(_)) => {
                    (*self.reconnect_trigger)();
                    Ok(AsyncSink::NotReady(item))
                }
                poll_res => poll_res,
            }
        } else {
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if match self.state.try_read() {
            Some(state) => (*state).0 != NatsConnectionState::Connected,
            _ => true,
        } {
            return Ok(Async::NotReady);
        }

        if let Some(mut inner) = self.inner.try_write() {
            match inner.poll_complete() {
                Err(RatsioError::ServerDisconnected(_)) => {
                    (*self.reconnect_trigger)();
                    Ok(Async::NotReady)
                }
                poll_res => poll_res,
            }
        } else {
            Ok(Async::NotReady)
        }
    }
}

impl Stream for NatsConnSinkStream {
    type Error = RatsioError;
    type Item = Op;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if match self.state.try_read() {
            Some(state) => (*state).0 != NatsConnectionState::Connected,
            _ => true,
        } {
            return Ok(Async::NotReady);
        }

        if let Some(mut inner) = self.inner.try_write() {
            match inner.poll() {
                Err(RatsioError::ServerDisconnected(_)) => {
                    (*self.reconnect_trigger)();
                    Ok(Async::NotReady)
                }
                poll_res => poll_res,
            }
        } else {
            Ok(Async::NotReady)
        }
    }
}
