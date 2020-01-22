use crate::error::RatsioError;
use crate::ops::Op;
use futures::{
    future::{self, Either},
    prelude::*,
};
use parking_lot::RwLock;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    str::FromStr,
    sync::Arc,
};
use super::connection_inner::NatsConnectionInner;
use super::ReconnectHandler;
use url::Url;
use failure::_core::task::{Context, Poll};
use failure::_core::pin::Pin;
use futures::executor::LocalPool;
use futures::task::{LocalSpawnExt, SpawnExt};
use async_std::task;


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
    pub(crate) inner: Arc<RwLock<(Url, NatsConnectionInner)>>,
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
    pub(crate) inner: Arc<RwLock<(Url, NatsConnectionInner)>>,
    /// Current state of the connection, and connect version.
    /// Version only increments on a successful reconnect.
    pub(crate) state: Arc<RwLock<(NatsConnectionState, u64)>>,

    /// Reconnect trigger
    pub(crate) reconnect_trigger: Box<Fn() -> () + Sync + Send>,
}


impl NatsConnection {
    /// Connect to a raw TCP socket
    async fn connect(addr: SocketAddr) -> Result<NatsConnectionInner, RatsioError> {
        let socket = NatsConnectionInner::connect_tcp(&addr).await?;
        debug!(target: "ratsio", "Got a socket successfully.");
        Ok(socket.into())
    }

    /// Connect to a TLS over TCP socket. Upgrade is performed automatically
    async fn connect_tls(host: String, addr: SocketAddr) -> Result<NatsConnectionInner, RatsioError> {
        let socket = NatsConnectionInner::connect_tcp(&addr).await?;
        let upgraded_socke = NatsConnectionInner::upgrade_tcp_to_tls(&host, socket).await?;
        Ok(socket.into())
    }

    /// Tries to reconnect once to the server; Only used internally. Blocks polling during reconnecting
    /// by forcing the object to return `Async::NotReady`/`AsyncSink::NotReady`
    pub(crate) fn trigger_reconnect(conn: Arc<Self>) {
        trace!(target: "ratsio", "Trigger reconnection ");
        let connect_version = conn.state.read().1;
        {
            let mut state_guard = conn.state.write();
            if state_guard.0 == NatsConnectionState::Reconnecting {
                // Another thread is busy reconnecting...
                trace!(target: "ratsio", "Already reconnection, nothing to do");
                return;
            } else if state_guard.0 == NatsConnectionState::Connected && state_guard.1 > connect_version {
                // Another thread has already reconnected ...
                trace!(target: "ratsio", "Another thread has reconnected, nothing to do");
                return;
            } else {
                let current_version = state_guard.1;
                *state_guard = (NatsConnectionState::Disconnected, current_version);
            }
        }
        NatsConnection::reconnect(conn);
    }

    fn reconnect(conn: Arc<Self>) {
        trace!(target: "ratsio", "Reconnecting");
        {
            let mut state_guard = conn.state.write();
            if state_guard.0 == NatsConnectionState::Disconnected {
                *state_guard = (NatsConnectionState::Reconnecting, state_guard.1);
            } else {
                return;
            }
        }

        let cluster_addrs: Vec<_> = NatsConnection::parse_uris(&conn.reconnect_hosts.read());
        trace!(target: "ratsio", "Retrying {:?}", &*conn.reconnect_hosts.read());

        let mut executor = LocalPool::new();
        let spawner = executor.spawner();
        spawner.spawn(async {
            let inner_result = NatsConnection::get_conn_inner(cluster_addrs, conn.is_tls).await;

            let connect_version = (*conn.state.read()).1;
            let retry_conn = conn.clone();
            match inner_result {
                Ok(new_inner) => {
                    *conn.inner.write() = new_inner;
                    *conn.state.write() = (NatsConnectionState::Connected, connect_version + 1);
                    let _ = conn.reconnect_handler.unbounded_send(conn.clone());
                    debug!(target: "ratsio", "Got a connection");
                    Ok(())
                }
                Err(err) => {
                    error!(target: "ratsio", "Error reconnecting :: {:?}", err);
                    *retry_conn.state.write() = (NatsConnectionState::Disconnected, connect_version);
                    //Rescedule another attempt
                    use std::time::{Instant, Duration};
                    let _ = task::sleep(Duration::from_millis(retry_conn.reconnect_timeout)).await;
                    NatsConnection::trigger_reconnect(retry_conn);
                    Ok(())
                }
            };
        });
    }

    pub async fn create_connection(reconnect_handler: ReconnectHandler, reconnect_timeout: u64,
                                   cluster_uris: &[String], tls_required: bool) -> Result<NatsConnection, RatsioError> {
        let cluster_addrs = NatsConnection::parse_uris(cluster_uris);
        let init_hosts = cluster_uris.to_vec();
        let inner = NatsConnection::get_conn_inner(cluster_addrs, tls_required).await?;
        Ok(NatsConnection {
            is_tls: tls_required,
            state: Arc::new(RwLock::new((NatsConnectionState::Connected, 0))),
            inner: Arc::new(RwLock::new(inner)),
            init_hosts: init_hosts.clone(),
            reconnect_hosts: RwLock::new(init_hosts),
            reconnect_handler,
            reconnect_timeout,
        })
    }

    pub fn parse_uris(cluster_uris: &[String]) -> Vec<(Url, SocketAddr)> {
        cluster_uris.iter().map(|cluster_uri| {
            let formatted_url = if cluster_uri.starts_with("nats://") {
                cluster_uri.clone()
            } else {
                format!("nats://{}", cluster_uri)
            };
            let node_url = Url::parse(&formatted_url);
            match node_url {
                Ok(node_url) =>
                    match node_url.host_str() {
                        Some(host) => {
                            let host_and_port = format!("{}:{}", &host, node_url.port().unwrap_or(4222));
                            match SocketAddr::from_str(&host_and_port) {
                                Ok(sock_addr) => {
                                    info!(" Resolved {} to {}", &host, &sock_addr);
                                    vec!((node_url.clone(), sock_addr))
                                }
                                Err(_) => {
                                    match host_and_port.to_socket_addrs() {
                                        Ok(ips_iter) => ips_iter.map(|x| {
                                            info!(" Resolved {} to {}", &host, &x);
                                            (node_url.clone(), x)
                                        }).collect::<Vec<_>>(),
                                        Err(err) => {
                                            error!("Unable resolve url => {} to ip address => {}", cluster_uri, err);
                                            Vec::new()
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            Vec::new()
                        }
                    }
                Err(err) => {
                    error!("Unable to parse url => {} => {}", cluster_uri, err);
                    Vec::new()
                }
            }
        }).flatten().collect()
    }


    async fn get_conn_inner(cluster_addrs: Vec<(Url, SocketAddr)>, tls_required: bool)
                            -> Result<(Url, NatsConnectionInner), RatsioError> {
        if cluster_addrs.is_empty() {
            warn!("No addresses to connect to.");
            return Err(RatsioError::NoRouteToHostError);
        }
        async fn get_conn_step(cluster_addrs: &[(Url, SocketAddr)], tls_required: bool)
                               -> Result<(Url, NatsConnectionInner), RatsioError> {
            if cluster_addrs.is_empty() {
                Err(RatsioError::NoRouteToHostError)
            } else {
                let (node_url, node_addr) = cluster_addrs[0].clone();
                if tls_required {
                    match node_url.host_str() {
                        Some(host) => {
                            let conn = NatsConnection::connect_tls(host.to_string(), node_addr).await?;
                            Ok((node_url.clone(), conn))
                        }
                        None => Err(RatsioError::NoRouteToHostError),
                    }
                } else {
                    let conn = NatsConnection::connect(node_addr).await?;
                    Ok((node_url.clone(), conn))
                }
            }
        }
        let mut cluster_addrs = cluster_addrs;
        loop {
            match get_conn_step(&cluster_addrs[..], tls_required).await {
                Ok(inner) => {
                    return Ok(inner);
                }
                Err(err) => {
                    let rem_addrs = Vec::from(&cluster_addrs[1..]).clone();
                    if rem_addrs.is_empty() {
                        return Err(RatsioError::NoRouteToHostError);
                    } else {
                        cluster_addrs = rem_addrs
                    }
                }
            }
        }
    }
}

impl Sink<Op> for NatsConnSinkStream {
    type Error = RatsioError;

    fn start_send(&mut self, item: Op) -> Result<(), Self::Error> {
        if match self.state.try_read() {
            Some(state) => (*state).0 != NatsConnectionState::Connected,
            _ => true,
        } {
            return Err(RatsioError::IOError("Not connected".into()));
        }

        if let Some(mut inner) = self.inner.try_write() {
            match (*inner).1.start_send(item.clone()) {
                Err(RatsioError::ServerDisconnected(_)) => {
                    (*self.reconnect_trigger)();
                    Ok(())
                }
                poll_res => poll_res,
            }
        } else {
            Err(RatsioError::IOError("Not connected".into()))
        }
    }

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.state.try_read() {
            Some(state) =>
                if (*state).0 != NatsConnectionState::Connected {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            _ =>
                Poll::Poll::Pending
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if let Some(mut inner) = self.inner.try_write() {
            match (*inner).1.poll_flush() {
                Err(RatsioError::ServerDisconnected(_)) => {
                    (*self.reconnect_trigger)();
                    Poll::Pending
                }
                poll_res => poll_res,
            }
        } else {
            Poll::Pending
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if let Some(mut inner) = self.inner.try_write() {
            match (*inner).1.poll_close() {
                Err(RatsioError::ServerDisconnected(_)) => {
                    (*self.reconnect_trigger)();
                    Poll::Pending
                }
                poll_res => poll_res,
            }
        } else {
            Poll::Pending
        }
    }
}

impl Stream for NatsConnSinkStream {
    type Item = Op;


    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if match self.state.try_read() {
            Some(state) => (*state).0 != NatsConnectionState::Connected,
            _ => true,
        } {
            return Poll::Pending;
        }

        if let Some(mut inner) = self.inner.try_write() {
            match (*inner).1.poll_next() {
                Err(RatsioError::ServerDisconnected(_)) => {
                    (*self.reconnect_trigger)();
                    Poll::Pending
                }
                poll_res => poll_res,
            }
        } else {
            Poll::Pending
        }
    }
}
