use atomic_counter::AtomicCounter;
use atomic_counter::ConsistentCounter;

use crate::error::RatsioError;
use crate::net::*;
use crate::ops::{ Message, Op, Publish, Subscribe, UnSubscribe};
use futures::{
    future::{self, Either, Loop, loop_fn},
    Future,
    prelude::*,
    Stream,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
    },
};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::Arc,
};
use std::time::{Duration, Instant};
use tokio::timer::Delay;
use tokio::timer::Interval;

use super::*;

impl NatsClientMultiplexer {
    fn new(stream: NatsStream, subs_map: Arc<RwLock<HashMap<String, SubscriptionSink>>>,
           control_tx: mpsc::UnboundedSender<Op>) -> Self {
        let mltpx_subs_map = subs_map.clone();
        let control_tx2 = control_tx.clone();
        // Here we filter the incoming TCP stream Messages by subscription ID and sending it to the appropriate Sender
        let multiplexer_fut = stream
            .for_each(move |op| {
                match op {
                    Op::MSG(msg) => {
                        if let Some(s) = (*mltpx_subs_map.read()).get(&msg.sid) {
                            let _ = s.tx.unbounded_send(SinkMessage::Message(msg));
                        }
                    }
                    // Forward the rest of the messages to the owning client
                    op => {
                        let _ = control_tx2.clone().unbounded_send(op);
                    }
                }

                future::ok::<(), RatsioError>(())
            })
            .map(|_| ())
            .from_err();

        tokio::spawn(multiplexer_fut);

        NatsClientMultiplexer { subs_map, control_tx }
    }

    pub fn for_sid(&self, cmd: Subscribe) -> impl Stream<Item=Message, Error=RatsioError> + Send + Sync {
        let (tx, rx) = mpsc::unbounded();
        let sid = cmd.sid.clone();
        let subject = cmd.subject.clone();
        (*self.subs_map.write()).insert(
            sid.clone(),
            SubscriptionSink {
                cmd,
                tx,
                max_count: None,
                count: 0,
            },
        );

        rx.map_err(|_| {
            RatsioError::InnerBrokenChain
        }).take_while(move |sink_msg| {
            match sink_msg {
                SinkMessage::CLOSE => {
                    warn!(target:"ratsio", "Closing sink for => {} / {}", &sid, &subject);
                    Ok(false)
                },
                _ => Ok(true)
            }
        }).filter_map(|sink_msg| match sink_msg {
            SinkMessage::Message(msg) => Some(msg),
            _ => None,
        })
    }

    pub fn remove_sid(&self, sid: &str) {
        if (*self.subs_map.write()).remove(sid).is_some() {
            debug!(target: "ratsio", "Removing sid {}", &sid);
        }
    }
}


impl NatsClient {
    pub fn add_reconnect_handler(&self, hid: String,
                                 handler: Box<Fn(Arc<NatsClient>) -> () + Send + Sync>) {
        self.reconnect_handlers.write().insert(hid, handler);
    }

    pub fn remove_reconnect_handler(&self, hid: &str) {
        self.reconnect_handlers.write().remove(hid);
    }

    pub fn get_state(&self) -> NatsClientState {
        self.state.read().clone()
    }
    /// Creates a client and initiates a connection to the server
    ///
    /// Returns `impl Future<Item = Self, Error = RatsioError>`
    pub fn from_options(opts: NatsClientOptions) -> impl Future<Item=Arc<Self>, Error=RatsioError> + Send + Sync {
        loop_fn(opts, move |opts| {
            let cont_opts = opts.clone();
            NatsClient::create_client(opts)
                .and_then(move |client| {
                    Ok(Loop::Break(client))
                })
                .or_else(move |_err| {
                    if cont_opts.ensure_connect {
                        let when = Instant::now() + Duration::from_millis(cont_opts.reconnect_timeout);
                        Either::A(Delay::new(when)
                            .and_then(move |_| {
                                Ok(Loop::Continue(cont_opts))
                            }).map_err(|_| RatsioError::InnerBrokenChain))
                    } else {
                        Either::B(future::err(RatsioError::NoRouteToHostError))
                    }
                })
        })
    }
    /// Create nats client with options
    /// Called internally depending on the user options.
    fn create_client(opts: NatsClientOptions) -> impl Future<Item=Arc<Self>, Error=RatsioError> + Send + Sync {
        let tls_required = opts.connect.tls_required;
        let cluster_uris = opts.cluster_uris.clone();
        let recon_opts = opts.clone();
        let (reconnect_handler_tx, reconnect_handler_rx) = mpsc::unbounded();
        NatsConnection::create_connection(reconnect_handler_tx.clone(),
                                          opts.reconnect_timeout, cluster_uris, tls_required)
            .and_then(move |connection| {
                let connection = Arc::new(connection);
                let stream_conn = connection.clone();
                let ping_conn = connection.clone();
                let (sink, stream): (NatsSink, NatsStream) = NatsConnSinkStream {
                    inner: connection.inner.clone(),
                    state: connection.state.clone(),
                    reconnect_trigger: Box::new(move || {
                        NatsConnection::trigger_reconnect(stream_conn.clone());
                    }),
                }.split();

                let (control_tx, control_rx) = mpsc::unbounded();
                let subs_map: Arc<RwLock<HashMap<String, SubscriptionSink>>> =
                    Arc::new(RwLock::new(HashMap::default()));
                let recon_subs_map = subs_map.clone();

                let receiver = NatsClientMultiplexer::new(stream, subs_map.clone(), control_tx.clone());
                let sender = NatsClientSender::new(sink);

                let (unsub_tx, unsub_rx) = mpsc::unbounded();

                let ping_interval = u64::from(opts.ping_interval);
                let ping_max_out = usize::from(opts.ping_max_out);

                let client = Arc::new(NatsClient {
                    connection: connection.clone(),
                    sender: Arc::new(RwLock::new(sender)),
                    server_info: Arc::new(RwLock::new(None)),
                    unsub_receiver: Box::new(unsub_rx.map_err(|_| RatsioError::InnerBrokenChain)),
                    receiver: Arc::new(RwLock::new(receiver)),
                    control_tx: Arc::new(RwLock::new(control_tx)),
                    state: Arc::new(RwLock::new(NatsClientState::Connected)),
                    opts,
                    reconnect_handlers: Arc::new(RwLock::new(HashMap::default())),
                });

                let ping_client = client.clone();
                let ping_attempts = Arc::new(ConsistentCounter::new(0));
                let pong_reset = ping_attempts.clone();
                let recon_ping_attempts = ping_attempts.clone();
                NatsClient::control_receiver(control_rx, unsub_tx.clone(), client.clone(), pong_reset);


                //Send pings to server to check if we're still connected.
                tokio::spawn(Interval::new_interval(Duration::from_secs(ping_interval))
                    .for_each(move |_| {
                        if *ping_client.state.read() == NatsClientState::Connected {
                            trace!(target: "ratsio", " Send {:?}", Op::PING);
                            ping_client.sender.read().send(Op::PING);
                            let attempts = ping_attempts.inc();
                            if attempts >= 1 {
                                debug!(target: "ratsio", "Skipped a ping.");
                            }

                            if attempts > ping_max_out {
                                error!(target: "ratsio", "Pings are not responded to, we ma be down.");
                                *ping_client.state.write() = NatsClientState::Disconnected;
                                NatsConnection::trigger_reconnect(ping_conn.clone());
                            }
                        }
                        Ok(())
                    }).map_err(|_| ()));


                let recon_client = client.clone();
                tokio::spawn(reconnect_handler_rx.for_each(move |conn| {
                    *recon_client.state.write() = NatsClientState::Reconnecting;
                    if !recon_opts.subscribe_on_reconnect {
                        let _: Vec<_> = recon_subs_map.read().iter().map(|(_, sink)| {
                            let _ = sink.tx.unbounded_send(SinkMessage::CLOSE);
                            debug!(target:"ratsio", "Closing sink for => {:?}", &sink.cmd.subject);
                        }).collect();
                        recon_subs_map.write().clear();
                    }

                    let _ = recon_client.control_tx.read().unbounded_send(Op::CLOSE);
                    recon_ping_attempts.reset();
                    let stream_conn = conn.clone();
                    let (sink, stream): (NatsSink, NatsStream) = NatsConnSinkStream {
                        inner: conn.inner.clone(),
                        state: conn.state.clone(),
                        reconnect_trigger: Box::new(move || {
                            NatsConnection::trigger_reconnect(stream_conn.clone());
                        }),
                    }.split();

                    let (control_tx, control_rx) = mpsc::unbounded();
                    let receiver = NatsClientMultiplexer::new(stream, recon_subs_map.clone(), control_tx.clone());
                    let sender = NatsClientSender::new(sink);

                    NatsClient::control_receiver(control_rx, unsub_tx.clone(), recon_client.clone(),
                                                 recon_ping_attempts.clone());

                    *recon_client.sender.write() = sender;
                    *recon_client.receiver.write() = receiver;
                    *recon_client.control_tx.write() = control_tx;
                    *recon_client.state.write() = NatsClientState::Connected;

                    if recon_opts.subscribe_on_reconnect {
                        let subs_sender = recon_client.sender.clone();
                        let subs_fut_list: Vec<_> = recon_subs_map.read().iter().map(|(_, sink)| {
                            subs_sender.read()
                                .send(Op::SUB(sink.cmd.clone()))
                                .map_err(|err| {
                                    //TODO ----------
                                    error!(target: "ratsio", "Error re-subscribing {:?}", err);
                                })
                        }).collect();

                        tokio::spawn(future::join_all(subs_fut_list).map(|_| ()));
                    }

                    let cb_client = recon_client.clone();
                    recon_client.reconnect_handlers.read().iter()
                        .for_each(move |(_, handler)| {
                            (*handler)(cb_client.clone());
                        });
                    Ok(())
                }));
                future::ok(client)
            })
    }

    fn control_receiver(control_rx: UnboundedReceiver<Op>,
                        unsub_tx: UnboundedSender<Op>, client: Arc<NatsClient>,
                        pong_reset: Arc<ConsistentCounter>) {
        let control_fut = control_rx
            .take_while(|op| {
                match op {
                    Op::CLOSE => Ok(false),
                    _ => Ok(true),
                }
            })
            .for_each(move |op| {
                match op {
                    Op::PING => {
                        pong_reset.reset();
                        tokio::spawn(client.sender.read().send(Op::PONG)
                            .map(|_| {
                                debug!(target: "ratsio", "Sent {:?}", Op::PONG);
                            })
                            .map_err(|err| {
                                error!(target: "ratsio", "Error could not send pong to server: {:?}", err);
                            }));
                        let _ = unsub_tx.unbounded_send(op.clone());
                    }
                    Op::PONG => {
                        debug!(target: "ratsio", " Received {:?}", Op::PONG);
                        pong_reset.reset();
                    }
                    Op::INFO(server_info) => {
                        pong_reset.reset();
                        *client.server_info.write() = Some(server_info.clone());
                        let mut reconnect_hosts = server_info.connect_urls.clone();
                        for host in client.connection.init_hosts.clone() {
                            reconnect_hosts.push(host);
                        }
                        *client.connection.reconnect_hosts.write() = reconnect_hosts;

                    }
                    Op::ERR(msg) => {
                        error!(target: "ratsio", "NATS Server - Error - {}", msg);
                    }
                    Op::CLOSE => {
                        warn!(target: "ratsio", "Stream closed");
                    }
                    op => {
                        let _ = unsub_tx.unbounded_send(op.clone());
                        pong_reset.reset();
                    }
                };
                Ok(())
            })
            .into_future()
            .map(|_| ())
            .map_err(|_| ());
        tokio::spawn(control_fut);
    }

    /// Sends the CONNECT command to the server to setup connection
    ///
    /// Returns `impl Future<Item = Self, Error = RatsioError>`
    pub fn connect(client: &Arc<Self>) -> impl Future<Item=Arc<Self>, Error=RatsioError> + Send + Sync {
        let ret_client = client.clone();
        client.sender.read()
            .send(Op::CONNECT(client.opts.connect.clone()))
            .and_then(move |_| future::ok(ret_client))
    }


    /// Send a PUB command to the server
    ///
    /// Returns `impl Future<Item = (), Error = RatsioError>`
    pub fn publish(&self, cmd: Publish) -> impl Future<Item=(), Error=RatsioError> + Send + Sync {
        if let Some(ref server_info) = *self.server_info.read() {
            if cmd.payload.len() > server_info.max_payload {
                return Either::A(future::err(RatsioError::MaxPayloadOverflow(server_info.max_payload)));
            }
        }

        Either::B(self.sender.read().send(Op::PUB(cmd)))
    }

    /// Send a UNSUB command to the server and de-register stream in the multiplexer
    ///
    /// Returns `impl Future<Item = (), Error = RatsioError>`
    pub fn unsubscribe(&self, cmd: UnSubscribe) -> impl Future<Item=(), Error=RatsioError> + Send + Sync {
        if let Some(max) = cmd.max_msgs {
            if let Some(mut s) = (*self.receiver.read().subs_map.write()).get_mut(&cmd.sid) {
                s.max_count = Some(max);
            }
        }

        self.sender.read().send(Op::UNSUB(cmd))
    }

    /// Send a SUB command and register subscription stream in the multiplexer and return that `Stream` in a future
    ///
    /// Returns `impl Future<Item = impl Stream<Item = Message, Error = RatsioError>>`
    pub fn subscribe(
        &self,
        cmd: Subscribe,
    ) -> impl Future<Item=impl Stream<Item=Message, Error=RatsioError> + Send + Sync, Error=RatsioError> + Send + Sync
    {
        let receiver = self.receiver.clone();
        let subs_receiver = self.receiver.clone();
        let sid = cmd.sid.clone();
        debug!(target: "ratsio", "Subscription for {} / {}", &cmd.subject, &sid);
        let subs_cmd = cmd.clone();
        self.sender.read().send(Op::SUB(cmd))
            .and_then(move |_| {
                let stream = receiver.read()
                    .for_sid(subs_cmd)
                    .and_then(move |msg| {
                        let lock = subs_receiver.read();
                        let mut stx = lock.subs_map.write();
                        let mut delete = None;

                        if let Some(s) = stx.get_mut(&sid) {
                            if let Some(max_count) = s.max_count {
                                s.count += 1;
                                if s.count >= max_count {
                                    delete = Some(max_count);
                                }
                            }
                        }

                        if let Some(count) = delete.take() {
                            if stx.remove(&sid).is_some() {
                                debug!(target: "ratsio", "Deleting subscription for {}", &sid);
                            }
                            return Err(RatsioError::SubscriptionReachedMaxMsgs(count));
                        }
                        Ok(msg)
                    });

                future::ok(stream)
            })
    }

    /// Performs a request to the server following the Request/Reply pattern. Returns a future containing the MSG that will be replied at some point by a third party
    ///
    pub fn request(
        &self,
        subject: String,
        payload: &[u8],
    ) -> impl Future<Item=Message, Error=RatsioError> + Send + Sync {
        if let Some(ref server_info) = *self.server_info.read() {
            if payload.len() > server_info.max_payload {
                return Either::A(future::err(RatsioError::MaxPayloadOverflow(server_info.max_payload)));
            }
        }

        let inbox = Publish::generate_reply_to();
        let pub_cmd = Publish {
            subject,
            payload: Vec::from(&payload[..]),
            reply_to: Some(inbox.clone()),
        };

        let sub_cmd = Subscribe {
            queue_group: None,
            sid: Subscribe::generate_sid(),
            subject: inbox,
        };

        let sid = sub_cmd.sid.clone();

        let unsub_cmd = UnSubscribe {
            sid: sub_cmd.sid.clone(),
            max_msgs: Some(1),
        };

        let unsub_sender = self.sender.clone();
        let pub_sender = self.sender.clone();
        let receiver = self.receiver.clone();
        let stream = self.receiver.read()
            .for_sid(sub_cmd.clone())
            .take(1)
            .into_future()
            .map(move |(surely_message, _)| {
                let msg = surely_message.unwrap();
                receiver.read().remove_sid(&sid);
                msg
            }).map_err(|(e, _)| e);


        Either::B(
            self.sender.read()
                .send(Op::SUB(sub_cmd))
                .and_then(move |_| unsub_sender.read().send(Op::UNSUB(unsub_cmd)))
                .and_then(move |_| pub_sender.read().send(Op::PUB(pub_cmd)))
                .and_then(move |_| stream),
        )
    }
}
