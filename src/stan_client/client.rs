use crate::error::RatsioError;
use crate::nats_client::NatsClient;
use crate::nuid::NUID;
use crate::ops::{Publish, Subscribe};
use crate::protocol::{
    Ack, CloseRequest, ConnectRequest, ConnectResponse,
    PubMsg, SubscriptionRequest,
    SubscriptionResponse,
};
use futures::{
    future::{self, Either}, Future,
    stream::Stream,
    sync::mpsc,
};
use parking_lot::RwLock;
use protobuf::{Message as ProtoMessage, parse_from_bytes};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::AtomicBool,
    },
};
use super::*;

impl Into<ClientInfo> for ConnectResponse {
    fn into(self) -> ClientInfo {
        ClientInfo {
            pub_prefix: self.pubPrefix.clone(),
            sub_requests: self.subRequests.clone(),
            unsub_requests: self.unsubRequests.clone(),
            sub_close_requests: self.subCloseRequests.clone(),
            close_requests: self.closeRequests.clone(),
            ping_requests: self.pingRequests.clone(),
        }
    }
}


impl StanClient {
    pub fn from_options(options: StanOptions) -> impl Future<Item=Arc<Self>, Error=RatsioError> {
        let id_generator = Arc::new(RwLock::new({
            let mut id_gen = NUID::new();
            id_gen.randomize_prefix();
            id_gen
        }));
        let conn_id = id_generator.write().next();
        debug!(target: "ratsio", "Connection id => {}", &conn_id);
        let heartbeat_inbox: String = format!("_HB.{}", id_generator.write().next());
        let discover_subject: String = format!("{}.{}", DEFAULT_DISCOVER_PREFIX, options.cluster_id);
        let client_id = options.client_id.clone();

        let mut nats_options = options.nats_options.clone();
        nats_options.name = client_id.clone();
        nats_options.subscribe_on_reconnect = false;
        NatsClient::from_options(nats_options.clone())
            .and_then(|client| {
                debug!(target: "ratsio", "Connecting NATS client");
                NatsClient::connect(&client)
            })
            .and_then(move |nats_client: Arc<NatsClient>| {
                debug!(target: "ratsio", "Got NATS client");
                let mut connect_request = ConnectRequest::new();
                connect_request.set_clientID(client_id.clone());
                connect_request.set_connID(conn_id.clone().into_bytes());
                connect_request.set_heartbeatInbox(heartbeat_inbox.clone());

                debug!(target: "ratsio", "Connecting STAN Client");
                let connect_payload = ProtoMessage::write_to_bytes(&connect_request).unwrap();
                let (tx, rx) = mpsc::unbounded::<String>();

                debug!(target: "ratsio", "Subscibing to STAN Client heartbeats");
                StanClient::process_heartbeats(
                    id_generator.clone(), &conn_id, &client_id, &heartbeat_inbox, nats_client.clone());

                let recon_discover_subject = discover_subject.clone();
                debug!(target: "ratsio", "Issuing STAN join request");
                //TODO add a timeout for cases where the STAN server does not reply.
                nats_client.request(discover_subject, &connect_payload).map(move |response| {
                    let connect_response = parse_from_bytes::<ConnectResponse>(
                        &response.payload[..]).unwrap();
                    let client_info : ClientInfo= connect_response.clone().into();
                    let stan_client = Arc::new(StanClient {
                        //subs_tx: Arc::new(RwLock::new(HashMap::default())),
                        options: StanOptions {
                            nats_options,
                            ..options
                        },

                        nats_client,
                        client_id: client_id.clone(),
                        conn_id: Arc::new(RwLock::new(conn_id.into_bytes())),
                        client_info: Arc::new(RwLock::new(client_info)),

                        ping_max_out: connect_response.pingMaxOut,
                        ping_interval: connect_response.pingInterval,
                        protocol: connect_response.protocol,
                        public_key: connect_response.publicKey,

                        id_generator,

                        subscriptions: Arc::new(RwLock::new(HashMap::default())),
                        pub_ack_map: Arc::new(RwLock::new(HashMap::default())),
                        unsub_tx: tx,
                    });

                    // Unsubscribe channel receiver.
                    let unsub_cb_stan_client = stan_client.clone();
                    tokio::spawn(rx.for_each(move |sub_id| {
                        debug!(target: "ratsio", "unsubscribing => {} ", sub_id);
                        unsub_cb_stan_client.subscriptions.write().remove(&sub_id[..]);
                        Ok(())
                    }));

                    StanClient::register_reconnect_handler(stan_client.clone(),
                                                           recon_discover_subject.clone());

                    stan_client
                })
            })
    }

    fn register_reconnect_handler(stan_client: Arc<StanClient>, discover_subject: String) {
        let nats_client = stan_client.nats_client.clone();
        let client_id = stan_client.client_id.clone();

        nats_client.add_reconnect_handler(String::from("_STAN"), Box::new(move |nats_client| {
            //We may need to disconnect first ......
            let heartbeat_inbox: String = format!("_HB.{}", stan_client.id_generator.write().next());
            let mut close_request = CloseRequest::new();
            let close_requests_subject = stan_client.client_info.read().close_requests.clone();
            close_request.set_clientID(stan_client.client_id.clone());
            let buf = ProtoMessage::write_to_bytes(&close_request).unwrap();
            info!(target: "ratsio", " STAN Reconnecting closing old connection");
            let close_fut = nats_client.request(close_requests_subject.clone(), buf[..].into());

            let conn_id = stan_client.id_generator.write().next();
            *stan_client.conn_id.write() = conn_id.clone().into_bytes();

            let nats_client = stan_client.nats_client.clone();
            StanClient::process_heartbeats(
                stan_client.id_generator.clone(),
                &conn_id, &client_id,
                &heartbeat_inbox, nats_client.clone());

            let mut connect_request = ConnectRequest::new();
            connect_request.set_clientID(client_id.clone());
            connect_request.set_connID(conn_id.clone().into_bytes());
            connect_request.set_heartbeatInbox(heartbeat_inbox.clone());


            let print_subs = stan_client.subscriptions.read().clone();
            info!(target: "ratsio", " 1 STAN Reconnecting Subscriptions [{}]\n\n{:?}", print_subs.len(),
                  print_subs.iter().map(|(key, sub)| format!("{} =>  {}", key, sub.inbox)).collect::<Vec<_>>());


            let buf = ProtoMessage::write_to_bytes(&connect_request).unwrap();
            let recon_subs_stan_client = stan_client.clone();
            let recon_fut = stan_client.nats_client
                .request(discover_subject.clone(), &buf)
                .map_err(|_| ())
                .and_then(move |response| {
                    info!(target: "ratsio", " STAN Reconnecting Response => {:?}", &response);
                    let connect_response = parse_from_bytes::<ConnectResponse>(
                        &response.payload[..]).unwrap();

                    let client_info: ClientInfo = connect_response.into();
                    info!(target: "ratsio", " STAN Reconnecting ClientInfo => {:?}", &client_info);
                    *recon_subs_stan_client.client_info.write() = client_info;

                    let print_subs = recon_subs_stan_client.subscriptions.read().clone();
                    info!(target: "ratsio", " STAN Reconnecting Subscriptions [{}]\n\n{:?}", print_subs.len(),
                          print_subs.iter().map(|(key, sub)| format!("{} =>  {}", key, sub.inbox)).collect::<Vec<_>>());

                    let subs_fut_list = {
                        let subscriptions = recon_subs_stan_client.subscriptions.read();
                        subscriptions.iter().map(|(id, sub)| {
                            let subject = sub.cmd.subject.clone();
                            let err_subject = sub.cmd.subject.clone();
                            recon_subs_stan_client
                                .subscribe_inner(sub.cmd.clone(), id.clone(), sub.handler.clone())
                                .map(move |_| {
                                    debug!(target: "ratsio", "Subject re-subscribed to => {}", subject)
                                })
                                .or_else(move |err| {
                                    error!(target: "ratsio", "Error re-subscribing to => {} after reconnect {:?}", err_subject, err);
                                    Ok(())
                                })
                        }).collect::<Vec<_>>()
                    };
                    future::join_all(subs_fut_list).map(|_| ())
                });
            tokio::spawn(close_fut.then(|_| {
                info!(target: "ratsio", " STAN Reconnecting ...");
                recon_fut.map_err(|err| {
                    error!(target: "ratsio", "Error reconnecting to STAN: {:?}", err)
                }).map(|_| {
                    info!(target: "ratsio", " STAN Reconnecting Done, Ready!");
                })
            }));
        }));
    }

    fn process_heartbeats(id_generator: Arc<RwLock<NUID>>, conn_id: &str,
                          client_id: &str, heartbeat_inbox: &str,
                          nats_client: Arc<NatsClient>) {
        let hb_conn_id = conn_id.to_string();
        let hb_client_id = client_id.to_string();
        debug!(target: "ratsio", "Subscribing to heartbeat => {}", &heartbeat_inbox);
        let sub = Subscribe::builder().subject(heartbeat_inbox.to_string()).build().unwrap();
        tokio::spawn(nats_client.clone().subscribe(sub)
            .and_then(|stream| {
                stream
                    .for_each(move |msg| {
                        debug!(target: "ratsio", "HEARTBEAT {}", msg.subject);
                        if let Some(reply_to) = msg.reply_to {
                            let mut reply_msg = PubMsg::new();
                            reply_msg.set_clientID(hb_client_id.clone());
                            reply_msg.set_connID(hb_conn_id.clone().into_bytes());
                            reply_msg.set_subject(reply_to.clone());
                            reply_msg.set_data(Vec::new());
                            reply_msg.set_guid(id_generator.write().next());
                            let buf = ProtoMessage::write_to_bytes(&reply_msg).unwrap();
                            trace!(target: "ratsio", "HEARTBEAT -- reply_to {}", &reply_to, );
                            let reply_publish = Publish::builder()
                                .subject(reply_to.clone())
                                .payload(buf)
                                .build().unwrap();
                            Either::A(nats_client.publish(reply_publish)
                                .map(|_| {
                                    trace!(target: "ratsio", "HEARTBEAT -- heartbeat reply was sent");
                                })
                                .or_else(|err| {
                                    error!(target: "ratsio", "Error replying to heartbeat {:?}", err);
                                    Ok(())
                                }))
                        } else {
                            Either::B(future::ok(()))
                        }
                    })
            }).map_err(|err| {
            error!(target: "ratsio", "Error in heartbeat stream {:?}", err);
        }));
    }

    fn sub_request_payload(&self, subscribe: &StanSubscribe, inbox: &str) -> Vec<u8> {
        let mut sub_request = SubscriptionRequest::new();
        sub_request.set_clientID(self.client_id.clone());
        sub_request.set_subject(subscribe.subject.clone());
        if let Some(queue_group) = subscribe.queue_group.clone() {
            sub_request.set_qGroup(queue_group);
        }
        if let Some(durable_name) = subscribe.durable_name.clone() {
            sub_request.set_durableName(durable_name);
        }
        sub_request.set_ackWaitInSecs(subscribe.ack_wait_in_secs);
        sub_request.set_startSequence(subscribe.start_sequence);
        sub_request.set_maxInFlight(subscribe.max_in_flight);
        sub_request.set_startPosition(match subscribe.start_position {
            StartPosition::NewOnly => crate::protocol::StartPosition::NewOnly,
            StartPosition::LastReceived => crate::protocol::StartPosition::LastReceived,
            StartPosition::TimeDeltaStart => crate::protocol::StartPosition::TimeDeltaStart,
            StartPosition::SequenceStart => crate::protocol::StartPosition::SequenceStart,
            StartPosition::First => crate::protocol::StartPosition::First,
        });
        sub_request.set_inbox(inbox.to_string());
        ProtoMessage::write_to_bytes(&sub_request).unwrap()
    }

    pub fn get_subscription(&self, id: &str) -> Option<Arc<Subscription>> {
        self.subscriptions.read().get(id).cloned()
    }

    pub fn subscribe<T>(
        &self,
        subscribe: StanSubscribe,
        handler: T,
    ) -> impl Future<Item=String, Error=RatsioError>
        where T: Into<SubscriptionHandler> {
        let subscription_id = self.id_generator.write().next();
        self.subscribe_inner(subscribe, subscription_id, Arc::new(handler.into()))
    }

    fn subscribe_inner(
        &self,
        subscribe: StanSubscribe,
        subscription_id: String,
        handler: Arc<SubscriptionHandler>,
    ) -> impl Future<Item=String, Error=RatsioError> {
        let inbox: String = format!("_SUB.{}", self.id_generator.write().next());

        let subs_nats_client = self.nats_client.clone();
        let subs_client_id = self.client_id.clone();

        let unsub_requests = self.client_info.read().unsub_requests.clone();
        let close_requests = self.client_info.read().close_requests.clone();
        let subscriptions = self.subscriptions.clone();
        let unsub_tx = self.unsub_tx.clone();

        let payload = self.sub_request_payload(&subscribe, &inbox);
        self.nats_client.clone().request(self.client_info.read().sub_requests.clone(), &payload)
            .and_then(move |sub_response| {
                let sub_response = parse_from_bytes::<SubscriptionResponse>(
                    &sub_response.payload[..]).unwrap();

                let sub = Subscribe::builder().subject(inbox.clone()).build().unwrap();
                subs_nats_client.subscribe(sub).and_then(move |stream| {
                    let subscription = Subscription {
                        subscription_id: subscription_id.clone(),
                        client_id: subs_client_id,
                        inbox: inbox.clone(),
                        cmd: subscribe,
                        ack_inbox: sub_response.ackInbox.clone(),
                        nats_client: subs_nats_client,
                        unsub_requests,
                        close_requests,
                        is_closed: AtomicBool::new(false),
                        unsub_tx,
                        handler,
                    }.start(Box::new(stream));
                    subscriptions.write().insert(subscription_id.clone(), subscription.clone());
                    future::ok(subscription_id)
                })
            })
    }

    fn ack_message(&self, ack_inbox: String, subject: String, sequence: u64) -> impl Future<Item=(), Error=RatsioError> {
        let mut ack_request = Ack::new();
        ack_request.set_subject(subject);
        ack_request.set_sequence(sequence);
        let buf = ProtoMessage::write_to_bytes(&ack_request).unwrap();
        self.nats_client.publish(Publish::builder()
            .payload(Vec::from(&buf[..]))
            .subject(ack_inbox).build().unwrap())
    }

    /// Sends an OP to the server
    pub fn send(&self, message: StanMessage) -> impl Future<Item=(), Error=RatsioError> {
        let mut pub_msg = PubMsg::new();
        let mut hasher = Sha256::new();
        hasher.input(&message.payload[..]);
        pub_msg.set_sha256(Vec::from(&hasher.result()[..]));

        pub_msg.set_clientID(self.client_id.clone());
        pub_msg.set_subject(message.subject.clone());
        pub_msg.set_data(message.payload);
        if let Some(reply_to) = message.reply_to {
            pub_msg.set_reply(reply_to);
        }
        let conn_id = self.conn_id.read().clone();
        trace!(target: "ratsio", "Sending conn id {}", ::std::str::from_utf8(&conn_id[..]).unwrap());
        pub_msg.set_connID(conn_id);
        let guid = self.id_generator.write().next();
        pub_msg.set_guid(guid.clone());
        self.pub_ack_map.write().insert(guid, 0);

        let payload = ProtoMessage::write_to_bytes(&pub_msg).unwrap();
        let publ = Publish::builder()
            .subject(format!("{}.{}", self.client_info.read().pub_prefix, message.subject))
            .payload(payload).build().unwrap();
        trace!(target: "ratsio", "publishing to topic : {}", publ.subject);
        self.nats_client.publish(publ)
    }


    pub fn close(&self) -> impl Future<Item=(), Error=()> {
        let nats_client = self.nats_client.clone();
        let close_requests = self.client_info.read().close_requests.clone();
        let client_id = self.client_id.clone();

        let subscriptions = self.subscriptions.clone();

        let subs_futures = subscriptions.read().iter().map(|(_, s)| {
            let s = s.clone();
            s.close()
        }).collect::<Vec<_>>();
        future::join_all(subs_futures).map_err(|_| RatsioError::GenericError("Closing connection".into()))
            .and_then(move |_| {
                let mut close_request = CloseRequest::new();
                close_request.set_clientID(client_id);
                let buf = ProtoMessage::write_to_bytes(&close_request).unwrap();
                debug!(target: "ratsio", " STAN Shutting down ...");
                nats_client.request(close_requests.clone(), buf[..].into())
            })
            .map(|_| debug!(target: "ratsio", "STAN Shutting down - DONE "))
            .from_err()
    }
}

