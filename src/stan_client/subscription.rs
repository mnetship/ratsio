use crate::error::RatsioError;
use crate::nats_client::{NatsClient, };
use crate::ops::{Message, Publish,};
use crate::protocol::{
    Ack, MsgProto, UnsubscribeRequest,
};
use futures::{
    prelude::*,
    future::{self, Either}, Future,
    stream::Stream,
};
use protobuf::{Message as ProtoMessage, parse_from_bytes};
use std::{
    sync::{
        Arc,
        atomic::Ordering,
    },
};
use super::*;

impl Subscription {
    pub(crate) fn start(self, stream: Box<dyn Stream<Item=Message> + Send + Sync>) -> Arc<Self> {
        let arc_self = Arc::new(self);
        let handler_subscr = arc_self.clone();
        let subs_nats_client = arc_self.nats_client.clone();

        let subs_future = stream
            .for_each(move |nats_msg| {
                debug!(target: "ratsio", "message => {:#?} ", &nats_msg);
                let msg = parse_from_bytes::<MsgProto>(&nats_msg.payload[..]).unwrap();
                let stan_msg = StanMessage {
                    subject: msg.subject,
                    reply_to: if !msg.reply.is_empty() { Some(msg.reply) } else { None },
                    payload: msg.data,
                    timestamp: msg.timestamp,
                    sequence: msg.sequence,
                    redelivered: msg.redelivered,
                };
                if handler_subscr.clone().is_closed.load(Ordering::Relaxed) {
                    future::ok(())
                } else {
                    let _ = handler_subscr.handler.0(stan_msg, handler_subscr.clone(), subs_nats_client.clone()).map(|_| {
                        trace!(target: "ratsio", "Message handler completed");
                    });
                    future::ok(())
                }
            })
            .map_err(|err| error!(target: "ratsio", " STAN stream error  -> {}", err));

        tokio::spawn(subs_future.into_future()
            .map(|_| {
                debug!(target: "ratsio", "done with subscription");
            })
            .map_err(|_| {
                debug!(target: "ratsio", "done with subscription with error  ");
            }));
        arc_self
    }

    ///
    /// The UnsubscribeRequest unsubcribes the connection from the specified subject.
    /// The inbox specified is the inbox returned from the NATS Streaming Server in the SubscriptionResponse.
    pub fn unsubscribe(&self) -> impl Future<Item=(), Error=()> {
        self.unsub(self.unsub_requests.clone())
    }

    ///
    /// The UnsubscribeRequest closes subcribtion for specified subject.
    /// The inbox specified is the inbox returned from the NATS Streaming Server in the SubscriptionResponse.
    pub fn close(&self) -> impl Future<Item=(), Error=()> {
        self.unsub(self.close_requests.clone())
    }

    fn unsub(&self, subject: String) -> impl Future<Item=(), Error=()> {
        let mut unsub_request = UnsubscribeRequest::new();
        unsub_request.set_clientID(self.client_id.clone());
        unsub_request.set_subject(self.cmd.subject.clone());
        unsub_request.set_inbox(self.inbox.clone());
        if let Some(durable_name) = self.cmd.durable_name.clone() {
            unsub_request.set_durableName(durable_name);
        }
        self.is_closed.store(true, Ordering::Relaxed);
        let buf = ProtoMessage::write_to_bytes(&unsub_request).unwrap();
        let unsub_tx = self.unsub_tx.clone();
        let subscription_id = self.subscription_id.clone();
        self.nats_client.request(subject.clone(), &buf[..])
            .map_err(|err| error!(target: "ratsio", " STAN Unsubscribe error => {}", err))
            .and_then(move |_| {
                info!(target: "ratsio", " STAN Unsubscribe for {} DONE", subject);
                unsub_tx.unbounded_send(subscription_id)
                    .into_future()
                    .map_err(|_| ())
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
}

impl Into<SubscriptionHandler> for SyncHandler {
    fn into(self) -> SubscriptionHandler {
        SubscriptionHandler(Box::new(
            move |stan_msg: StanMessage, subscr: Arc<Subscription>, _nats_client: Arc<NatsClient>| {
                let subject = stan_msg.subject.clone();
                let sequence = stan_msg.sequence;
                let manual_acks = subscr.cmd.manual_acks;
                let ack_inbox = subscr.ack_inbox.clone();
                let _ = (self.0)(stan_msg).map(move |_| {
                    if !manual_acks {
                        tokio::spawn(
                            subscr.ack_message(ack_inbox, subject, sequence)
                                .map_err(|err| error!(target: "ratsio", " STAN stream error -> {} ", err))
                        );
                    }
                });
                Ok(())
            }))
    }
}

impl Into<SubscriptionHandler> for AsyncHandler {
    fn into(self) -> SubscriptionHandler {
        SubscriptionHandler(Box::new(
            move |stan_message: StanMessage, subscr: Arc<Subscription>, nats_client: Arc<NatsClient>| {
                let ack_sequence = stan_message.sequence;
                let ack_subject = stan_message.subject.clone();
                let manual_acks = subscr.cmd.manual_acks;
                let ack_inbox = subscr.ack_inbox.clone();
                tokio::spawn((self.0)(stan_message)
                    .then(move |_| {
                        //stan_message.
                        if !manual_acks {
                            let mut ack_request = Ack::new();
                            ack_request.set_subject(ack_subject);
                            ack_request.set_sequence(ack_sequence);
                            let buf = ProtoMessage::write_to_bytes(&ack_request).unwrap();
                            Either::Left(nats_client
                                .publish(Publish::builder()
                                    .payload(Vec::from(&buf[..]))
                                    .subject(ack_inbox).build().unwrap())
                                .map_err(|err| {
                                    error!(" Error acknowledging message {}", err);
                                }))
                        } else {
                            Either::Right(future::ok(()))
                        }
                    }));
                Ok(())
            }))
    }
}
