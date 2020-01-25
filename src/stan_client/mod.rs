use crate::nats_client::{NatsClient, NatsClientOptions, NatsSid, ClosableMessage};
use crate::nuid::NUID;

use std::{
    collections::HashMap,
    sync::Arc,
};
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;
use failure::_core::fmt::{Debug, Formatter, Error};

pub mod client;

// DefaultConnectWait is the default timeout used for the connect operation
//const DEFAULT_CONNECT_WAIT: u64 = 2 * 60000;
// DefaultDiscoverPrefix is the prefix subject used to connect to the NATS Streaming server
const DEFAULT_DISCOVER_PREFIX: &str = "_STAN.discover";
// DefaultACKPrefix is the prefix subject used to send ACKs to the NATS Streaming server
const DEFAULT_ACK_PREFIX: &str = "_STAN.acks";
// DefaultMaxPubAcksInflight is the default maximum number of published messages
// without outstanding ACKs from the server
const DEFAULT_MAX_PUB_ACKS_INFLIGHT: u32 = 16384;
// DefaultPingInterval is the default interval (in seconds) at which a connection sends a PING to the server
const DEFAULT_PING_INTERVAL: u32 = 5;
// DefaultPingMaxOut is the number of PINGs without a response before the connection is considered lost.
const DEFAULT_PING_MAX_OUT: u32 = 3;

// DefaultAckWait indicates how long the server should wait for an ACK before resending a message
const DEFAULT_ACK_WAIT: i32 = 30 * 60000;
// DefaultMaxInflight indicates how many messages with outstanding ACKs the server can send
const DEFAULT_MAX_INFLIGHT: i32 = 1024;

#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(setter(into), default)]
pub struct StanOptions {
    pub nats_options: NatsClientOptions,
    pub cluster_id: String,
    pub client_id: String,

    pub ping_interval: u32,
    pub ping_max_out: u32,
    pub max_pub_acks_inflight: u32,

    pub discover_prefix: String,
    pub ack_prefix: String,
}

#[derive(Debug, Clone)]
pub struct StanSid(pub(crate) NatsSid);


impl StanOptions {
    pub fn new<S>(cluster_id: S, client_id: S) -> StanOptions where S: ToString{
        StanOptions{
            client_id: client_id.to_string(),
            cluster_id: cluster_id.to_string(),
            ..Default::default()
        }
    }

    pub fn with_options<T, S>(nats_options: T, cluster_id: S, client_id: S) -> StanOptions
        where T: Into<NatsClientOptions> ,
            S: ToString{
        StanOptions{
            client_id: client_id.to_string(),
            cluster_id: cluster_id.to_string(),
            nats_options: nats_options.into(),
            ..Default::default()
        }
    }
    pub fn builder() -> StanOptionsBuilder {
        StanOptionsBuilder::default()
    }
}

impl Default for StanOptions {
    fn default() -> Self {
        StanOptions {
            nats_options: NatsClientOptions::default(),
            cluster_id: String::from(""),
            client_id: String::from(""),

            ping_interval: DEFAULT_PING_INTERVAL,
            ping_max_out: DEFAULT_PING_MAX_OUT,
            max_pub_acks_inflight: DEFAULT_MAX_PUB_ACKS_INFLIGHT,

            discover_prefix: DEFAULT_DISCOVER_PREFIX.into(),
            ack_prefix: DEFAULT_ACK_PREFIX.into(),
        }
    }
}

pub(crate) struct AckHandler(Box<dyn Fn() -> () + Send + Sync>);


impl Drop for AckHandler {
    fn drop(&mut self) {
        self.0()
    }
}

impl Debug for AckHandler {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.write_str("<ack-handler>")
    }
}

#[derive(Debug, Builder)]
#[builder(default)]
pub struct StanMessage {
    pub subject: String,
    pub reply_to: Option<String>,
    pub payload: Vec<u8>,
    pub timestamp: i64,
    pub sequence: u64,
    pub redelivered: bool,
    pub ack_inbox: Option<String>,
    #[builder(setter(skip))]
    ack_handler: Option<AckHandler>,
}

impl StanMessage {
    pub fn new(subject: String, payload: Vec<u8>) -> Self {
        StanMessage {
            subject,
            payload,
            reply_to: None,
            timestamp: 0,
            sequence: 0,
            redelivered: false,
            ack_inbox: None,
            ack_handler: None,
        }
    }

    pub fn with_reply(subject: String, payload: Vec<u8>, reply_to: Option<String>) -> Self {
        StanMessage {
            subject,
            payload,
            reply_to,
            timestamp: 0,
            sequence: 0,
            redelivered: false,
            ack_inbox: None,
            ack_handler: None,
        }
    }

    pub fn builder() -> StanMessageBuilder {
        StanMessageBuilder::default()
    }
}

impl Default for StanMessage {
    fn default() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now();
        let tstamp = now.duration_since(UNIX_EPOCH).unwrap();
        let tstamp_ms = tstamp.as_millis() as i64;
        StanMessage {
            subject: String::new(),
            reply_to: None,
            payload: Vec::new(),
            timestamp: tstamp_ms,
            sequence: 0,
            redelivered: false,
            ack_inbox: None,
            ack_handler: None,
        }
    }
}

impl Clone for StanMessage {
    fn clone(&self) -> Self {
        StanMessage {
            subject: self.subject.clone(),
            reply_to: self.reply_to.clone(),
            payload: self.payload.clone(),
            timestamp: self.timestamp,
            sequence: self.sequence,
            redelivered: self.redelivered,
            ack_inbox: self.ack_inbox.clone(),
            ack_handler: None,
        }
    }
}


#[derive(Clone, Debug, PartialEq)]
pub enum StartPosition {
    //Send only new messages
    NewOnly = 0,
    //Send only the last received message
    LastReceived = 1,
    //Send messages from duration specified in the startTimeDelta field.
    TimeDeltaStart = 2,
    //Send messages starting from the sequence in the startSequence field.
    SequenceStart = 3,
    //Send all available messages
    First = 4,
}

#[derive(Clone, Debug, PartialEq, Builder)]
#[builder(default)]
pub struct StanSubscribe {
    pub subject: String,
    pub queue_group: Option<String>,
    pub durable_name: Option<String>,
    pub max_in_flight: i32,
    pub ack_wait_in_secs: i32,
    pub start_position: StartPosition,
    pub start_sequence: u64,
    pub start_time_delta: Option<i32>,
    pub manual_acks: bool,
}

impl StanSubscribe {
    pub fn builder() -> StanSubscribeBuilder {
        StanSubscribeBuilder::default()
    }
}

impl Default for StanSubscribe {
    fn default() -> Self {
        StanSubscribe {
            subject: String::from(""),
            queue_group: None,
            durable_name: None,
            max_in_flight: DEFAULT_MAX_INFLIGHT,
            ack_wait_in_secs: DEFAULT_ACK_WAIT,
            start_position: StartPosition::LastReceived,
            start_sequence: 0,
            start_time_delta: None,
            manual_acks: false,
        }
    }
}

#[derive(Clone)]
struct Subscription {
    client_id: String,
    subject: String,
    queue_group: Option<String>,
    durable_name: Option<String>,
    max_in_flight: i32,
    ack_wait_in_secs: i32,
    inbox: String,
    ack_inbox: String,
    unsub_requests: String,
    close_requests: String,
    sender: UnboundedSender<ClosableMessage>,
}

pub struct StanClient {
    //subs_tx: Arc<RwLock<HashMap<String, Subscription>>>,
    pub options: StanOptions,
    pub nats_client: Arc<NatsClient>,
    pub client_id: String,

    client_info: Arc<RwLock<ClientInfo>>,
    id_generator: Arc<RwLock<NUID>>,
    conn_id: RwLock<Vec<u8>>,
    subscriptions: RwLock<HashMap<String, Subscription>>,
    self_reference: RwLock<Option<Arc<StanClient>>>,
}

#[derive(Clone, Debug, Default)]
pub struct ClientInfo {
    pub_prefix: String,
    sub_requests: String,
    unsub_requests: String,
    sub_close_requests: String,
    close_requests: String,
    ping_requests: String,
    public_key: String,
}
