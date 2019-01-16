use crate::nats_client::{NatsClient, NatsClientOptions};
use futures::{
    Future,
    sync::mpsc,
};
use nuid::NUID;
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::AtomicBool,
    },
};
mod client;
mod subscription;

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
#[builder(default)]
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

impl StanOptions {
    pub fn new(cluster_id: String, client_id: String) -> StanOptions {
        let mut options = StanOptions::default();
        options.client_id = client_id;
        options.cluster_id = cluster_id;
        options
    }

    pub fn with_options(nats_options: NatsClientOptions, cluster_id: String, client_id: String) -> StanOptions {
        let mut options = StanOptions::default();
        options.client_id = client_id;
        options.cluster_id = cluster_id;
        options.nats_options = nats_options;
        options
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

#[derive(Clone, Debug, PartialEq, Builder)]
#[builder(default)]
pub struct StanMessage {
    pub subject: String,
    pub reply_to: Option<String>,
    pub payload: Vec<u8>,
    pub timestamp: i64,
    pub sequence: u64,
    pub redelivered: bool,
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
        let tstamp_ms = tstamp.as_secs() as i64 * 1000 + i64::from(tstamp.subsec_millis());
        StanMessage {
            subject: String::new(),
            reply_to: None,
            payload: Vec::new(),
            timestamp: tstamp_ms,
            sequence: 0,
            redelivered: false,
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

pub struct Subscription {
    subscription_id: String,
    client_id: String,
    inbox: String,
    ack_inbox: String,
    nats_client: Arc<NatsClient>,
    unsub_requests: String,
    close_requests: String,
    is_closed: AtomicBool,
    unsub_tx: mpsc::UnboundedSender<String>,
    handler: Arc<SubscriptionHandler>,
    cmd: StanSubscribe,
}

pub struct StanClient {
    //subs_tx: Arc<RwLock<HashMap<String, Subscription>>>,
    pub options: StanOptions,

    pub nats_client: Arc<NatsClient>,
    pub client_id: String,

    pub client_info: Arc<RwLock<ClientInfo>>,

    pub_ack_map: Arc<RwLock<HashMap<String, u64>>>,
    id_generator: Arc<RwLock<NUID>>,
    conn_id: Arc<RwLock<Vec<u8>>>,

    subscriptions: Arc<RwLock<HashMap<String, Arc<Subscription>>>>,

    pub ping_max_out: i32,
    pub ping_interval: i32,

    pub protocol: i32,
    pub public_key: String,
    unsub_tx: mpsc::UnboundedSender<String>,
}

#[derive(Clone, Debug)]
pub struct ClientInfo {
    pub_prefix: String,
    sub_requests: String,
    unsub_requests: String,
    sub_close_requests: String,
    close_requests: String,
    ping_requests: String,
}

pub struct AsyncHandler(pub Box<Fn(StanMessage) -> Box<Future<Item=(), Error=()> + Send + Sync> + Send + Sync>);

pub struct SyncHandler(pub Box<Fn(StanMessage) -> Result<(), ()> + Send + Sync>);

pub type Handler = Fn(StanMessage, Arc<Subscription>, Arc<NatsClient>) -> Result<(), ()> + Send + Sync;

pub struct SubscriptionHandler(Box<Handler>);
