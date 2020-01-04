use crate::error::RatsioError;
use crate::net::*;
use crate::ops::{Connect, Message, Op, ServerInfo, Subscribe};
use futures::{
    prelude::*,
    stream,
    task::Poll,
    channel::mpsc::{self, UnboundedSender},
    Future, Stream,
};
use parking_lot::RwLock;
use std::fmt::Debug;
use std::{collections::HashMap, sync::Arc};

type NatsSink = stream::SplitSink<NatsConnSinkStream, Op>;
type NatsStream = stream::SplitStream<NatsConnSinkStream>;

mod client;

#[derive(Clone, Debug)]
pub struct NatsClientSender {
    tx: UnboundedSender<Op>,
}

impl NatsClientSender {
    fn new(sink: NatsSink) -> Self {
        let (tx, rx) = mpsc::unbounded::<Op>();
        let work = sink.send_all(&mut rx);
        tokio::spawn(work);

        NatsClientSender { tx }
    }
    /// Sends an OP to the server
    pub fn send(&self, op: Op) -> impl Future<Output = ()> {
        //let _verbose = self.verbose.clone();
        self.tx
            .unbounded_send(op)
            .map_err(|_| RatsioError::InnerBrokenChain)
            .into_future()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SinkMessage {
    Message(Message),
    CLOSE,
}

#[derive(Debug, Clone)]
pub(crate) struct SubscriptionSink {
    cmd: Subscribe,
    tx: mpsc::UnboundedSender<SinkMessage>,
    max_count: Option<u32>,
    count: u32,
}

#[derive(Debug)]
pub struct NatsClientMultiplexer {
    control_tx: mpsc::UnboundedSender<Op>,
    subs_map: Arc<RwLock<HashMap<String, SubscriptionSink>>>,
}

/// UriVec allows ergonomic use of NatsClientOptions.
/// ``` rust
/// ratsio::prelude::NatsClientOptions::builder()
///    .cluster_uris("localhost:4222")
///    .build();
/// ```
/// or
/// ``` rust
/// ratsio::prelude::NatsClientOptions::builder()
///    .cluster_uris(vec!("localhost:4222", "other_location:4222"))
///    .build();
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct UriVec(Vec<String>);

impl From<Vec<&str>> for UriVec {
    fn from(xs: Vec<&str>) -> Self {
        UriVec(xs.into_iter().map(|x| x.into()).collect())
    }
}

impl From<Vec<String>> for UriVec {
    fn from(xs: Vec<String>) -> Self {
        UriVec(xs)
    }
}

impl From<String> for UriVec {
    fn from(x: String) -> Self {
        UriVec(vec![x])
    }
}

impl From<&str> for UriVec {
    fn from(x: &str) -> Self {
        UriVec(vec![x.to_owned()])
    }
}

/// An alias representing the requirements for the nonce signing callback function
pub type SignerCallback =
    Arc<dyn Fn(&[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> + Send + Sync>;

/// An option that indicates client JWT authentication should be used. Takes a callback that
/// will be used to sign the nonce the server supplies. For security reasons, ensure that
/// you keep the seed in memory only as long as is necessary. Because of the tokio wrappings
/// used by this client, the callback must be wrapped in an Arc of the signer callback function type.
#[derive(Clone)]
pub struct UserJWT {
    jwt: String,
    signer: SignerCallback,
}

impl Debug for UserJWT {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "UserJWT {{ jwt: {}, signer: (func) }}", self.jwt)
    }
}

impl PartialEq for UserJWT {
    fn eq(&self, other: &UserJWT) -> bool {
        self.jwt == other.jwt
    }
}

impl UserJWT {
    /// Creates a new UserJWT option from an encoded JWT and a callback to be invoked to sign
    /// the server-provided nonce
    pub fn new(jwt: String, signer: SignerCallback) -> UserJWT {
        UserJWT { jwt, signer }
    }
}

/// Options that are to be given to the client for initialization
#[derive(Debug, Clone, Builder, PartialEq)]
#[builder(setter(into), default)]
pub struct NatsClientOptions {
    /// Cluster username, can be overwritten by host url nats://<username>:<password>@<host>:<port>
    pub username: String,
    /// Cluster password, can be overwritten by host url nats://<username>:<password>@<host>:<port>
    pub password: String,
    /// Cluster auth_token
    pub auth_token: String,
    /// Whether TLS is required.
    pub tls_required: bool,
    /// verbosity, default true
    pub verbose: bool,
    /// pedantic, default false
    pub pedantic: bool,
    /// pedantic, default true
    pub echo: bool,
    /// Optional client name
    pub name: String,

    /// Cluster URI in the IP:PORT format
    pub cluster_uris: UriVec,

    /// Ping interfval in seconds
    pub ping_interval: u16,
    /// No of unsuccessful pings before the connection is deemed disconnected.
    pub ping_max_out: u16,
    /// If we should re-subscribe all subscriptions on re-connection.
    /// If you don't want re-subscription, add a reconnect_handler and do your thing there.
    pub subscribe_on_reconnect: bool,
    /// If connect fails, keep trying, forever,
    pub ensure_connect: bool,
    /// Time between connection retries
    pub reconnect_timeout: u64,
    /// When using NATS 2.x decentralized security, supply a user JWT for authN/authZ
    pub user_jwt: Option<UserJWT>,
}

impl Default for NatsClientOptions {
    fn default() -> Self {
        NatsClientOptions {
            username: String::new(),
            password: String::new(),
            tls_required: false,
            auth_token: String::new(),
            verbose: true,
            pedantic: false,
            echo: true,
            name: String::new(),
            cluster_uris: UriVec(Vec::new()),
            ping_interval: 5,
            ping_max_out: 3,
            subscribe_on_reconnect: true,
            ensure_connect: true,
            reconnect_timeout: 1000,
            user_jwt: None,
        }
    }
}

impl NatsClientOptions {
    pub fn builder() -> NatsClientOptionsBuilder {
        NatsClientOptionsBuilder::default()
    }
}

#[derive(PartialEq, Clone, Debug)]
pub enum NatsClientState {
    Connecting,
    Connected,
    Reconnecting,
    Disconnected,
}

type HandlerMap = HashMap<String, Box<dyn Fn(Arc<NatsClient>) -> () + Send + Sync>>;

/// The NATS Client. What you'll be using mostly. All the async handling is made internally except for
/// the system messages that are forwarded on the `Stream` that the client implements
pub struct NatsClient {
    connection: Arc<NatsConnection>,

    /// Backup of options
    opts: NatsClientOptions,
    /// Server info
    server_info: Arc<RwLock<Option<ServerInfo>>>,
    /// Stream of the messages that are not caught for subscriptions (only system messages like PING/PONG should be here)
    unsub_receiver: Box<dyn Stream<Item = Result<Op, RatsioError>> + Send + Sync>,
    /// Sink part to send commands
    pub sender: Arc<RwLock<NatsClientSender>>,
    /// Subscription multiplexer
    pub receiver: Arc<RwLock<NatsClientMultiplexer>>,

    /// For control Ops (PING, PONG, CLOSE, SERVER_INFO) and misc operations.
    control_tx: Arc<RwLock<UnboundedSender<Op>>>,

    state: Arc<RwLock<NatsClientState>>,
    reconnect_handlers: Arc<RwLock<HandlerMap>>,
}

impl ::std::fmt::Debug for NatsClient {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        f.debug_struct("NatsClient")
            .field("opts", &self.opts)
            .field("sender", &self.sender)
            .field("receiver", &self.receiver)
            .field("other_rx", &"Box<Stream>...")
            .finish()
    }
}

impl Stream for NatsClient {
    type Item = Op;

    fn poll_next(self: Pin<&mut Self>,
        cx: &mut Context<'_>,) -> Poll<Option<Self::Item>> {
        self.unsub_receiver
            .poll_next()
            .map_err(|_| RatsioError::InnerBrokenChain)
    }
}
