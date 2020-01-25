pub mod client;
mod client_inner;
 mod converters;

use crate::net::nats_tcp_stream::NatsTcpStream;
use crate::ops::{ServerInfo, Op, Message, Subscribe};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

use std::fmt::Debug;
use futures::stream::{ SplitSink};
use futures::lock::Mutex;
use nom::lib::std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone)]
pub struct NatsSid(pub(crate) String);

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
    Shutdown,
}
pub(crate) type ReconnectHandler = Box<dyn Fn(&NatsClient) -> () + Send + Sync>;
pub use crate::ops::Message as NatsMessage;

pub struct NatsClient {
    inner: Arc<NatsClientInner>,
    reconnect_handlers: RwLock<Vec<ReconnectHandler>>,
}

#[derive(Debug)]
pub (crate) enum ClosableMessage {
    Message(Message),
    Close,
}

pub struct NatsClientInner {
    conn_sink: Arc<Mutex<SplitSink<NatsTcpStream, Op>>>,
    /// Backup of options
    opts: NatsClientOptions,
    /// Server info
    server_info: RwLock<Option<ServerInfo>>,
    subscriptions: Arc<Mutex<HashMap<String, (UnboundedSender<ClosableMessage>, Subscribe)>>>,
    on_reconnect: tokio::sync::Mutex<Option<Pin<Box<dyn Future<Output=()> + Send + Sync>>>>,
    state: RwLock<NatsClientState>,
    last_ping: RwLock<u128>,
    client_ref: RwLock<Option<Arc<NatsClient>>>,
    reconnect_version: RwLock<u128>,
}

impl ::std::fmt::Debug for NatsClient {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        f.debug_struct("NatsClient")
            .field("opts", &self.inner.opts)
            .finish()
    }
}


#[derive(Clone, Debug, PartialEq)]
pub struct UriVec(Vec<String>);


impl From<String> for NatsClientOptions {
    fn from(uri: String) -> Self {
        NatsClientOptions{
            cluster_uris: UriVec(vec![uri]),
            ..Default::default()
        }
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
