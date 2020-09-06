use std::io;
use futures::task::SpawnError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RatsioError {
    //Http-like errors

    #[error("CommandBuildError: {0}")]
    CommandBuildError(String),
    /// Generic IO error from stdlib
    #[error("IOError: {0:?}")]
    IOError(#[from] io::Error),
    /// Occurs when the client is not yet connected or got disconnected from the server.
    /// Contains `Some<io::Error>` when it's actually a disconnection or contains `None` when we are not connected at all
    #[error("ServerDisconnected: {0:?}")]
    ServerDisconnected(#[from] Option<io::Error>),
    /// Protocol error
    /// Occurs if we try to parse a string that is supposed to be valid UTF8 and...is actually not
    #[error("UTF8Error: {0}")]
    UTF8Error(#[from]  ::std::string::FromUtf8Error),
    /// Error on TLS handling
    // Occurs when the host is not provided, removing the ability for TLS to function correctly for server identify verification
    #[error("NoRouteToHostError: Host is missing, can't verify server identity")]
    NoRouteToHostError,
    /// Cannot parse an URL
    #[error("Request Stream closed before a result was obtained.")]
    RequestStreamClosed,

    /// Cannot decode protobuf message
    #[error("Request Stream closed before a result was obtained.")]
    ProstDecodeError(#[from] prost::DecodeError),

    /// Cannot parse an IP
    #[error("AddrParseError: {0}")]
    AddrParseError(#[from] ::std::net::AddrParseError),
    /// Cannot reconnect to server after retrying once
    #[error("CannotReconnectToServer: cannot reconnect to server")]
    CannotReconnectToServer,
    /// Something went wrong in one of the Reciever/Sender pairs
    #[error("InnerBrokenChain: the sender/receiver pair has been disconnected")]
    InnerBrokenChain,
    /// Something unexpected went wrong
    #[error("InternalServerError: something unexpected went wrong")]
    InternalServerError,
    /// The user supplied a too big payload for the server
    #[error("MaxPayloadOverflow: the given payload exceeds the server setting (max_payload_size = {0})")]
    MaxPayloadOverflow(usize),
    /// Generic string error
    #[error("GenericError: {0}")]
    GenericError(String),
    /// Error thrown when a subscription is fused after reaching the maximum messages
    #[error("SubscriptionReachedMaxMsgs after {0} messages")]
    SubscriptionReachedMaxMsgs(u32),

    #[error("Stream Closed for {0}")]
    StreamClosed(String),

    #[error("Missing ack_inbox for acknowledgement")]
    AckInboxMissing,

    #[error("SpawnError for {0:?}")]
    SpawnError(#[from] SpawnError)
}

impl From<RatsioError> for () {
    fn from(err: RatsioError) -> Self {
         error!(target:"ratsio", "Rats-io error => {}", err);
    }
}
