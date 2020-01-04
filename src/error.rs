use std::io;

macro_rules! from_error {
    ($type:ty, $target:ident, $targetvar:expr) => {
        impl From<$type> for $target {
            fn from(s: $type) -> Self {
                $targetvar(s.into())
            }
        }
    };
}


/// Error enum for all cases of internal/external errors occuring during client execution
#[derive(Debug, Fail)]
pub enum RatsioError {
    /// Building a command has failed because of invalid syntax or incorrect arguments
    #[fail(display = "CommandBuildError: {}", _0)]
    CommandBuildError(String),
    /// Generic IO error from stdlib
    #[fail(display = "IOError: {:?}", _0)]
    IOError(io::Error),
    /// Occurs when the client is not yet connected or got disconnected from the server.
    /// Contains `Some<io::Error>` when it's actually a disconnection or contains `None` when we are not connected at all
    #[fail(display = "ServerDisconnected: {:?}", _0)]
    ServerDisconnected(Option<io::Error>),
    /// Protocol error
    /// Occurs if we try to parse a string that is supposed to be valid UTF8 and...is actually not
    #[fail(display = "UTF8Error: {}", _0)]
    UTF8Error(::std::string::FromUtf8Error),
    /// Error on TLS handling
    #[fail(display = "TlsError: {}", _0)]
    TlsError(::native_tls::Error),
    // Occurs when the host is not provided, removing the ability for TLS to function correctly for server identify verification
    #[fail(display = "NoRouteToHostError: Host is missing, can't verify server identity")]
    NoRouteToHostError,
    /// Cannot parse an URL
    #[fail(display = "UrlParseError: {}", _0)]
    UrlParseError(::url::ParseError),
    /// Cannot parse an IP
    #[fail(display = "AddrParseError: {}", _0)]
    AddrParseError(::std::net::AddrParseError),
    /// Occurs when we cannot resolve the URI given using the local host's DNS resolving mechanisms
    /// Will contain `Some(io::Error)` when the resolving has been tried with an error, and `None` when
    /// resolving succeeded but gave no results
    #[fail(display = "UriDNSResolveError: {:?}", _0)]
    UriDNSResolveError(Option<io::Error>),
    /// Cannot reconnect to server after retrying once
    #[fail(display = "CannotReconnectToServer: cannot reconnect to server")]
    CannotReconnectToServer,
    /// Something went wrong in one of the Reciever/Sender pairs
    #[fail(display = "InnerBrokenChain: the sender/receiver pair has been disconnected")]
    InnerBrokenChain,
    /// The user supplied a too big payload for the server
    #[fail(
        display = "MaxPayloadOverflow: the given payload exceeds the server setting (max_payload_size = {})",
        _0
    )]
    MaxPayloadOverflow(usize),
    /// Generic string error
    #[fail(display = "GenericError: {}", _0)]
    GenericError(String),
    /// Error thrown when a subscription is fused after reaching the maximum messages
    #[fail(display = "SubscriptionReachedMaxMsgs after {} messages", _0)]
    SubscriptionReachedMaxMsgs(u32),

    #[fail(display = "Stream Closed for {}", _0)]
    StreamClosed(String)
}

impl From<io::Error> for RatsioError {
    fn from(err: io::Error) -> Self {
        match err.kind() {
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionRefused => {
                RatsioError::ServerDisconnected(Some(err))
            }
            _ => RatsioError::IOError(err),
        }
    }
}

impl From<::futures::channel::mpsc::SendError> for RatsioError {
    fn from(_: ::futures::channel::mpsc::SendError) -> Self {
        RatsioError::InnerBrokenChain
    }
}

impl From<RatsioError> for () {
    fn from(err: RatsioError) -> Self {
         error!(target:"ratsio", "Rats-io error => {}", err);
    }
}

from_error!(::std::string::FromUtf8Error, RatsioError, RatsioError::UTF8Error);
from_error!(::native_tls::Error, RatsioError, RatsioError::TlsError);
from_error!(String, RatsioError, RatsioError::GenericError);
from_error!(::url::ParseError, RatsioError, RatsioError::UrlParseError);
