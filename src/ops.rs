use crate::error::RatsioError;
use crate::nuid::NUID;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::convert::From;
use ::std::fmt;

#[derive(Debug, PartialEq)]
pub enum JsonValue {
    String(String),
    Number(f32),
    Boolean(bool),
    Array(Vec<JsonValue>),
    Object(HashMap<String, JsonValue>),
}

macro_rules! get_json_string {
    ($obj:expr, $key:expr, $default:expr) => {{
        match $obj.get($key) {
            Some(JsonValue::String(s)) => s.to_owned(),
            _ => $default,
        }
    }};

    ($obj:expr, $key:expr) => {{
        match $obj.get($key) {
            Some(JsonValue::String(s)) => s.to_owned(),
            _ => String::from(""),
        }
    }};
}

macro_rules! get_json_opt_string {
    ($obj:expr, $key:expr) => {{
        match $obj.get($key) {
            Some(JsonValue::String(s)) => Some(s.to_owned()),
            _ => None,
        }
    }};
}

macro_rules! get_json_number {
    ($obj:expr, $key:expr, $default:expr, $t:ty) => {{
        match $obj.get($key) {
            Some(JsonValue::Number(f)) => *f as $t,
            _ => $default as $t,
        }
    }};

    ($obj:expr, $key:expr, $t:ty) => {{
        match $obj.get($key) {
            Some(JsonValue::Number(f)) => *f as $t,
            _ => 0 as $t,
        }
    }};
}

macro_rules! get_json_boolean {
    ($obj:expr, $key:expr, $default:expr) => {{
        match $obj.get($key) {
            Some(JsonValue::Boolean(b)) => *b,
            _ => $default,
        }
    }};

    ($obj:expr, $key:expr) => {{
        match $obj.get($key) {
            Some(JsonValue::Boolean(b)) => *b,
            _ => false,
        }
    }};
}

/// INFO from nats.io server {["option_name":option_value],...}
///
/// The valid options are as follows:
/// * server_id: The unique identifier of the NATS server
/// * version: The version of the NATS server
/// * go: The version of golang the NATS server was built with
/// * host: The IP address used to start the NATS server, by default this will be 0.0.0.0 and can be configured with -client_advertise host:port
/// * port: The port number the NATS server is configured to listen on
/// * max_payload: Maximum payload size, in bytes, that the server will accept from the client.
/// * proto: An integer indicating the protocol version of the server. The server version 1.2.0 sets this to 1 to indicate that it supports the “Echo” feature.
/// * client_id: An optional unsigned integer (64 bits) representing the internal client identifier in the server. This can be used to filter client connections in monitoring, correlate with error logs, etc…
/// * auth_required: If this is set, then the client should try to authenticate upon connect.
/// * tls_required: If this is set, then the client must perform the TLS/1.2 handshake. Note, this used to be ssl_required and has been updated along with the protocol from SSL to TLS.
/// * tls_verify: If this is set, the client must provide a valid certificate during the TLS handshake.
/// * connect_urls : An optional list of server urls that a client can connect to.
///
///
#[derive(Clone, Debug, PartialEq)]
pub struct ServerInfo {
    pub server_id: String,
    pub version: String,
    pub go: String,
    pub host: String,
    pub port: u32,
    pub max_payload: usize,
    pub proto: u32,
    pub client_id: u64,
    pub auth_required: bool,
    pub tls_required: bool,
    pub tls_verify: bool,
    pub connect_urls: Vec<String>,
    pub nonce: String,
}

impl Default for ServerInfo {
    fn default() -> Self {
        ServerInfo {
            server_id: "".into(),
            version: "".into(),
            go: "".into(),
            host: "".into(),
            port: 0,
            max_payload: 10,
            proto: 0,
            client_id: 0,
            auth_required: false,
            tls_required: false,
            tls_verify: false,
            connect_urls: Vec::new(),
            nonce: "".to_string(),
        }
    }
}

impl ToString for ServerInfo {
    fn to_string(&self) -> String {
        let mut buff = String::default();
        buff.push_str(&format!(r#"{{"server_id": "{}""#, &self.server_id));
        buff.push_str(&format!(r#","version": "{}""#, self.version));
        buff.push_str(&format!(r#","go": "{}""#, self.go));
        buff.push_str(&format!(r#","host": "{}""#, self.go));
        buff.push_str(&format!(r#","port": {}"#, self.port));
        buff.push_str(&format!(r#","max_payload": {}"#, self.max_payload));
        buff.push_str(&format!(r#","proto": {}"#, self.proto));
        buff.push_str(&format!(r#","client_id": {}"#, self.client_id));
        buff.push_str(&format!(r#","auth_required": {}"#, self.auth_required));
        buff.push_str(&format!(r#","tls_required": {}"#, self.tls_required));
        buff.push_str(&format!(r#","tls_verify": {}"#, self.tls_verify));

        if self.connect_urls.len() > 0 {
            buff.push_str(r#","connect_urls": ["#);
            let xs = self.connect_urls.clone().into_iter().fold(String::default(), |acc, url| {
                if acc.len() > 0 {
                    format!(r#"{}, "{}""#, acc, url)
                } else {
                    format!(r#""{}""#, url)
                }
            });
            buff.push_str(&xs);
            buff.push_str(r#","]"#);
        }
        buff.push_str(&format!(r#","nonce": {}"#, self.nonce));

        buff.push_str("}");
        buff
    }
}

impl From<JsonValue> for ServerInfo {
    fn from(value: JsonValue) -> Self {
        match value {
            JsonValue::Object(obj) => {
                let connect_urls: Vec<String> = match obj.get("connect_urls") {
                    Some(JsonValue::Array(arr)) => arr
                        .into_iter()
                        .filter_map(|v| match v {
                            JsonValue::String(s) => Some(s.to_owned()),
                            _ => None,
                        })
                        .collect(),
                    _ => Vec::new(),
                };
                ServerInfo {
                    server_id: get_json_string!(obj, "server_id"),
                    version: get_json_string!(obj, "version"),
                    go: get_json_string!(obj, "go"),
                    host: get_json_string!(obj, "host"),
                    port: get_json_number!(obj, "port", u32),
                    max_payload: get_json_number!(obj, "max_payload", usize),
                    proto: get_json_number!(obj, "proto", 0, u32),
                    client_id: get_json_number!(obj, "client_id", u64),
                    auth_required: get_json_boolean!(obj, "auth_required", false),
                    tls_required: get_json_boolean!(obj, "tls_required", false),
                    tls_verify: get_json_boolean!(obj, "tls_verify", false),
                    connect_urls,
                    nonce: get_json_string!(obj, "nonce"),
                }
            }
            _ => ServerInfo::default(),
        }
    }
}

///
/// CONNECT {["option_name":option_value],...}
///
/// The valid options are as follows:
///
/// * verbose: Turns on +OK protocol acknowledgements.
/// * pedantic: Turns on additional strict format checking, e.g. for properly formed subjects
/// * tls_required: Indicates whether the client requires an SSL connection.
/// * auth_token: Client authorization token (if auth_required is set)
/// * user: Connection username (if auth_required is set)
/// * pass: Connection password (if auth_required is set)
/// * name: Optional client name
/// * lang: The implementation language of the client.
/// * version: The version of the client.
/// * jwt: If using User JWT credentials, this contains an encoded JWT for the user
/// * sig: A signature produced from the nonce the server sent with its INFO message (if using JWT security)
/// * protocol: optional int. Sending 0 (or absent) indicates client supports original protocol. Sending 1 indicates that the client supports dynamic reconfiguration of cluster topology changes by asynchronously receiving INFO messages with known servers it can reconnect to.
/// * echo: Optional boolean. If set to true, the server (version 1.2.0+) will not send originating messages from this connection to its own subscriptions. Clients should set this to true only for server supporting this feature, which is when proto in the INFO protocol is set to at least 1.
#[derive(Clone, Debug, PartialEq)]
pub struct Connect {
    pub verbose: bool,
    pub pedantic: bool,
    pub tls_required: bool,
    pub auth_token: Option<String>,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub name: Option<String>,
    pub lang: String,
    pub version: String,
    pub protocol: u32,
    pub echo: bool,
    pub sig: Option<String>,
    pub jwt: Option<String>,
}

impl Connect {}

impl ToString for Connect {
    fn to_string(&self) -> String {
        let mut buff = String::default();
        buff.push_str("{\"verbose\": ");
        buff.push_str(&self.verbose.to_string());
        buff.push_str(&format!(r#","pedantic": {}"#, self.pedantic));
        buff.push_str(&format!(r#","tls_required": {}"#, self.tls_required));
        if let Some(ref auth_token) = self.auth_token {
            buff.push_str(&format!(r#","auth_token": "{}""#, auth_token));
        }
        if let Some(ref user) = self.user {
            buff.push_str(&format!(r#","user": "{}""#, user));
        }
        if let Some(ref pass) = self.pass {
            buff.push_str(&format!(r#","pass": "{}""#, pass));
        }
        if let Some(ref name) = self.name {
            buff.push_str(&format!(r#","name": "{}""#, name));
        }
        buff.push_str(&format!(r#","lang": "{}""#, self.lang));
        buff.push_str(&format!(r#","version": "{}""#, self.version));
        buff.push_str(&format!(r#","protocol": {}"#, self.protocol));
        buff.push_str(&format!(r#","echo": {}"#, self.echo));
        if let Some(ref sig) = self.sig {
            buff.push_str(&format!(r#","sig": "{}""#, sig));
        }
        if let Some(ref jwt) = self.jwt {
            buff.push_str(&format!(r#","jwt": "{}""#, jwt));
        }
        buff.push_str("}");
        buff
    }
}

impl Default for Connect {
    fn default() -> Self {
        Connect {
            verbose: true,
            pedantic: false,
            tls_required: false,
            auth_token: None,
            user: None,
            pass: None,
            name: None,
            lang: "rust".into(),
            version: "0.2.0".into(),
            protocol: 1,
            echo: true,
            sig: None,
            jwt: None,
        }
    }
}

impl From<JsonValue> for Connect {
    fn from(value: JsonValue) -> Self {
        match value {
            JsonValue::Object(obj) => Connect {
                verbose: get_json_boolean!(obj, "verbose", true),
                pedantic: get_json_boolean!(obj, "pedantic", false),
                tls_required: get_json_boolean!(obj, "tls_required", false),
                auth_token: get_json_opt_string!(obj, "auth_token"),
                user: get_json_opt_string!(obj, "user"),
                pass: get_json_opt_string!(obj, "pass"),
                name: get_json_opt_string!(obj, "name"),
                lang: get_json_string!(obj, "lang"),
                version: get_json_string!(obj, "version"),
                protocol: get_json_number!(obj, "protocol", 0, u32),
                echo: get_json_boolean!(obj, "tls_verify", true),
                sig: get_json_opt_string!(obj, "sig"),
                jwt: get_json_opt_string!(obj, "jwt"),
            },
            _ => Connect::default(),
        }
    }
}

/// MSG  protocol message is used to deliver an application message to the client.
/// MSG <subject> <sid> [reply-to] <#bytes>\r\n[payload]\r\n
///
/// where:
///
/// * subject: Subject name this message was received on
/// *  sid: The unique alphanumeric subscription ID of the subject
/// * reply-to: The inbox subject on which the publisher is listening for responses
/// * #bytes: Size of the payload in bytes
/// * payload: The message payload data
///
#[derive(Clone, PartialEq)]
pub struct Message {
    pub subject: String,
    pub sid: String,
    pub reply_to: Option<String>,
    pub payload: Vec<u8>,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Message {{ subject: {}, sid: {}, reply_to: {:?} }}",
            self.subject, self.sid, self.reply_to
        )
    }
}

impl Default for Message {
    fn default() -> Self {
        Message {
            subject: String::from(""),
            sid: String::from(""),
            reply_to: None,
            payload: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Publish {
    pub subject: String,
    pub reply_to: Option<String>,
    pub payload: Vec<u8>,
}

impl Publish {
    pub fn generate_reply_to() -> String {
        NUID::new().next()
    }
}

impl Default for Publish {
    fn default() -> Self {
        Publish {
            subject: String::from(""),
            reply_to: None,
            payload: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Subscribe {
    pub subject: String,
    pub sid: String,
    pub queue_group: Option<String>,
}

impl Default for Subscribe {
    fn default() -> Self {
        Subscribe {
            subject: String::from(""),
            sid: Subscribe::generate_sid(),
            queue_group: None,
        }
    }
}

impl Subscribe {
    pub fn generate_sid() -> String {
        NUID::new().next()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnSubscribe {
    pub sid: String,
    pub max_msgs: Option<u32>,
}

impl UnSubscribe {}

impl Default for UnSubscribe {
    fn default() -> Self {
        UnSubscribe {
            sid: String::from(""),
            max_msgs: None,
        }
    }
}

/*
INFO	Server	Sent to client after initial TCP/IP connection
CONNECT	Client	Sent to server to specify connection information
PUB	Client	Publish a message to a subject, with optional reply subject
SUB	Client	Subscribe to a subject (or subject wildcard)
UNSUB	Client	Unsubscribe (or auto-unsubscribe) from subject
MSG	Server	Delivers a message payload to a subscriber
PING	Both	PING keep-alive message
PONG	Both	PONG keep-alive response
+OK	Server	Acknowledges well-formed protocol message in verbose mode
-ERR
*/
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    INFO(ServerInfo),
    CONNECT(Connect),
    OK,
    ERR(String),
    PING,
    PONG,
    MSG(Message),
    PUB(Publish),
    SUB(Subscribe),
    UNSUB(UnSubscribe),
    CLOSE,
}

#[inline]
fn extend_bytes<'a>(dst: &'a mut BytesMut, s: &[u8]) {
    let buf_len = s.len();
    if dst.remaining_mut() < buf_len {
        dst.reserve(buf_len);
    }
    dst.put(s);
}

impl Op {
    pub fn into_bytes(self) -> Result<Bytes, RatsioError> {
        match self {
            Op::INFO(info) => {
                let prefix = &b"INFO\t"[..];
                let serialized_info = info.to_string();
                let mut dst = BytesMut::with_capacity(serialized_info.len() + prefix.len() + 2);
                dst.put(&prefix[..]);
                dst.put(serialized_info.as_bytes());
                dst.put(&b"\r\n"[..]);
                Ok(dst.freeze())
            }
            Op::CONNECT(connect) => {

                let prefix = &b"CONNECT\t"[..];
                let serialized_connect = connect.to_string();
                let mut dst = BytesMut::with_capacity(serialized_connect.len() + prefix.len() + 2);
                dst.put(&prefix[..]);
                dst.put(serialized_connect.as_bytes());
                dst.put(&b"\r\n"[..]);
                Ok(dst.freeze())

            }
            Op::OK => {
                let cmd = &b"+OK\r\n"[..];
                let mut dst = BytesMut::with_capacity(cmd.len());
                dst.put(cmd);
                Ok(dst.freeze())
            }
            Op::PING => {
                let cmd = &b"PING\r\n"[..];
                let mut dst = BytesMut::with_capacity(cmd.len());
                dst.put(cmd);
                Ok(dst.freeze())
            }
            Op::PONG => {
                let cmd = &b"PONG\r\n"[..];
                let mut dst = BytesMut::with_capacity(cmd.len());
                dst.put(cmd);
                Ok(dst.freeze())
            }
            Op::ERR(msg) => {
                use regex::Regex;
                let re = Regex::new(r"[']").unwrap();
                let cmd = format!("-ERR '{}'\r\n", re.replace_all(msg.as_str(), "\\'"));
                let mut dst = BytesMut::with_capacity(cmd.len());
                dst.put(cmd.as_bytes());
                Ok(dst.freeze())
            }
            Op::MSG(msg) => {
                let mut dst = BytesMut::new();
                extend_bytes(&mut dst, &b"MSG\t"[..]);
                extend_bytes(&mut dst, msg.subject.as_bytes());
                extend_bytes(&mut dst, &b"\t"[..]);
                extend_bytes(&mut dst, msg.sid.as_bytes());
                if let Some(reply_to) = msg.reply_to {
                    extend_bytes(&mut dst, &b"\t"[..]);
                    extend_bytes(&mut dst, reply_to.as_bytes());
                }
                extend_bytes(&mut dst, format!("\t{}\r\n", msg.payload.len()).as_bytes());
                extend_bytes(&mut dst, &msg.payload[..]);
                extend_bytes(&mut dst, &b"\r\n"[..]);
                Ok(dst.freeze())
            }
            Op::PUB(publish) => {
                let mut dst = BytesMut::new();
                extend_bytes(&mut dst, &b"PUB\t"[..]);
                extend_bytes(&mut dst, publish.subject.as_bytes());
                if let Some(reply_to) = publish.reply_to {
                    extend_bytes(&mut dst, &b"\t"[..]);
                    extend_bytes(&mut dst, reply_to.as_bytes());
                }
                extend_bytes(
                    &mut dst,
                    format!("\t{}\r\n", publish.payload.len()).as_bytes(),
                );
                extend_bytes(&mut dst, &publish.payload[..]);
                extend_bytes(&mut dst, &b"\r\n"[..]);
                Ok(dst.freeze())
            }
            Op::SUB(sub) => {
                let mut dst = BytesMut::new();
                extend_bytes(&mut dst, &b"SUB\t"[..]);
                extend_bytes(&mut dst, sub.subject.as_bytes());
                if let Some(queue_group) = sub.queue_group {
                    extend_bytes(&mut dst, &b"\t"[..]);
                    extend_bytes(&mut dst, queue_group.as_bytes());
                }
                extend_bytes(&mut dst, &b"\t"[..]);
                extend_bytes(&mut dst, sub.sid.as_bytes());
                extend_bytes(&mut dst, &b"\r\n"[..]);
                Ok(dst.freeze())
            }
            Op::UNSUB(unsub) => {
                let mut dst = BytesMut::new();
                extend_bytes(&mut dst, &b"UNSUB\t"[..]);
                extend_bytes(&mut dst, unsub.sid.as_bytes());
                if let Some(max_msgs) = unsub.max_msgs {
                    extend_bytes(&mut dst, format!("\t{}", max_msgs).as_bytes());
                }
                extend_bytes(&mut dst, &b"\r\n"[..]);
                Ok(dst.freeze())
            }
            Op::CLOSE => {
                let cmd = &b"+CLOSE\r\n"[..];
                let mut dst = BytesMut::with_capacity(cmd.len());
                dst.put(cmd);
                Ok(dst.freeze())
            }
        }
    }
}

#[test]
fn ser_ok() {
    match Op::OK.into_bytes() {
        Ok(b) => {
            assert_eq!(&b[..], b"+OK\r\n");
        }
        Err(_) => {
            assert!(false);
        }
    }
}

#[test]
fn ser_ping() {
    match Op::PING.into_bytes() {
        Ok(b) => {
            assert_eq!(&b[..], b"PING\r\n");
        }
        Err(_) => {
            assert!(false);
        }
    }
}

#[test]
fn ser_pong() {
    match Op::PONG.into_bytes() {
        Ok(b) => {
            assert_eq!(&b[..], b"PONG\r\n");
        }
        Err(_) => {
            assert!(false);
        }
    }
}

#[test]
fn ser_connect() {
    match Op::CONNECT(Connect {
        verbose: false,
        pedantic: false,
        version: String::from("1.2.2"),
        protocol: 1,
        lang: String::from("go"),
        name: Some(String::from("")),
        tls_required: false,
        user: None,
        pass: None,
        auth_token: None,
        echo: true,
        sig: None,
        jwt: None,
    })
        .into_bytes()
        {
            Ok(b) => {
                println!(" -----------=> \n{}", String::from_utf8(Vec::from(&b[..])).unwrap());
                //                                           {"verbose": false,"pedantic": false,"tls_required": false,"name": "","lang": "go","version": "1.2.2","protocol": 1,"echo": "true"}
                let c = format!("CONNECT\t{}\r\n", r#"{"verbose": false,"pedantic": false,"tls_required": false,"name": "","lang": "go","version": "1.2.2","protocol": 1,"echo": true}"#);
                assert_eq!(&b[..], c.as_bytes());
            }
            Err(_) => {
                assert!(false);
            }
        }
}

#[test]
fn ser_message() {
    match Op::MSG(Message {
        subject: String::from("FOO.BAR"),
        sid: String::from("9"),
        reply_to: Some(String::from("INBOX.34")),
        payload: Vec::from(b"Hello World" as &[u8]),
    })
        .into_bytes()
        {
            Ok(b) => {
                //println!(" -----------=> \n{}", String::from_utf8(Vec::from(&b[..])).unwrap());
                let c = format!("MSG\tFOO.BAR\t9\tINBOX.34\t11\r\n{}\r\n", r#"Hello World"#);
                assert_eq!(&b[..], c.as_bytes());
            }
            Err(_) => {
                assert!(false);
            }
        }
}

#[test]
fn ser_message_no_reply() {
    match Op::MSG(Message {
        subject: String::from("FOO.BAR"),
        sid: String::from("9"),
        reply_to: None,
        payload: Vec::from(b"Hello New World" as &[u8]),
    })
        .into_bytes()
        {
            Ok(b) => {
                //println!(" -----------=> \n{}", String::from_utf8(Vec::from(&b[..])).unwrap());
                let c = format!("MSG\tFOO.BAR\t9\t15\r\n{}\r\n", r#"Hello New World"#);
                assert_eq!(&b[..], c.as_bytes());
            }
            Err(_) => {
                assert!(false);
            }
        }
}

#[test]
fn ser_publish() {
    match Op::PUB(Publish {
        subject: String::from("FRONT.DOOR"),
        reply_to: Some(String::from("INBOX.22")),
        payload: Vec::from(b"Knock Knock" as &[u8]),
    })
        .into_bytes()
        {
            Ok(b) => {
                //println!(" -----------=> \n{}", String::from_utf8(Vec::from(&b[..])).unwrap());
                assert_eq!(
                    &b[..],
                    &b"PUB\tFRONT.DOOR\tINBOX.22\t11\r\nKnock Knock\r\n"[..]
                );
            }
            Err(_) => {
                assert!(false);
            }
        }
}

#[test]
fn ser_publish_no_reply() {
    match Op::PUB(Publish {
        subject: String::from("FRONT.DOOR"),
        reply_to: None,
        payload: Vec::from(b"Knock Knock Again" as &[u8]),
    })
        .into_bytes()
        {
            Ok(b) => {
                //println!(" -----------=> \n{}", String::from_utf8(Vec::from(&b[..])).unwrap());
                assert_eq!(&b[..], &b"PUB\tFRONT.DOOR\t17\r\nKnock Knock Again\r\n"[..]);
            }
            Err(_) => {
                assert!(false);
            }
        }
}

#[test]
fn ser_sub() {
    match Op::SUB(Subscribe {
        subject: String::from("BAR"),
        sid: String::from("44"),
        queue_group: Some(String::from("G1")),
    })
        .into_bytes()
        {
            Ok(b) => {
                //println!(" -----------=> \n{}", String::from_utf8(Vec::from(&b[..])).unwrap());
                assert_eq!(&b[..], &b"SUB\tBAR\tG1\t44\r\n"[..]);
            }
            Err(_) => {
                assert!(false);
            }
        }
}

#[test]
fn ser_sub_no_group() {
    match Op::SUB(Subscribe {
        subject: String::from("BAR"),
        sid: String::from("44"),
        queue_group: None,
    })
        .into_bytes()
        {
            Ok(b) => {
                //println!(" -----------=> \n{}", String::from_utf8(Vec::from(&b[..])).unwrap());
                assert_eq!(&b[..], &b"SUB\tBAR\t44\r\n"[..]);
            }
            Err(_) => {
                assert!(false);
            }
        }
}

#[test]
fn ser_unsub() {
    match Op::UNSUB(UnSubscribe {
        sid: String::from("44234535"),
        max_msgs: Some(500),
    })
        .into_bytes()
        {
            Ok(b) => {
                //println!(" -----------=> \n{}", String::from_utf8(Vec::from(&b[..])).unwrap());
                assert_eq!(&b[..], &b"UNSUB\t44234535\t500\r\n"[..]);
            }
            Err(_) => {
                assert!(false);
            }
        }
}

#[test]
fn ser_unsub_no_max() {
    match Op::UNSUB(UnSubscribe {
        sid: String::from("44234535"),
        max_msgs: None,
    })
        .into_bytes()
        {
            Ok(b) => {
                //println!(" -----------=> \n{}", String::from_utf8(Vec::from(&b[..])).unwrap());
                assert_eq!(&b[..], &b"UNSUB\t44234535\r\n"[..]);
            }
            Err(_) => {
                assert!(false);
            }
        }
}
