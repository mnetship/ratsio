use crate::codec::OpCodec;
use futures::prelude::*;
use native_tls::TlsConnector as NativeTlsConnector;
use std::net::SocketAddr;
use tokio_tcp::TcpStream;
use tokio_tls::{TlsConnector, TlsStream};
use crate::error::*;
use crate::ops::Op;
use tokio::codec::{Decoder, Framed};

#[derive(Debug)]
pub(crate) enum NatsConnectionInner {
    Tcp(Box<Framed<TcpStream, OpCodec>>),

    Tls(Box<Framed<TlsStream<TcpStream>,OpCodec>>),
}


impl NatsConnectionInner {
    pub(crate) fn connect_tcp(addr: &SocketAddr) -> impl Future<Item=TcpStream, Error=RatsioError> {
        TcpStream::connect(addr).from_err()
    }

    pub(crate) fn upgrade_tcp_to_tls(host: &str, socket: TcpStream) -> impl Future<Item=TlsStream<TcpStream>, Error=RatsioError>{
        let tls_connector = NativeTlsConnector::builder().build().unwrap();
        let tls_stream: TlsConnector = tls_connector.into();
        tls_stream.connect(&host, socket).from_err()
    }
}

impl From<TcpStream> for NatsConnectionInner {
    fn from(socket: TcpStream) -> Self {
        NatsConnectionInner::Tcp(Box::new(OpCodec::default().framed(socket)))
    }
}

impl From<TlsStream<TcpStream>> for NatsConnectionInner {
    fn from(socket: TlsStream<TcpStream>) -> Self {
        NatsConnectionInner::Tls(Box::new(OpCodec::default().framed(socket)))
    }
}

impl Sink for NatsConnectionInner {
    type SinkError = RatsioError;
    type SinkItem = Op;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.start_send(item),
            NatsConnectionInner::Tls(framed) => framed.start_send(item),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.poll_complete(),
            NatsConnectionInner::Tls(framed) => framed.poll_complete(),
        }
    }
}

impl Stream for NatsConnectionInner {
    type Error = RatsioError;
    type Item = Op;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.poll(),
            NatsConnectionInner::Tls(framed) => framed.poll(),
        }
    }
}
