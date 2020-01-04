use crate::codec::OpCodec;
use futures::{prelude::*, task::{Context, Poll}};
use native_tls::TlsConnector as NativeTlsConnector;
use std::{net::SocketAddr, pin::Pin};
use tokio::net::TcpStream;
use tokio_tls::{TlsConnector, TlsStream};
use tokio_util::codec::{Decoder, Framed};
use crate::error::*;
use crate::ops::Op;

#[derive(Debug)]
pub(crate) enum NatsConnectionInner {
    Tcp(Box<Framed<TcpStream, OpCodec>>),

    Tls(Box<Framed<TlsStream<TcpStream>,OpCodec>>),
}


impl NatsConnectionInner {
    pub(crate) fn connect_tcp(addr: SocketAddr) -> impl Future<Output=Result<TcpStream, RatsioError>> {
        TcpStream::connect(addr)
            .map_err(|err| RatsioError::from(err))
    }

    pub(crate) fn upgrade_tcp_to_tls(host: String, socket: TcpStream) -> impl Future<Output=Result<TlsStream<TcpStream>, RatsioError>>{
        let tls_connector = NativeTlsConnector::builder().build().unwrap();
        let tls_stream: TlsConnector = tls_connector.into();
        tls_stream.connect(&host, socket)
            .map(|result| result.map_err(|err| RatsioError::from(err)))
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

impl Sink<Op> for NatsConnectionInner {
    type Error = RatsioError;

    fn start_send(&mut self, item: Op) -> Result<(), Self::Error> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.start_send(item),
            NatsConnectionInner::Tls(framed) => framed.start_send(item),
        }
    }

    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.poll_complete(),
            NatsConnectionInner::Tls(framed) => framed.poll_complete(),
        }
    }
}

impl Stream for NatsConnectionInner {
    type Item = Op;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.poll_next(),
            NatsConnectionInner::Tls(framed) => framed.poll_next(),
        }
    }
}
