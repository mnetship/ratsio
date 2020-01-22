use crate::codec::OpCodec;
use futures::prelude::*;
use native_tls::{TlsConnector as NativeTlsConnector};
use std::net::{SocketAddr};
use tokio_tls::{TlsConnector, TlsStream};
use crate::error::*;
use crate::ops::Op;
use tokio_codec::{Decoder, Framed};
use failure::_core::task::{Context, Poll};
use failure::_core::pin::Pin;
use std::ops::Deref;
use async_std::net::TcpStream;


#[derive(Debug)]
pub(crate) enum NatsConnectionInner {
    Tcp(Box<Framed<TcpStream, OpCodec>>),
    Tls(Box<Framed<TlsStream<TcpStream>,OpCodec>>),
}


impl NatsConnectionInner {
    pub(crate) async fn connect_tcp(addr: &SocketAddr) -> Result<TcpStream,RatsioError> {
        TcpStream::connect(addr).await.map_err(|e| e.into())
    }

    pub(crate) async fn upgrade_tcp_to_tls(host: &str, socket: TcpStream) ->Result<TlsStream<TcpStream>, RatsioError>{
        let tls_connector = NativeTlsConnector::builder().build().unwrap();
        let tls_stream: TlsConnector = tls_connector.into();
        tls_stream.connect(&host, socket).await.map_err(|e| e.into())
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

    fn start_send(&mut self, item: Op) -> Result<(), RatsioError> {
        match self {
            NatsConnectionInner::Tcp(framed) => framed.start_send(item),
            NatsConnectionInner::Tls(framed) => framed.start_send(item),
        }
    }

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.deref() {
            NatsConnectionInner::Tcp(framed) => framed.poll_ready(),
            NatsConnectionInner::Tls(framed) => framed.poll_ready(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.deref() {
            NatsConnectionInner::Tcp(framed) => framed.poll_flush(),
            NatsConnectionInner::Tls(framed) => framed.poll_flush(),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.deref() {
            NatsConnectionInner::Tcp(framed) => framed.poll_close(),
            NatsConnectionInner::Tls(framed) => framed.poll_close(),
        }
    }
}

impl Stream for NatsConnectionInner {
    type Item = Op;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
         match self.deref() {
            NatsConnectionInner::Tcp(framed) => framed.poll_next(),
            NatsConnectionInner::Tls(framed) => framed.poll_next(),
        }
    }
}
