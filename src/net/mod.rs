use futures::sync::mpsc::UnboundedSender;
use std::sync::Arc;
pub(crate) use self::connection::{NatsConnSinkStream, NatsConnection};

pub(crate) mod connection;
mod connection_inner;


pub(crate) type ReconnectHandler = UnboundedSender<Arc<NatsConnection>>;
