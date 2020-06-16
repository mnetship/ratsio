#[macro_use]
extern crate nom;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate derive_builder;

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/pb.rs"));
}

pub mod net;
pub mod parser;
pub mod ops;
pub mod error;
pub mod nuid;
pub mod nats_client;
pub mod stan_client;

pub use nats_client::{NatsMessage, NatsSid, NatsClient, NatsClientOptions};
pub use stan_client::{StanMessage, StanSid, StanClient, StanOptions, StartPosition};
pub use error::RatsioError;

