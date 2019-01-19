//! A "prelude" for users of the `ratsio` crate.
//!
//! This prelude is similar to the standard library's prelude in that you'll
//! almost always want to import its entire contents, but unlike the standard
//! library's prelude you'll have to do so manually:
//!
//! ```
//! use ratsio::prelude::*;
//! ```
//!
//! The prelude may grow over time as additional items see ubiquitous use.

pub use super::{
    nats_client::{
        NatsClientOptions,
        NatsClient,
        NatsClientState,
        UriVec,
    },
    error::RatsioError,
    ops::{
        Connect,
        Message,
        Op,
        Publish,
        Subscribe,
        UnSubscribe,
    },
    stan_client::{
        StanClient,
        StanOptions,
        StanMessage,
        StanSubscribe,
        StartPosition,
        SubscriptionHandler,
        AsyncHandler,
        SyncHandler,
    },
};
