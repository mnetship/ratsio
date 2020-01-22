# Ratsio

Ratsio is a Rust client library for [NATS](https://nats.io) messaging system and [NATS Event Streaming](https://nats.io/documentation/streaming/nats-streaming-intro/).

Inspired by [nitox](https://raw.githubusercontent.com/YellowInnovation/nitox) and [rust-nats](https://github.com/jedisct1/rust-nats) but my project needed NATS streaming, so I couldn't use any of those 2. If this project is useful to you, feel free to contribute or suggest features. at the moment it's just the features I need.

Add the following to your Cargo.toml.

```rust
[dependencies]
ratsio = "0.3.0-alpha.1"
```
Rust -stable, -beta and -nightly are supported.

## Features:
- [x] Nats messaging queue. Publish, Subcribe and Request.
- [x] Nats cluster support, auto reconnect 
- [ ] Dynamic cluster hosts update. 
- [x] Async from the ground up, using  [tokio](https://crates.io/crates/tokio) and [futures](https://crates.io/crates/futures).
- [?] TLS mode
- [x] NATS 1.x Authentication
- [?] NATS 2.0 JWT-based client authentication
- [x] NATS Streaming Server
# Usage

Subscribing and Publishing to a NATS subject: see examples/nats_subscribe.rs
```rust
use ratsio::{NatsClient, RatsioError};
use log::info;
use futures::StreamExt;

pub fn logger_setup() {
    use log::LevelFilter;
    use std::io::Write;
    use env_logger::Builder;

    let _ = Builder::new()
        .format(|buf, record| {
            writeln!(buf,
                     "[{}] - {}",
                     record.level(),
                     record.args()
            )
        })
        .filter(None, LevelFilter::Trace)
        .try_init();
}


#[tokio::main]
async fn main() -> Result<(), RatsioError> {
    logger_setup();

    //Create nats client
    let nats_client = NatsClient::new("nats://localhost:4222").await?;
    
    //subscribe to nats subject 'foo'
    let (sid, mut subscription) = nats_client.subscribe("foo").await?;
    tokio::spawn(async move {
        //Listen for messages on the 'foo' description 
        //The loop terminates when the upon un_subscribe
        while let Some(message) = subscription.next().await {
            info!(" << 1 >> got message --- {:?}\n\t{:?}", &message,
                  String::from_utf8_lossy(message.payload.as_ref()));
        }
        info!(" << 1 >> unsubscribed. loop is terminated.")
    });

    //subscribe to nats subject 'foo', another subscription 
    let (_sid, mut subscription2) = nats_client.subscribe("foo").await?;
    tokio::spawn(async move {
        //Listen for messages on the 'foo' description
        while let Some(message) = subscription2.next().await {
            info!(" << 2 >> got message --- {:?}\n\t{:?}", &message,
                  String::from_utf8_lossy(message.payload.as_ref()));
        }
    });

    //Publish some messages, restart nats server during this time.
    use std::{thread, time};
    thread::sleep(time::Duration::from_secs(5));


    //Publish message
    let _ = nats_client.publish("foo", b"Publish Message 1").await?;
    thread::sleep(time::Duration::from_secs(1));

    //Unsubscribe
    let _ = nats_client.un_subscribe(&sid).await?;
    thread::sleep(time::Duration::from_secs(3));

    //Publish some messages.
    thread::sleep(time::Duration::from_secs(1));
    let _ = nats_client.publish("foo", b"Publish Message 2").await?;
    thread::sleep(time::Duration::from_secs(600));
    info!(" ---- done --- ");
    Ok(())
}
```

Subscribing and Publishing to a NATS streaming subject: see tests/stan_subscribe.rs
``` rust
use log::info;
use futures::StreamExt;
use ratsio::{RatsioError, StanClient, StanOptions};

pub fn logger_setup() {
    use log::LevelFilter;
    use std::io::Write;
    use env_logger::Builder;

    let _ = Builder::new()
        .format(|buf, record| {
            writeln!(buf,
                     "[{}] - {}",
                     record.level(),
                     record.args()
            )
        })
        .filter(None, LevelFilter::Trace)
        .try_init();
}


#[tokio::main]
async fn main() -> Result<(), RatsioError> {
    logger_setup();
    // Create stan options
    let client_id = "test1".to_string();
    let opts = StanOptions::with_options("localhost:4222", "test-cluster", &client_id[..]);
    //Create STAN client
    let stan_client = StanClient::from_options(opts).await?;
    
    //Subscribe to STAN subject 'foo'
    let (sid, mut subscription) = stan_client.subscribe("foo", None, None).await?;
    tokio::spawn(async move {
        while let Some(message) = subscription.next().await {
            info!(" << 1 >> got stan message --- {:?}\n\t{:?}", &message,
                  String::from_utf8_lossy(message.payload.as_ref()));
        }
        info!(" ----- the subscription loop is done ---- ")
    });
    
    //Publish some mesesages to 'foo', use 'cargo run --example stan_publish foo "hi there"' 
    use std::{thread, time};
    thread::sleep(time::Duration::from_secs(60));
    
    //Unsubscribe 
    let _ = stan_client.un_subscribe(&sid).await;
    thread::sleep(time::Duration::from_secs(10));
    info!(" ---- done --- ");
    Ok(())
}    
```
#  Important Changes

## Version 0.2
All version 0.2.* related information is available here [Version 0.2.*](https://github.com/mnetship/ratsio/tree/ratsio-0.2https://github.com/mnetship/ratsio/tree/ratsio-0.2). 

## Version 0.3.0-alpha.1
Breaking API changes from 0.2
This is the first async/await compatible version, it's not production ready yet, still work in progress.
See examples in examples/ folder.


# Contact
For bug reports, patches, feature requests or other messages, please send a mail to michael@zulzi.com

# License
This project is licensed under the MIT License.


