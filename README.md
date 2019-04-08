# Ratsio

Ratsio is a Rust client library for [NATS](https://nats.io) messaging system and [NATS Event Streaming](https://nats.io/documentation/streaming/nats-streaming-intro/).

Inspired by [nitox](https://raw.githubusercontent.com/YellowInnovation/nitox) and [rust-nats](https://github.com/jedisct1/rust-nats) but my project needed NATS streaming, so I couldn't use any of those 2. If this project is useful to you, feel free to contribute or suggest features. at the moment it's just the features I need.

Add the following to your Cargo.toml.

```rust
[dependencies]
ratsio = "^0.2"
```
Rust -stable, -beta and -nightly are supported.

## Features:
- [x] Nats messaging queue. Publish, Subcribe and Request.
- [x] Nats cluster support, auto reconnect and dynamic cluster hosts update.
- [x] Async from the ground up, using  [tokio](https://crates.io/crates/tokio) and [futures](https://crates.io/crates/futures).
- [x] TLS mode
- [x] NATS 1.x Authentication
- [x] NATS 2.0 JWT-based client authentication
- [x] NATS Streaming Server
# Usage

Subscribing and Publishing to a NATS subject: see tests/nats_client_test.rs
```rust
    let mut runtime = Runtime::new().unwrap();
    let options = NatsClientOptions::builder()
        .cluster_uris(vec!("127.0.0.1:4222".to_string()))
        .build()
        .unwrap();

    let fut = NatsClient::from_options(options)
        .and_then(|client| NatsClient::connect(&client))
        .and_then(|client| {
            client
                .subscribe(Subscribe::builder().subject("foo".into()).build().unwrap())
                .map_err(|_| RatsioError::InnerBrokenChain)
                .and_then(move |stream| {
                    let _ = client
                        .publish(Publish::builder().subject("foo".into()).payload(Vec::from(&b"bar"[..])).build().unwrap())
                        .wait();

                    stream
                        .take(1)
                        .into_future()
                        .map(|(maybe_message, _)| maybe_message.unwrap())
                        .map_err(|_| RatsioError::InnerBrokenChain)
                })
        });

    let (tx, rx) = oneshot::channel();
    runtime.spawn(fut.then(|r| tx.send(r).map_err(|e| panic!("Cannot send Result {:?}", e))));
    let connection_result = rx.wait().expect("Cannot wait for a result");
    let _ = runtime.shutdown_now().wait();
    info!(target: "ratsio", "can_sub_and_pub::connection_result {:#?}", connection_result);
    assert!(connection_result.is_ok());
    let msg = connection_result.unwrap();
    assert_eq!(msg.payload, Vec::from(&b"bar"[..]));
```

Subscribing and Publishing to a NATS streaming subject: see tests/stan_client_test.rs
``` rust
    let mut runtime = Runtime::new().unwrap();
    let nats_options = NatsClientOptions::builder()
        .cluster_uris(vec!("127.0.0.1:4222".to_string()))
        .build()
        .unwrap();
    let stan_options = StanOptions::builder()
        .nats_options(nats_options)
        .cluster_id("test-cluster")
        .client_id("main-1").build()
        .unwrap();
    let (result_tx, result_rx) = mpsc::unbounded();

    let (stan_client_tx, stan_client_rx) = oneshot::channel();
    let subject: String = "test.subject".into();
    let subject1 = subject.clone();
    let subject2 = subject.clone();
    let program = StanClient::from_options(stan_options)
        .and_then(move |stan_client| {
            let sub = StanSubscribe::builder()
                .subject(subject1.clone())
                .start_position(StartPosition::NewOnly)
                .build().unwrap();
            stan_client
                .subscribe(sub, SyncHandler(Box::new(move |stan_msg: StanMessage| {
                    info!(target: "ratsio", " GOT :::: {:?}", stan_msg);
                    tokio::spawn(result_tx.clone().send(stan_msg).into_future()
                        .map(|_| ()).map_err(|_| ()));
                    Ok(())
                })))
                .and_then(move |_| Ok(stan_client))
        })
        .and_then(move |stan_client| {
            let stan_msg = StanMessage::new(subject2.clone(), Vec::from(&b"hello"[..]));
            stan_client.send(stan_msg).map(|_| stan_client)
        })
        .map_err(|_| ())
        .and_then(|stan_client| {
            stan_client_tx.send(stan_client)
                .into_future()
                .map(|_| ())
                .map_err(|_| ())
        }).map_err(|_| ());

    runtime.spawn(program);

    let stan_client = stan_client_rx.wait().expect(" No STAN Client");

    match result_rx.wait().next().expect("Cannot wait for a result") {
        Ok(stan_msg) => {
            info!(target: "ratsio", "Got stan_msg {:?}", stan_msg);
            assert_eq!(stan_msg.subject, subject);
            assert_eq!(stan_msg.payload, Vec::from(&b"hello"[..]));
        }
        Err(_) => {
            assert!(false);
        }
    };

    let (close_tx, close_rx) = oneshot::channel();
    runtime.spawn(stan_client.close()
        .and_then(|_| close_tx
            .send(true).into_future()
            .map(|_| ())
            .map_err(|_| ()))
    );

    let _ = close_rx.wait().expect(" Could not close STAN Client");
    let _ = runtime.shutdown_now().wait();
```
#  Important Changes

### Version 0.2
Users no longer need to use ratsio::ops::Connect struct when configuring a connection. Options are now availabble
on NatsClientOptions, username, password, tls_required, auth_token, etc

For example
``` rust
    let nats_options = NatsClientOptions::builder()
        .username("user")
        .password("password")
        .cluster_uris(vec!("127.0.0.1:4222"))
        .build()
        .unwrap();
```
Internal nuid fork from [nuid](https://github.com/casualjim/rs-nuid) upgraded to use [rand](https://crates.io/crates/rand) version ^0.6

### Version 0.2.1
More ergonomics when creating options. A bit easier on the eye.

It is now possible to pass either of String, &str, Vec<String> or Vec<&str> to cluster_uris(...) on NatsClientOptions::builder(), use
```use ratsio::prelude::VecUri;``` or just ```use ratsio::prelude::*;```
``` rust
    let nats_options = NatsClientOptions::builder()
        .username("user")
        .password("password")
        .cluster_uris(vec!("127.0.0.1:4222"))
        .build()
        .unwrap();
```
or
``` rust
    let nats_options = NatsClientOptions::builder()
        .username("user")
        .password("password")
        .cluster_uris("127.0.0.1:4222") // For 1 url
        .build()
        .unwrap();
```

# Version 0.3.0 
Merged the `from_options` and `connect` methods because `from_options` was actually making a TCP connection, and so its 
name was misleading. Further, the new client now properly awaits the `INFO` pre-amble from the server before supplying the 
`CONNECT` message.

Added support for NATS 2.0 client authentication via JWTs and [nkeys](https://github.com/nats-io/nkeys). You can now pass a 
`UserJWT` option along with a callback used to sign a `nonce` (a small random string the server uses to verify that the client
does possess the private key), as shown in the following example:

```rust
    let raw_jwt = String::from("--Encoded JWT goes here--");
    let opt_jwt = UserJWT::new(raw_jwt, Arc::new(sign_nonce));
    
    let mut runtime = Runtime::new().unwrap();
    let options = NatsClientOptions::builder()
        .cluster_uris(vec!["127.0.0.1:4222"])
        .user_jwt(opt_jwt)
        .build()
        .unwrap();

    let client = NatsClient::connect(options);
```

And here's what a sample `sign_nonce` function looks like:

```rust
fn sign_nonce(nonce: &[u8]) -> Result<Vec<u8>, Box<std::error::Error>> { 
    let raw_nkey = "--secret/seed key goes here--";
    let kp = nkeys::KeyPair::from_seed(raw_nkey).unwrap();
    Ok(kp.sign(nonce).unwrap())
}
```

At the moment, you have to define your own callback to ensure that your code can manage the lifetime of the seed key in a 
way that is hopefully short-lived. If the NATS client library managed your seed key lifetime, it would have to enforce a
`'static` guarantee, which isn't the most secure approach.

# Contact
For bug reports, patches, feature requests or other messages, please send a mail to michael@zulzi.com

# License
This project is licensed under the MIT License.


