# Ratsio

Ratsio is a Rust client library for [NATS](https://nats.io) messaging system and [NATS Event Streaming](https://nats.io/documentation/streaming/nats-streaming-intro/).

Inspired by [nitox](https://raw.githubusercontent.com/YellowInnovation/nitox) and [rust-nats](https://github.com/jedisct1/rust-nats) but my project needed NATS streaming, so I couldn't use any of those 2. If this project is useful to you, feel free to contribute or suggest features. at the moment it's just the features I need. Add the following to your Cargo.toml.

```rust
[dependencies]
ratsio = "^0.1"
```
Rust -stable, -beta and -nightly are supported.

## Features:
- [x] Nats messaging queue. Publish, Subcribe and Request.
- [x] Nats cluster support, auto reconnect and dynamic cluster hosts update.
- [x] Async from the ground up, using  [tokio](https://crates.io/crates/tokio) and [futures](https://crates.io/crates/futures).
- [ ] TLS mode
- [ ] Authentication
- [x] NATS Streaming Server
# Usage

Subscribing and Publishing to a NATS subject: see tests/nats_client_test.rs
```rust
    let mut runtime = Runtime::new().unwrap();
    let connect_cmd = Connect::builder().build().unwrap();
    let options = NatsClientOptions::builder()
        .connect(connect_cmd)
        .cluster_uris(vec!(String::from("127.0.0.1:4222")))
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
        .connect(Connect::builder().build().unwrap())
        .cluster_uris(vec!(String::from("127.0.0.1:4222")))
        .build()
        .unwrap();
    let stan_options = StanOptions::builder()
        .nats_options(nats_options)
        .cluster_id("test-cluster".into())
        .client_id("main-1".into()).build()
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


# Contact
For bug reports, patches, feature requests or other messages, please send a mail to michael@zulzi.com

# License
This project is licensed under the MIT License.
