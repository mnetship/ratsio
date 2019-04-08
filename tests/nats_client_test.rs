#[macro_use]
extern crate log;

use futures::{prelude::*, sync::oneshot};
use ratsio::error::RatsioError;
use ratsio::nats_client::*;
use ratsio::ops::*;
use tokio::runtime::Runtime;

use std::sync::Arc;

mod common;

#[test]
fn test_pub_sub() {
    common::setup();

    let mut runtime = Runtime::new().unwrap();
    let options = NatsClientOptions::builder()
        .username("")
        .cluster_uris("localhost:4222")
        .build()
        .unwrap();

    let fut = NatsClient::connect(options).and_then(|client| {
        client
            .subscribe(Subscribe::builder().subject("foo".into()).build().unwrap())
            .map_err(|_| RatsioError::InnerBrokenChain)
            .and_then(move |stream| {
                let _ = client
                    .publish(
                        Publish::builder()
                            .subject("foo".into())
                            .payload(Vec::from(&b"bar"[..]))
                            .build()
                            .unwrap(),
                    )
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
}

#[test]
fn test_connect() {
    common::setup();
    let mut runtime = Runtime::new().unwrap();
    let options = NatsClientOptions::builder()
        .cluster_uris(vec!["127.0.0.1:4222", "192.168.0.253:4222"])
        .build()
        .unwrap();

    let connection = NatsClient::connect(options);
    let (tx, rx) = oneshot::channel();
    runtime
        .spawn(connection.then(|r| tx.send(r).map_err(|e| panic!("Cannot send Result {:?}", e))));
    let connection_result = rx.wait().expect("Cannot wait for a result");
    let _ = runtime.shutdown_now().wait();
    info!(target: "ratsio", "can_connect::connection_result {:#?}", connection_result);
    assert!(connection_result.is_ok());
}

#[test]
fn test_request() {
    common::setup();
    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let options = NatsClientOptions::builder()
        .cluster_uris(vec![String::from("127.0.0.1:4222")])
        .build()
        .unwrap();

    let fut = NatsClient::connect(options)
        //.and_then(|client| NatsClient::connect(&client))
        .map(|client: Arc<NatsClient>| {
            let sub = Subscribe::builder().subject("foo2".into()).build().unwrap();
            let sender = client.sender.clone();
            tokio::spawn(
                client
                    .subscribe(sub)
                    .and_then(|stream| {
                        stream.for_each(move |msg| {
                            match msg.reply_to {
                                Some(reply_to) => {
                                    let publ = Publish::builder()
                                        .subject(reply_to)
                                        .payload(Vec::from(&b"bar"[..]))
                                        .build()
                                        .unwrap();
                                    sender.read().send(Op::PUB(publ));
                                }
                                _ => {}
                            };
                            Ok(())
                        })
                    })
                    .map_err(|_| ()),
            );
            client
        })
        .and_then(|client| client.request("foo2".into(), "foo".as_bytes()));

    let (tx, rx) = oneshot::channel();
    runtime.spawn(fut.then(|r| tx.send(r).map_err(|e| panic!("Cannot send Result {:?}", e))));
    let connection_result = rx.wait().expect("Cannot wait for a result");
    let _ = runtime.shutdown_now().wait();
    info!(target: "ratsio", "can_request::connection_result {:#?}", connection_result);
    assert!(connection_result.is_ok());
    let msg = connection_result.unwrap();
    info!(target: "ratsio", "can_request::msg {:#?}", msg);
    assert_eq!(msg.payload, Vec::from(&b"bar"[..]));
}
