#[macro_use]
extern crate log;

use futures::{
    prelude::*,
    sync::{mpsc, oneshot},
};
use ratsio::nats_client::*;
use ratsio::ops::*;
use ratsio::stan_client::*;
use tokio::{
    runtime::Runtime,
};

mod common;

#[test]
fn test_stan_pub_sub() {
    common::setup();
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
                    info!(target: "ratsio", "GOT stan_msg {:?}", stan_msg);
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

    let stan_client = stan_client_rx.wait().expect("No STAN Client");

    match result_rx.wait().next().expect("Cannot wait for a result") {
        Ok(stan_msg) => {
            info!(target: "ratsio", "Got stan_msg => {:?}", stan_msg);
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

    let _ = close_rx.wait().expect("Could not close STAN Client");
    let _ = runtime.shutdown_now().wait();
}

