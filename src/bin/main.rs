#[macro_use]
extern crate log;

use chrono::Local;
use env_logger::Builder;
use futures::{
    prelude::*,
};
use futures::{
    sync::{mpsc, oneshot},
};
use log::LevelFilter;
use ratsio::nats_client::*;
use ratsio::stan_client::*;
use std::io::Write;
use tokio::runtime::Runtime;

// RUST_BACKTRACE=1 RUST_LOG=ratsio=debug cargo run --
fn main() {
    let mut runtime = Runtime::new().unwrap();
    let _ = Builder::new()
        .format(|buf, record| {
            writeln!(buf,
                     "{} [{}] - {}",
                     Local::now().format("%Y-%m-%dT%H:%M:%S"),
                     record.level(),
                     record.args()
            )
        })
        .filter(None, LevelFilter::Info)
        .try_init();

    let nats_options = NatsClientOptions::builder()
        .cluster_uris(vec!("127.0.0.1:4222"))
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
                .start_position(StartPosition::First)
                .build().unwrap();
            stan_client
                .subscribe(sub, SyncHandler(Box::new(move |stan_msg: StanMessage| {
                    info!(target: "ratsio", " ------------------- GOT :::: {:?}", stan_msg);
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
            info!(target: "ratsio", " -------------------:::: {:?}", stan_msg);
            assert_eq!(stan_msg.subject, subject);
            assert_eq!(stan_msg.payload, Vec::from(&b"hello"[..]));
        }
        Err(_) => {
            assert!(false);
        }
    };


    let (close_tx, close_rx) = oneshot::channel();
    runtime.spawn(stan_client.close()
        .and_then(|_| {
            close_tx
                .send(true).unwrap();
            Ok(())
        }).map_err(|_| ())
    );

    let _ = close_rx.wait().expect(" Could not close STAN Client");
    let _ = runtime.shutdown_now().wait();
}
