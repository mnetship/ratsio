#[macro_use]
extern crate log;

use futures::{prelude::*, sync::oneshot};
use ratsio::nats_client::*;
use tokio::runtime::Runtime;

use std::sync::Arc;

mod common;

// NOTE: only run this test when you're using a gnatsd instance >= v2.0 and it was configured
// with the nats2_server.conf file. This configuration file contains a root operator (homelab_operator),
// a single account, and a single user (used for the following test).
#[test]
fn test_nats2_connect() {
    let raw_jwt = String::from("eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJQSFpSVjJRVDQzNkVMUEZYSFZCV01aT0xGQ0RDSU1FNzZQTEdJNFZMRzc1TzdMMkhaRU5RIiwiaWF0IjoxNTU0Mzc4Nzg1LCJpc3MiOiJBREZZRzNZWVNKVUVIS01LTVg3NzdDM082VExWSE8yQkdNVlFGQkVFUVNOUlVFTktIQUlZMkU1RSIsIm5hbWUiOiJob21lbGFiX3VzZXIiLCJzdWIiOiJVQU1DVzVCT1pSQ1BaS0JYS0tRUktUR0I2TEpYSVVFT0U2NDNRSlNXTlFDRVQ1WUhSWEI0VjdSQyIsInR5cGUiOiJ1c2VyIiwibmF0cyI6eyJwdWIiOnt9LCJzdWIiOnt9fX0.EPs_K7vpKSURcjxEWNEL22l5CfPMNQM15IULvuSY03US5gtI4MAtl9R6shhKQy11k126LnAr1kj3V7cAVZwpAw");

    let opt_jwt = UserJWT::new(raw_jwt, Arc::new(sign_nonce));

    common::setup();
    let mut runtime = Runtime::new().unwrap();
    let options = NatsClientOptions::builder()
        .cluster_uris(vec!["127.0.0.1:4222"])
        .user_jwt(opt_jwt)
        .build()
        .unwrap();

    let client = NatsClient::connect(options);
    let (tx, rx) = oneshot::channel();
    runtime.spawn(client.then(|r| tx.send(r).map_err(|e| panic!("Cannot send Result {:?}", e))));
    let connection_result = rx.wait().expect("Cannot wait for a result");
    std::thread::sleep(std::time::Duration::from_millis(600));
    let _ = runtime.shutdown_now().wait();
    info!(target: "ratsio", "can_connect::connection_result {:#?}", connection_result);
    assert!(connection_result.is_ok());
}

// Callback to sign the nonce. In real applications, key should be injected via environment
// variables or pulled from a vault, etc.
fn sign_nonce(nonce: &[u8]) -> Result<Vec<u8>, Box<std::error::Error>> {
    info!(target: "ratsio", "Signing nonce!");
    let raw_nkey = "SUAGMSQYN72ENPO3NVRLNDIDF4LQ2SZXSV23CHCN3LGE44FQUG2YGEBMUI";
    let kp = nkeys::KeyPair::from_seed(raw_nkey).unwrap();
    Ok(kp.sign(nonce).unwrap())
}
