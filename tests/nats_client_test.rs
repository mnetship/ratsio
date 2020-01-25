use ratsio::nats_client::NatsClient;
use log::info;
use futures::StreamExt;
use ratsio::error::RatsioError;
use ratsio::protocol;
use ratsio::nuid;

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


#[tokio::test]
async fn test1() -> Result<(), RatsioError> {
    logger_setup();

    let nats_client = NatsClient::new("nats://localhost:4222").await?;
    let (sid, mut subscription) = nats_client.subscribe("foo").await?;
    tokio::spawn(async move {
        while let Some(message) = subscription.next().await {
            info!(" << 1 >> got message --- {:?}\n\t{:?}", &message,
                  String::from_utf8_lossy(message.payload.as_ref()));
        }
    });

    let (_sid, mut subscription2) = nats_client.subscribe("foo").await?;
    tokio::spawn(async move {
        while let Some(message) = subscription2.next().await {
            info!(" << 2 >> got message --- {:?}\n\t{:?}", &message,
                  String::from_utf8_lossy(message.payload.as_ref()));
        }
    });

    use std::{thread, time};
    thread::sleep(time::Duration::from_secs(5));
    let _ = nats_client.publish("foo", b"Publish Message 1").await?;
    thread::sleep(time::Duration::from_secs(1));
    let _ = nats_client.un_subscribe(&sid).await?;
    thread::sleep(time::Duration::from_secs(3));

    thread::sleep(time::Duration::from_secs(1));
    let _ = nats_client.publish("foo", b"Publish Message 2").await?;


    let _discover_subject = "_STAN.discover.test-cluster";
    let client_id = "test-1";
    let conn_id = nuid::next();
    let heartbeat_inbox = format!("_HB.{}", &conn_id);
    let _connect_request = protocol::ConnectRequest {
        client_id: client_id.into(),
        conn_id: conn_id.clone().as_bytes().into(),
        heartbeat_inbox: heartbeat_inbox.clone(),
        ..Default::default()
    };
    tokio::time::delay_for(std::time::Duration::from_secs(2)).await;
    Ok(())
}