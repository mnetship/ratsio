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
    //E.g. use 'cargo run --example nats_publish foo "hi there"'
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