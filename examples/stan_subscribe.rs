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