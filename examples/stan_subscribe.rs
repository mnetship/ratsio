use log::info;
use futures::StreamExt;
use ratsio::{RatsioError, StanClient, StanOptions};
use std::env;

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
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <subject>", args[0]);
        return Err(RatsioError::GenericError("Invalid input".into()));
    }

    // Create stan options
    let client_id = "test1".to_string();
    let opts = StanOptions::with_options("localhost:4222", "test-cluster", &client_id[..]);
    //Create STAN client
    let stan_client = StanClient::from_options(opts).await?;

    let subject = args[1].clone();

    //Subscribe to STAN subject 'foo'
    let (sid, mut subscription) = stan_client.subscribe(subject.clone(), None,
                                                        Some(format!("durable-{}", subject))).await?;

    ctrlc::set_handler(move || {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let _ = runtime.block_on(stan_client.un_subscribe(&sid));
    }).expect("Error setting Ctrl-C handler");

    while let Some(message) = subscription.next().await {

        info!("{:?}\n\t{:?}", &message,
              String::from_utf8_lossy(message.payload.as_ref()));
    }
    Ok(())
}