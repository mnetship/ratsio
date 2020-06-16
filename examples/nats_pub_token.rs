use ratsio::{RatsioError, NatsClient, NatsClientOptions};
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
    if args.len() != 3 {
        eprintln!("Usage: {} <subject> <message>", args[0]);
        return Err(RatsioError::GenericError("Invalid input".into()))
    }

    let mut options = NatsClientOptions::default();
    //options.username = "user".into();
    //options.password = "passwd".into();
    options.auth_token = "dag0HTXl4RGg7dXdaJwbC8".into();
    options.cluster_uris =vec!["nats://localhost:4222".to_string()].into();
    let nats_client = NatsClient::new(options).await?;
    let _ = nats_client.publish(args[1].clone(), args[2].as_bytes()).await?;
    let _ = nats_client.close().await?;
    Ok(())
}