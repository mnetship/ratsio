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
    if args.len() != 3 {
        eprintln!("Usage: {} <subject> <message>", args[0]);
        return Err(RatsioError::GenericError("Invalid input".into()))
    }

    let client_id = ratsio::nuid::next();
    let opts = StanOptions::with_options("localhost:4222", "test-cluster", &client_id[..]);
    let stan_client = StanClient::from_options(opts).await?;
    let _ = stan_client.publish(args[1].clone(), args[2].as_bytes()).await?;
    let _ = stan_client.close().await?;
    Ok(())
}