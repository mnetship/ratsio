use log::LevelFilter;
use chrono::Local;
use std::io::Write;
use env_logger::Builder;


pub fn setup() {
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
}