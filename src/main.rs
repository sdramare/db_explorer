use std::sync::mpsc;

use db_explorer::app::DbExplorerApp;
use db_explorer::messages::{WorkerCommand, WorkerEvent};
use db_explorer::worker::spawn_worker;
use tracing::info;
use tracing_subscriber::EnvFilter;

const COMMAND_CHANNEL_CAPACITY: usize = 32;

#[tokio::main]
async fn main() -> Result<(), eframe::Error> {
    init_tracing();
    info!("starting DynamoDB Explorer");

    let (command_tx, command_rx) =
        tokio::sync::mpsc::channel::<WorkerCommand>(COMMAND_CHANNEL_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>();

    spawn_worker(command_rx, event_tx);

    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "DynamoDB Explorer",
        options,
        Box::new(move |_cc| Ok(Box::new(DbExplorerApp::new(command_tx, event_rx)))),
    )
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("db_explorer=info,warn"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();
}
