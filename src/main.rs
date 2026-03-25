use std::sync::mpsc;

use db_explorer::app::DbExplorerApp;
use db_explorer::aws::dynamodb::DynamoDbService;
use db_explorer::messages::{WorkerCommand, WorkerEvent};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), eframe::Error> {
    init_tracing();
    info!("starting DynamoDB Explorer");

    let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel::<WorkerCommand>();
    let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>();

    tokio::spawn(async move {
        info!("starting DynamoDB worker");
        let service = DynamoDbService::new().await;

        while let Some(command) = command_rx.recv().await {
            match command {
                WorkerCommand::LoadTables { request_id } => {
                    info!(request_id, "worker: loading table list");
                    let result = service.list_tables().await;
                    if let Err(err) = &result {
                        error!(request_id, error = %err, "worker: failed to load table list");
                    }

                    let _ = event_tx.send(WorkerEvent::TablesLoaded { request_id, result });
                }
                WorkerCommand::LoadTableMetadata {
                    request_id,
                    table_name,
                } => {
                    info!(request_id, table_name = %table_name, "worker: loading table metadata");
                    let result = service.load_table_metadata(&table_name).await;
                    if let Err(err) = &result {
                        error!(request_id, table_name = %table_name, error = %err, "worker: failed to load table metadata");
                    }

                    let _ = event_tx.send(WorkerEvent::TableMetadataLoaded {
                        request_id,
                        table_name,
                        result,
                    });
                }
            }
        }
    });

    let options = eframe::NativeOptions::default();
    let app_tx = command_tx.clone();
    eframe::run_native(
        "DynamoDB Explorer",
        options,
        Box::new(move |_cc| Ok(Box::new(DbExplorerApp::new(app_tx.clone(), event_rx)))),
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
