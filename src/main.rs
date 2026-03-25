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
        if let Err(err) = &service {
            error!(error = %err, "worker: failed to initialize DynamoDB service");
        }

        while let Some(command) = command_rx.recv().await {
            match command {
                WorkerCommand::LoadTables { request_id } => {
                    info!(request_id, "worker: loading table list");
                    let result = match &service {
                        Ok(service) => service.list_tables().await.map_err(|err| err.to_string()),
                        Err(err) => Err(err.to_string()),
                    };
                    if let Err(err) = &result {
                        error!(request_id, error = %err, "worker: failed to load table list");
                    }

                    if event_tx
                        .send(WorkerEvent::TablesLoaded { request_id, result })
                        .is_err()
                    {
                        info!("worker: event channel closed, stopping worker");
                        break;
                    }
                }
                WorkerCommand::LoadTableMetadata {
                    request_id,
                    table_name,
                    exact_item_count,
                } => {
                    info!(request_id, table_name = %table_name, exact_item_count, "worker: loading table metadata");
                    let result = match &service {
                        Ok(service) => service
                            .load_table_metadata(&table_name, exact_item_count)
                            .await
                            .map_err(|err| err.to_string()),
                        Err(err) => Err(err.to_string()),
                    };
                    if let Err(err) = &result {
                        error!(request_id, table_name = %table_name, error = %err, "worker: failed to load table metadata");
                    }

                    if event_tx
                        .send(WorkerEvent::TableMetadataLoaded {
                            request_id,
                            table_name,
                            result,
                        })
                        .is_err()
                    {
                        info!("worker: event channel closed, stopping worker");
                        break;
                    }
                }
            }
        }
    });

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
