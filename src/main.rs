use std::sync::mpsc;
use std::time::{Duration, Instant};

use db_explorer::app::DbExplorerApp;
use db_explorer::aws::dynamodb::DynamoDbService;
use db_explorer::messages::{WorkerCommand, WorkerEvent};
use tokio::task::JoinHandle;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

const COMMAND_CHANNEL_CAPACITY: usize = 32;
const INIT_RETRY_COOLDOWN: Duration = Duration::from_secs(5);
const CLIENT_UNAVAILABLE_MSG: &str =
    "DynamoDB client is unavailable. Check AWS region/credentials and retry.";

#[tokio::main]
async fn main() -> Result<(), eframe::Error> {
    init_tracing();
    info!("starting DynamoDB Explorer");

    let (command_tx, mut command_rx) =
        tokio::sync::mpsc::channel::<WorkerCommand>(COMMAND_CHANNEL_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>();

    tokio::spawn(async move {
        info!("starting DynamoDB worker");
        let mut service = match DynamoDbService::new().await {
            Ok(service) => Some(service),
            Err(err) => {
                let message = err.to_string();
                error!(error = %message, "worker: failed to initialize DynamoDB service");
                None
            }
        };
        let mut next_allowed_init_retry = Instant::now();
        let mut active_metadata_task: Option<JoinHandle<()>> = None;

        while let Some(command) = command_rx.recv().await {
            if active_metadata_task
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
            {
                active_metadata_task = None;
            }

            if service.is_none() && Instant::now() >= next_allowed_init_retry {
                info!("worker: retrying DynamoDB service initialization");
                service = match DynamoDbService::new().await {
                    Ok(service) => {
                        info!("worker: DynamoDB service initialized successfully");
                        Some(service)
                    }
                    Err(err) => {
                        error!(error = %err, "worker: DynamoDB service initialization retry failed");
                        next_allowed_init_retry = Instant::now() + INIT_RETRY_COOLDOWN;
                        None
                    }
                };
            }

            match command {
                WorkerCommand::LoadTables { request_id } => {
                    info!(request_id, "worker: loading table list");
                    let result = match service.as_ref() {
                        Some(service) => service.list_tables().await.map_err(|err| err.to_string()),
                        None => Err(CLIENT_UNAVAILABLE_MSG.to_string()),
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
                    if let Some(handle) = active_metadata_task.take() {
                        handle.abort();
                        info!("worker: aborted previous metadata load task");
                    }

                    let event_tx = event_tx.clone();
                    let service = service.clone();
                    let table_name_for_task = table_name.clone();
                    active_metadata_task = Some(tokio::spawn(async move {
                        let result = match service {
                            Some(service) => service
                                .load_table_metadata(&table_name_for_task, exact_item_count)
                                .await
                                .map_err(|err| err.to_string()),
                            None => Err(CLIENT_UNAVAILABLE_MSG.to_string()),
                        };

                        if let Err(err) = &result {
                            error!(request_id, table_name = %table_name_for_task, error = %err, "worker: failed to load table metadata");
                        }

                        if event_tx
                            .send(WorkerEvent::TableMetadataLoaded {
                                request_id,
                                table_name,
                                result,
                            })
                            .is_err()
                        {
                            info!("worker: event channel closed, stopping metadata task");
                        }
                    }));
                }
                WorkerCommand::CancelMetadataLoad { request_id } => {
                    if let Some(handle) = active_metadata_task.take() {
                        handle.abort();
                        info!(request_id, "worker: canceled metadata load task");
                    } else {
                        info!(request_id, "worker: no active metadata load task to cancel");
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
