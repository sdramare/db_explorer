use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::aws::dynamodb::DynamoDbService;
use crate::messages::{WorkerCommand, WorkerEvent};
use tokio::task::JoinHandle;
use tracing::{error, info};

const INIT_RETRY_COOLDOWN: Duration = Duration::from_secs(5);
const CLIENT_UNAVAILABLE_MSG: &str =
    "DynamoDB client is unavailable. Check AWS region/credentials and retry.";

struct WorkerRuntime {
    service: Option<DynamoDbService>,
    next_allowed_init_retry: Instant,
    active_metadata_task: Option<JoinHandle<()>>,
    active_export_task: Option<JoinHandle<()>>,
}

impl WorkerRuntime {
    fn new(service: Option<DynamoDbService>) -> Self {
        Self {
            service,
            next_allowed_init_retry: Instant::now(),
            active_metadata_task: None,
            active_export_task: None,
        }
    }

    fn prune_finished_tasks(&mut self) {
        if self
            .active_metadata_task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            self.active_metadata_task = None;
        }
        if self
            .active_export_task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            self.active_export_task = None;
        }
    }

    async fn maybe_retry_service_init(&mut self) {
        if self.service.is_some() || Instant::now() < self.next_allowed_init_retry {
            return;
        }

        info!("worker: retrying DynamoDB service initialization");
        self.service = match DynamoDbService::new().await {
            Ok(service) => {
                info!("worker: DynamoDB service initialized successfully");
                Some(service)
            }
            Err(err) => {
                error!(error = %err, "worker: DynamoDB service initialization retry failed");
                self.next_allowed_init_retry = Instant::now() + INIT_RETRY_COOLDOWN;
                None
            }
        };
    }

    async fn handle_command(
        &mut self,
        command: WorkerCommand,
        event_tx: &mpsc::Sender<WorkerEvent>,
    ) -> bool {
        match command {
            WorkerCommand::LoadTables { request_id } => {
                self.handle_load_tables(request_id, event_tx).await
            }
            WorkerCommand::LoadTableMetadata {
                request_id,
                table_name,
                exact_item_count,
            } => {
                self.handle_load_table_metadata(request_id, table_name, exact_item_count, event_tx);
                true
            }
            WorkerCommand::CancelMetadataLoad { request_id } => {
                self.handle_cancel_metadata_load(request_id);
                true
            }
            WorkerCommand::ExportTableToJson {
                request_id,
                table_name,
                output_path,
                pretty_print,
            } => {
                self.handle_export_table(
                    request_id,
                    table_name,
                    output_path,
                    pretty_print,
                    event_tx,
                );
                true
            }
        }
    }

    async fn handle_load_tables(
        &self,
        request_id: u64,
        event_tx: &mpsc::Sender<WorkerEvent>,
    ) -> bool {
        info!(request_id, "worker: loading table list");
        let result = match self.service.as_ref() {
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
            return false;
        }

        true
    }

    fn handle_load_table_metadata(
        &mut self,
        request_id: u64,
        table_name: String,
        exact_item_count: bool,
        event_tx: &mpsc::Sender<WorkerEvent>,
    ) {
        info!(request_id, table_name = %table_name, exact_item_count, "worker: loading table metadata");
        if let Some(handle) = self.active_metadata_task.take() {
            handle.abort();
            info!("worker: aborted previous metadata load task");
        }

        let event_tx = event_tx.clone();
        let service = self.service.clone();
        let table_name_for_task = table_name.clone();
        self.active_metadata_task = Some(tokio::spawn(async move {
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

    fn handle_cancel_metadata_load(&mut self, request_id: u64) {
        if let Some(handle) = self.active_metadata_task.take() {
            handle.abort();
            info!(request_id, "worker: canceled metadata load task");
        } else {
            info!(request_id, "worker: no active metadata load task to cancel");
        }
    }

    fn handle_export_table(
        &mut self,
        request_id: u64,
        table_name: String,
        output_path: std::path::PathBuf,
        pretty_print: bool,
        event_tx: &mpsc::Sender<WorkerEvent>,
    ) {
        info!(request_id, table_name = %table_name, output_path = %output_path.display(), "worker: exporting table to json");
        if let Some(handle) = self.active_export_task.take() {
            handle.abort();
            info!("worker: aborted previous export task");
        }

        let event_tx = event_tx.clone();
        let service = self.service.clone();
        let table_name_for_task = table_name.clone();
        let output_path_for_task = output_path.clone();
        self.active_export_task = Some(tokio::spawn(async move {
            let result = match service {
                Some(service) => service
                    .export_table_to_json_file(
                        &table_name_for_task,
                        &output_path_for_task,
                        pretty_print,
                    )
                    .await
                    .map_err(|err| err.to_string()),
                None => Err(CLIENT_UNAVAILABLE_MSG.to_string()),
            };

            if let Err(err) = &result {
                error!(request_id, table_name = %table_name_for_task, output_path = %output_path_for_task.display(), error = %err, "worker: failed to export table");
            }

            if event_tx
                .send(WorkerEvent::TableExported {
                    request_id,
                    table_name,
                    output_path,
                    result,
                })
                .is_err()
            {
                info!("worker: event channel closed, stopping export task");
            }
        }));
    }
}

pub fn spawn_worker(
    mut command_rx: tokio::sync::mpsc::Receiver<WorkerCommand>,
    event_tx: mpsc::Sender<WorkerEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting DynamoDB worker");
        let initial_service = match DynamoDbService::new().await {
            Ok(service) => Some(service),
            Err(err) => {
                let message = err.to_string();
                error!(error = %message, "worker: failed to initialize DynamoDB service");
                None
            }
        };
        let mut runtime = WorkerRuntime::new(initial_service);

        while let Some(command) = command_rx.recv().await {
            runtime.prune_finished_tasks();
            runtime.maybe_retry_service_init().await;

            if !runtime.handle_command(command, &event_tx).await {
                break;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::WorkerRuntime;
    use crate::messages::{WorkerCommand, WorkerEvent};

    fn recv_event(event_rx: &mpsc::Receiver<WorkerEvent>) -> WorkerEvent {
        event_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("expected worker event")
    }

    #[tokio::test]
    async fn load_tables_without_service_emits_unavailable_error() {
        let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>();
        let mut runtime = WorkerRuntime::new(None);

        let should_continue = runtime
            .handle_command(WorkerCommand::LoadTables { request_id: 7 }, &event_tx)
            .await;

        assert!(should_continue);
        match recv_event(&event_rx) {
            WorkerEvent::TablesLoaded { request_id, result } => {
                assert_eq!(request_id, 7);
                assert!(result.is_err());
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn load_metadata_without_service_emits_unavailable_error() {
        let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>();
        let mut runtime = WorkerRuntime::new(None);

        let should_continue = runtime
            .handle_command(
                WorkerCommand::LoadTableMetadata {
                    request_id: 11,
                    table_name: "users".to_string(),
                    exact_item_count: false,
                },
                &event_tx,
            )
            .await;

        if let Some(handle) = runtime.active_metadata_task.take() {
            let _ = handle.await;
        }

        assert!(should_continue);
        match recv_event(&event_rx) {
            WorkerEvent::TableMetadataLoaded {
                request_id,
                table_name,
                result,
            } => {
                assert_eq!(request_id, 11);
                assert_eq!(table_name, "users");
                assert!(result.is_err());
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn export_without_service_emits_unavailable_error() {
        let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>();
        let mut runtime = WorkerRuntime::new(None);

        let should_continue = runtime
            .handle_command(
                WorkerCommand::ExportTableToJson {
                    request_id: 19,
                    table_name: "orders".to_string(),
                    output_path: PathBuf::from("/tmp/orders.json"),
                    pretty_print: true,
                },
                &event_tx,
            )
            .await;

        if let Some(handle) = runtime.active_export_task.take() {
            let _ = handle.await;
        }

        assert!(should_continue);
        match recv_event(&event_rx) {
            WorkerEvent::TableExported {
                request_id,
                table_name,
                output_path,
                result,
            } => {
                assert_eq!(request_id, 19);
                assert_eq!(table_name, "orders");
                assert_eq!(output_path, PathBuf::from("/tmp/orders.json"));
                assert!(result.is_err());
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_metadata_load_clears_active_task() {
        let (event_tx, _event_rx) = mpsc::channel::<WorkerEvent>();
        let mut runtime = WorkerRuntime::new(None);
        runtime.active_metadata_task = Some(tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }));

        runtime
            .handle_command(
                WorkerCommand::CancelMetadataLoad { request_id: 99 },
                &event_tx,
            )
            .await;

        assert!(runtime.active_metadata_task.is_none());
    }
}
