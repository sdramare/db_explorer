use std::sync::mpsc::Receiver;

use eframe::egui;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info, warn};

use crate::aws::dynamodb::TableMetadata;
use crate::messages::{WorkerCommand, WorkerEvent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataState {
    Idle,
    Loading,
    Loaded(TableMetadata),
    Error(String),
}

#[derive(Debug)]
pub struct AppState {
    pub tables: Vec<String>,
    pub selected_table: Option<String>,
    pub tables_loading: bool,
    pub tables_error: Option<String>,
    pub metadata_state: MetadataState,
    next_request_id: u64,
    active_tables_request: Option<u64>,
    active_metadata_request: Option<u64>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            selected_table: None,
            tables_loading: false,
            tables_error: None,
            metadata_state: MetadataState::Idle,
            next_request_id: 1,
            active_tables_request: None,
            active_metadata_request: None,
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        id
    }

    #[must_use]
    pub fn begin_tables_refresh(&mut self) -> u64 {
        let request_id = self.next_id();
        self.tables_loading = true;
        self.tables_error = None;
        self.active_tables_request = Some(request_id);
        request_id
    }

    pub fn fail_tables_refresh(&mut self, error: String) {
        self.tables_loading = false;
        self.active_tables_request = None;
        self.tables_error = Some(error);
    }

    #[must_use]
    pub fn begin_metadata_load(&mut self, table_name: String) -> u64 {
        let request_id = self.next_id();
        self.selected_table = Some(table_name);
        self.metadata_state = MetadataState::Loading;
        self.active_metadata_request = Some(request_id);
        request_id
    }

    pub fn fail_metadata_load(&mut self, error: String) {
        self.active_metadata_request = None;
        self.metadata_state = MetadataState::Error(error);
    }

    pub fn handle_event(&mut self, event: WorkerEvent) {
        match event {
            WorkerEvent::TablesLoaded { request_id, result } => {
                if self.active_tables_request != Some(request_id) {
                    return;
                }
                self.active_tables_request = None;
                self.tables_loading = false;

                match result {
                    Ok(tables) => {
                        self.tables = tables;
                        self.tables_error = None;
                        if let Some(selected) = &self.selected_table
                            && !self.tables.contains(selected)
                        {
                            self.selected_table = None;
                            self.metadata_state = MetadataState::Idle;
                        }
                    }
                    Err(err) => {
                        self.tables.clear();
                        self.tables_error = Some(err);
                        self.selected_table = None;
                        self.metadata_state = MetadataState::Idle;
                    }
                }
            }
            WorkerEvent::TableMetadataLoaded {
                request_id,
                table_name,
                result,
            } => {
                if self.active_metadata_request != Some(request_id) {
                    return;
                }
                self.active_metadata_request = None;
                self.selected_table = Some(table_name);

                match result {
                    Ok(metadata) => {
                        self.metadata_state = MetadataState::Loaded(metadata);
                    }
                    Err(err) => {
                        self.metadata_state = MetadataState::Error(err);
                    }
                }
            }
        }
    }

    pub fn has_pending_requests(&self) -> bool {
        self.active_tables_request.is_some() || self.active_metadata_request.is_some()
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct DbExplorerApp {
    state: AppState,
    command_tx: UnboundedSender<WorkerCommand>,
    event_rx: Receiver<WorkerEvent>,
}

impl DbExplorerApp {
    pub fn new(
        command_tx: UnboundedSender<WorkerCommand>,
        event_rx: Receiver<WorkerEvent>,
    ) -> Self {
        let mut app = Self {
            state: AppState::new(),
            command_tx,
            event_rx,
        };
        app.request_tables();
        app
    }

    fn request_tables(&mut self) {
        let request_id = self.state.begin_tables_refresh();
        info!(request_id, "ui: requesting table list");
        if let Err(err) = self
            .command_tx
            .send(WorkerCommand::LoadTables { request_id })
        {
            warn!(error = %err, "ui: failed to send table list request to worker");
            self.state.fail_tables_refresh(format!(
                "Unable to request table list: worker is unavailable ({err})"
            ));
        }
    }

    fn request_metadata(&mut self, table_name: String, exact_item_count: bool) {
        let request_id = self.state.begin_metadata_load(table_name.clone());
        info!(request_id, table_name = %table_name, exact_item_count, "ui: requesting table metadata");
        if let Err(err) = self.command_tx.send(WorkerCommand::LoadTableMetadata {
            request_id,
            table_name,
            exact_item_count,
        }) {
            warn!(error = %err, "ui: failed to send metadata request to worker");
            self.state.fail_metadata_load(format!(
                "Unable to request table metadata: worker is unavailable ({err})"
            ));
        }
    }

    fn drain_events(&mut self) -> bool {
        let mut received = false;
        while let Ok(event) = self.event_rx.try_recv() {
            debug!(?event, "ui: received worker event");
            self.state.handle_event(event);
            received = true;
        }

        received
    }

    fn render_metadata_panel(&self, ui: &mut egui::Ui) {
        ui.heading("Table Metadata");
        ui.separator();

        match &self.state.metadata_state {
            MetadataState::Idle => {
                ui.label("Select a table to view metadata.");
            }
            MetadataState::Loading => {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("Loading metadata...");
                });
            }
            MetadataState::Loaded(metadata) => {
                ui.label(format!("Name: {}", metadata.name));
                ui.label(format!(
                    "Date of creation: {}",
                    metadata.formatted_creation_date()
                ));
                let count_label = if metadata.item_count_is_exact {
                    "Number of items (exact)"
                } else {
                    "Number of items (approximate)"
                };
                ui.label(format!("{count_label}: {}", metadata.item_count));
            }
            MetadataState::Error(err) => {
                ui.colored_label(egui::Color32::from_rgb(180, 30, 30), err);
            }
        }
    }
}

impl eframe::App for DbExplorerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let events_processed = self.drain_events();

        egui::TopBottomPanel::top("toolbar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(
                        !self.state.tables_loading,
                        egui::Button::new("Refresh tables"),
                    )
                    .clicked()
                {
                    info!("ui: refresh tables clicked");
                    self.request_tables();
                }

                let selected_text = self
                    .state
                    .selected_table
                    .as_deref()
                    .unwrap_or("Select table");

                let mut pending_selection = self.state.selected_table.clone();
                let previous_selection = pending_selection.clone();
                egui::ComboBox::from_label("Tables")
                    .selected_text(selected_text)
                    .show_ui(ui, |ui| {
                        for table in &self.state.tables {
                            ui.selectable_value(&mut pending_selection, Some(table.clone()), table);
                        }
                    });
                self.state.selected_table = pending_selection;

                if self.state.selected_table != previous_selection
                    && let Some(selected) = self.state.selected_table.clone()
                {
                    info!(table_name = %selected, "ui: table selection changed");
                    self.request_metadata(selected, false);
                }

                if ui
                    .add_enabled(
                        self.state.selected_table.is_some()
                            && !matches!(self.state.metadata_state, MetadataState::Loading),
                        egui::Button::new("Recount exactly"),
                    )
                    .clicked()
                    && let Some(selected) = self.state.selected_table.clone()
                {
                    info!(table_name = %selected, "ui: exact recount requested");
                    self.request_metadata(selected, true);
                }
            });

            if self.state.tables_loading {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("Loading table list...");
                });
            }

            if let Some(err) = &self.state.tables_error {
                ui.colored_label(egui::Color32::from_rgb(180, 30, 30), err);
            }

            if !self.state.tables_loading
                && self.state.tables.is_empty()
                && self.state.tables_error.is_none()
            {
                ui.label("No DynamoDB tables found for the current AWS profile and region.");
            }
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            self.render_metadata_panel(ui);
        });

        if events_processed || self.state.has_pending_requests() {
            ctx.request_repaint();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use crate::aws::dynamodb::TableMetadata;
    use crate::messages::WorkerEvent;
    use tokio::sync::mpsc::error::TryRecvError;

    use super::{AppState, DbExplorerApp, MetadataState};

    #[test]
    fn starts_loading_tables() {
        let mut state = AppState::new();
        let request_id = state.begin_tables_refresh();

        assert!(state.tables_loading);
        assert_eq!(request_id, 1);
    }

    #[test]
    fn applies_loaded_table_list() {
        let mut state = AppState::new();
        let request_id = state.begin_tables_refresh();

        state.handle_event(WorkerEvent::TablesLoaded {
            request_id,
            result: Ok(vec!["orders".to_string(), "users".to_string()]),
        });

        assert!(!state.tables_loading);
        assert_eq!(state.tables.len(), 2);
    }

    #[test]
    fn ignores_stale_metadata_response() {
        let mut state = AppState::new();
        let current_request_id = state.begin_metadata_load("users".to_string());

        let stale_id = current_request_id.saturating_sub(1);
        state.handle_event(WorkerEvent::TableMetadataLoaded {
            request_id: stale_id,
            table_name: "users".to_string(),
            result: Ok(TableMetadata {
                name: "users".to_string(),
                created_at: None,
                item_count: 10,
                item_count_is_exact: true,
            }),
        });

        assert_eq!(state.metadata_state, MetadataState::Loading);
    }

    #[test]
    fn metadata_error_is_stored() {
        let mut state = AppState::new();
        let request_id = state.begin_metadata_load("orders".to_string());

        state.handle_event(WorkerEvent::TableMetadataLoaded {
            request_id,
            table_name: "orders".to_string(),
            result: Err("boom".to_string()),
        });

        assert_eq!(
            state.metadata_state,
            MetadataState::Error("boom".to_string())
        );
    }

    #[test]
    fn selection_change_moves_to_loading() {
        let mut state = AppState::new();
        let _request_id = state.begin_metadata_load("users".to_string());
        assert_eq!(state.selected_table.as_deref(), Some("users"));
        assert_eq!(state.metadata_state, MetadataState::Loading);
    }

    #[test]
    fn fail_tables_refresh_clears_pending_state() {
        let mut state = AppState::new();
        let _ = state.begin_tables_refresh();

        state.fail_tables_refresh("worker down".to_string());

        assert!(!state.tables_loading);
        assert!(!state.has_pending_requests());
        assert_eq!(state.tables_error.as_deref(), Some("worker down"));
    }

    #[test]
    fn app_new_requests_tables_immediately() {
        let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
        let (_event_tx, event_rx) = mpsc::channel();

        let app = DbExplorerApp::new(command_tx, event_rx);

        let command = command_rx
            .try_recv()
            .expect("app should request table list during startup");
        assert!(matches!(
            command,
            crate::messages::WorkerCommand::LoadTables { request_id: 1 }
        ));
        assert!(app.state.tables_loading);
    }

    #[test]
    fn app_new_surfaces_worker_unavailable_for_tables() {
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(command_rx);
        let (_event_tx, event_rx) = mpsc::channel();

        let app = DbExplorerApp::new(command_tx, event_rx);

        assert!(!app.state.tables_loading);
        assert!(app.state.tables_error.is_some());
        assert!(
            app.state
                .tables_error
                .as_deref()
                .unwrap_or_default()
                .contains("Unable to request table list")
        );
    }

    #[test]
    fn request_metadata_surfaces_worker_unavailable() {
        let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
        let (_event_tx, event_rx) = mpsc::channel();

        let mut app = DbExplorerApp::new(command_tx, event_rx);
        let _ = command_rx.try_recv();
        drop(command_rx);

        app.request_metadata("users".to_string(), false);

        assert!(matches!(app.state.metadata_state, MetadataState::Error(_)));
        if let MetadataState::Error(err) = &app.state.metadata_state {
            assert!(err.contains("Unable to request table metadata"));
        }
    }

    #[test]
    fn drain_events_applies_worker_updates() {
        let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel();

        let mut app = DbExplorerApp::new(command_tx, event_rx);
        let _ = command_rx.try_recv();

        event_tx
            .send(WorkerEvent::TablesLoaded {
                request_id: 1,
                result: Ok(vec!["users".to_string()]),
            })
            .expect("event should be sent");

        app.drain_events();

        assert!(!app.state.tables_loading);
        assert_eq!(app.state.tables, vec!["users".to_string()]);
        assert!(matches!(command_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn fail_metadata_load_sets_error_and_clears_pending_state() {
        let mut state = AppState::new();
        let _ = state.begin_metadata_load("orders".to_string());

        state.fail_metadata_load("worker down".to_string());

        assert_eq!(
            state.metadata_state,
            MetadataState::Error("worker down".to_string())
        );
        assert!(!state.has_pending_requests());
    }

    #[test]
    fn clears_selection_when_selected_table_missing_after_refresh() {
        let mut state = AppState::new();
        state.selected_table = Some("users".to_string());
        let request_id = state.begin_tables_refresh();

        state.handle_event(WorkerEvent::TablesLoaded {
            request_id,
            result: Ok(vec!["orders".to_string()]),
        });

        assert_eq!(state.selected_table, None);
        assert_eq!(state.metadata_state, MetadataState::Idle);
    }

    #[test]
    fn has_pending_requests_reflects_current_state() {
        let mut state = AppState::new();
        assert!(!state.has_pending_requests());

        let request_id = state.begin_tables_refresh();
        assert!(state.has_pending_requests());

        state.handle_event(WorkerEvent::TablesLoaded {
            request_id,
            result: Ok(vec![]),
        });

        assert!(!state.has_pending_requests());
    }
}
