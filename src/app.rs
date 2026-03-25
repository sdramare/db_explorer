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

#[derive(Debug, Clone)]
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

    pub fn begin_tables_refresh(&mut self) -> u64 {
        let request_id = self.next_id();
        self.tables_loading = true;
        self.tables_error = None;
        self.active_tables_request = Some(request_id);
        request_id
    }

    pub fn begin_metadata_load(&mut self, table_name: String) -> u64 {
        let request_id = self.next_id();
        self.selected_table = Some(table_name);
        self.metadata_state = MetadataState::Loading;
        self.active_metadata_request = Some(request_id);
        request_id
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
        }
    }

    fn request_metadata(&mut self, table_name: String) {
        let request_id = self.state.begin_metadata_load(table_name.clone());
        info!(request_id, table_name = %table_name, "ui: requesting table metadata");
        if let Err(err) = self.command_tx.send(WorkerCommand::LoadTableMetadata {
            request_id,
            table_name,
        }) {
            warn!(error = %err, "ui: failed to send metadata request to worker");
        }
    }

    fn drain_events(&mut self) {
        while let Ok(event) = self.event_rx.try_recv() {
            debug!(?event, "ui: received worker event");
            self.state.handle_event(event);
        }
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
                ui.label(format!("Number of items: {}", metadata.item_count));
            }
            MetadataState::Error(err) => {
                ui.colored_label(egui::Color32::from_rgb(180, 30, 30), err);
            }
        }
    }
}

impl eframe::App for DbExplorerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_events();

        egui::TopBottomPanel::top("toolbar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if ui.button("Refresh tables").clicked() {
                    info!("ui: refresh tables clicked");
                    self.request_tables();
                }

                let selected_text = self
                    .state
                    .selected_table
                    .as_deref()
                    .unwrap_or("Select table");

                let previous_selection = self.state.selected_table.clone();
                egui::ComboBox::from_label("Tables")
                    .selected_text(selected_text)
                    .show_ui(ui, |ui| {
                        for table in &self.state.tables {
                            ui.selectable_value(
                                &mut self.state.selected_table,
                                Some(table.clone()),
                                table,
                            );
                        }
                    });

                if self.state.selected_table != previous_selection
                    && let Some(selected) = self.state.selected_table.clone()
                {
                    info!(table_name = %selected, "ui: table selection changed");
                    self.request_metadata(selected);
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

        if self.state.has_pending_requests() {
            ctx.request_repaint();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::aws::dynamodb::TableMetadata;
    use crate::messages::WorkerEvent;

    use super::{AppState, MetadataState};

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
        state.begin_metadata_load("users".to_string());
        assert_eq!(state.selected_table.as_deref(), Some("users"));
        assert_eq!(state.metadata_state, MetadataState::Loading);
    }
}
