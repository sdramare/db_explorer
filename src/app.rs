use std::sync::mpsc::Receiver;

use chrono::Local;
use eframe::egui;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use tokio::sync::mpsc::{Sender, error::TrySendError};
use tracing::{debug, info, warn};

use crate::aws::dynamodb::TableMetadata;
use crate::messages::{ScanStartKey, WorkerCommand, WorkerEvent};

const DEFAULT_PAGE_SIZE: u32 = 20;
const PAGE_SIZE_OPTIONS: [u32; 3] = [10, 20, 50];
const ROW_NUMBER_COLUMN_WIDTH: f32 = 52.0;
const COLUMN_SEPARATOR_WIDTH: f32 = 8.0;
const APPROX_CHAR_WIDTH_PX: f32 = 7.0;
const MIN_CELL_COLUMN_WIDTH: f32 = 120.0;
const DEFAULT_CELL_COLUMN_WIDTH: f32 = 240.0;
const MAX_CELL_COLUMN_WIDTH: f32 = 500.0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataState {
    Idle,
    Loading,
    Loaded(TableMetadata),
    Canceled,
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportState {
    Idle,
    Exporting {
        table_name: String,
        output_path: PathBuf,
    },
    Success {
        table_name: String,
        output_path: PathBuf,
        item_count: u64,
    },
    Error(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ItemLoadMode {
    Replace,
    Append,
}

#[derive(Debug, Clone)]
pub struct ItemsState {
    pub rows: Vec<Value>,
    pub loading: bool,
    pub error: Option<String>,
    pub page_size: u32,
    pub next_start_key: Option<ScanStartKey>,
    pub has_more: bool,
    pub loaded_once: bool,
}

impl Default for ItemsState {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            loading: false,
            error: None,
            page_size: DEFAULT_PAGE_SIZE,
            next_start_key: None,
            has_more: true,
            loaded_once: false,
        }
    }
}

#[derive(Debug)]
pub struct AppState {
    pub tables: Vec<String>,
    pub selected_table: Option<String>,
    pub tables_loading: bool,
    pub tables_error: Option<String>,
    pub metadata_state: MetadataState,
    pub export_state: ExportState,
    pub items_state: ItemsState,
    next_request_id: u64,
    active_tables_request: Option<u64>,
    active_metadata_request: Option<u64>,
    active_export_request: Option<u64>,
    active_items_request: Option<u64>,
    active_items_mode: Option<ItemLoadMode>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            selected_table: None,
            tables_loading: false,
            tables_error: None,
            metadata_state: MetadataState::Idle,
            export_state: ExportState::Idle,
            items_state: ItemsState::default(),
            next_request_id: 1,
            active_tables_request: None,
            active_metadata_request: None,
            active_export_request: None,
            active_items_request: None,
            active_items_mode: None,
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
    pub fn begin_metadata_load(&mut self, table_name: &str) -> u64 {
        let request_id = self.next_id();
        self.selected_table = Some(table_name.to_string());
        self.metadata_state = MetadataState::Loading;
        self.active_metadata_request = Some(request_id);
        request_id
    }

    pub fn update_selected_table(&mut self, table_name: Option<String>) -> bool {
        if self.selected_table == table_name {
            return false;
        }

        self.selected_table = table_name;
        self.active_metadata_request = None;
        self.metadata_state = MetadataState::Idle;
        self.reset_items();
        true
    }

    pub fn fail_metadata_load(&mut self, error: String) {
        self.active_metadata_request = None;
        self.metadata_state = MetadataState::Error(error);
    }

    #[must_use]
    pub fn begin_export(&mut self, table_name: String, output_path: PathBuf) -> u64 {
        let request_id = self.next_id();
        self.active_export_request = Some(request_id);
        self.export_state = ExportState::Exporting {
            table_name,
            output_path,
        };
        request_id
    }

    pub fn fail_export(&mut self, error: String) {
        self.active_export_request = None;
        self.export_state = ExportState::Error(error);
    }

    pub fn cancel_metadata_load(&mut self) -> Option<u64> {
        let request_id = self.active_metadata_request.take()?;
        self.metadata_state = MetadataState::Canceled;
        Some(request_id)
    }

    pub fn reset_items(&mut self) {
        self.items_state.rows.clear();
        self.items_state.loading = false;
        self.items_state.error = None;
        self.items_state.next_start_key = None;
        self.items_state.has_more = true;
        self.items_state.loaded_once = false;
        self.active_items_request = None;
        self.active_items_mode = None;
    }

    #[must_use]
    fn begin_items_load(&mut self, mode: ItemLoadMode) -> (u64, Option<ScanStartKey>, u32) {
        let request_id = self.next_id();
        let start_key = match mode {
            ItemLoadMode::Replace => {
                self.items_state.rows.clear();
                self.items_state.next_start_key = None;
                self.items_state.has_more = true;
                self.items_state.loaded_once = false;
                None
            }
            ItemLoadMode::Append => self.items_state.next_start_key.clone(),
        };

        self.items_state.loading = true;
        self.items_state.error = None;
        self.active_items_request = Some(request_id);
        self.active_items_mode = Some(mode);

        (request_id, start_key, self.items_state.page_size)
    }

    pub fn fail_items_load(&mut self, error: String) {
        self.active_items_request = None;
        self.active_items_mode = None;
        self.items_state.loading = false;
        self.items_state.error = Some(error);
    }

    pub fn set_items_page_size(&mut self, page_size: u32) -> bool {
        if self.items_state.page_size == page_size {
            return false;
        }

        self.items_state.page_size = page_size;
        self.reset_items();
        true
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
                            self.reset_items();
                        }
                    }
                    Err(err) => {
                        self.tables.clear();
                        self.tables_error = Some(err);
                        self.selected_table = None;
                        self.metadata_state = MetadataState::Idle;
                        self.reset_items();
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
            WorkerEvent::TableExported {
                request_id,
                table_name,
                output_path,
                result,
            } => {
                if self.active_export_request != Some(request_id) {
                    return;
                }
                self.active_export_request = None;

                match result {
                    Ok(summary) => {
                        self.export_state = ExportState::Success {
                            table_name,
                            output_path,
                            item_count: summary.item_count,
                        };
                    }
                    Err(err) => {
                        self.export_state = ExportState::Error(err);
                    }
                }
            }
            WorkerEvent::TableItemsLoaded {
                request_id,
                table_name,
                result,
            } => {
                if self.active_items_request != Some(request_id) {
                    return;
                }

                self.active_items_request = None;
                self.items_state.loading = false;

                if self.selected_table.as_deref() != Some(table_name.as_str()) {
                    self.active_items_mode = None;
                    return;
                }

                let mode = self
                    .active_items_mode
                    .take()
                    .unwrap_or(ItemLoadMode::Replace);

                match result {
                    Ok(page) => {
                        match mode {
                            ItemLoadMode::Replace => {
                                self.items_state.rows = page.items;
                            }
                            ItemLoadMode::Append => {
                                self.items_state.rows.extend(page.items);
                            }
                        }
                        self.items_state.loaded_once = true;
                        self.items_state.error = None;
                        self.items_state.next_start_key = page.next_start_key;
                        self.items_state.has_more = page.has_more;
                    }
                    Err(err) => {
                        self.items_state.error = Some(err);
                    }
                }
            }
        }
    }

    pub fn has_pending_requests(&self) -> bool {
        self.active_tables_request.is_some()
            || self.active_metadata_request.is_some()
            || self.active_export_request.is_some()
            || self.active_items_request.is_some()
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct DbExplorerApp {
    state: AppState,
    command_tx: Sender<WorkerCommand>,
    event_rx: Receiver<WorkerEvent>,
    confirm_exact_recount: bool,
    column_widths: HashMap<String, f32>,
    cell_preview_open: bool,
    cell_preview_content: String,
}

impl DbExplorerApp {
    pub fn new(command_tx: Sender<WorkerCommand>, event_rx: Receiver<WorkerEvent>) -> Self {
        let mut app = Self {
            state: AppState::new(),
            command_tx,
            event_rx,
            confirm_exact_recount: false,
            column_widths: HashMap::new(),
            cell_preview_open: false,
            cell_preview_content: String::new(),
        };
        app.request_tables();
        app
    }

    fn request_tables(&mut self) {
        let request_id = self.state.begin_tables_refresh();
        info!(request_id, "ui: requesting table list");
        if let Err(err) = self
            .command_tx
            .try_send(WorkerCommand::LoadTables { request_id })
        {
            warn!(error = ?err, "ui: failed to send table list request to worker");
            let details = match err {
                TrySendError::Full(_) => "worker queue is full".to_string(),
                TrySendError::Closed(_) => "worker is unavailable".to_string(),
            };
            self.state
                .fail_tables_refresh(format!("Unable to request table list: {details}"));
        }
    }

    fn request_metadata(&mut self, table_name: String, exact_item_count: bool) {
        self.confirm_exact_recount = false;
        let request_id = self.state.begin_metadata_load(&table_name);
        info!(request_id, table_name = %table_name, exact_item_count, "ui: requesting table metadata");
        if let Err(err) = self.command_tx.try_send(WorkerCommand::LoadTableMetadata {
            request_id,
            table_name,
            exact_item_count,
        }) {
            warn!(error = ?err, "ui: failed to send metadata request to worker");
            let details = match err {
                TrySendError::Full(_) => "worker queue is full".to_string(),
                TrySendError::Closed(_) => "worker is unavailable".to_string(),
            };
            self.state
                .fail_metadata_load(format!("Unable to request table metadata: {details}"));
        }
    }

    fn cancel_metadata_load(&mut self) {
        let Some(request_id) = self.state.cancel_metadata_load() else {
            return;
        };

        info!(request_id, "ui: cancel metadata load clicked");
        if let Err(err) = self
            .command_tx
            .try_send(WorkerCommand::CancelMetadataLoad { request_id })
        {
            warn!(error = ?err, "ui: failed to send cancel metadata request to worker");
        }
    }

    fn request_export(&mut self, table_name: String, output_path: PathBuf, pretty_print: bool) {
        let request_id = self
            .state
            .begin_export(table_name.clone(), output_path.clone());
        info!(request_id, table_name = %table_name, output_path = %output_path.display(), "ui: requesting table export");
        if let Err(err) = self.command_tx.try_send(WorkerCommand::ExportTableToJson {
            request_id,
            table_name,
            output_path,
            pretty_print,
        }) {
            warn!(error = ?err, "ui: failed to send export request to worker");
            let details = match err {
                TrySendError::Full(_) => "worker queue is full".to_string(),
                TrySendError::Closed(_) => "worker is unavailable".to_string(),
            };
            self.state
                .fail_export(format!("Unable to export table: {details}"));
        }
    }

    fn request_items_reload(&mut self, table_name: String) {
        self.request_items(table_name, ItemLoadMode::Replace);
    }

    fn request_items_next_page(&mut self, table_name: String) {
        if self.state.items_state.loading
            || !self.state.items_state.has_more
            || self.state.items_state.next_start_key.is_none()
        {
            return;
        }
        self.request_items(table_name, ItemLoadMode::Append);
    }

    fn request_items(&mut self, table_name: String, mode: ItemLoadMode) {
        let (request_id, exclusive_start_key, page_size) = self.state.begin_items_load(mode);
        info!(request_id, table_name = %table_name, page_size, ?mode, "ui: requesting table items page");
        if let Err(err) = self.command_tx.try_send(WorkerCommand::LoadTableItems {
            request_id,
            table_name,
            page_size,
            exclusive_start_key,
        }) {
            warn!(error = ?err, "ui: failed to send items request to worker");
            let details = match err {
                TrySendError::Full(_) => "worker queue is full".to_string(),
                TrySendError::Closed(_) => "worker is unavailable".to_string(),
            };
            self.state
                .fail_items_load(format!("Unable to load table items: {details}"));
        }
    }

    fn pick_export_path(table_name: &str) -> Option<PathBuf> {
        let timestamp = Local::now().format("%Y%m%d_%H%M%S");
        let default_filename = format!("{table_name}_{timestamp}.json");

        rfd::FileDialog::new()
            .set_title("Export DynamoDB table as JSON")
            .add_filter("JSON", &["json"])
            .set_file_name(&default_filename)
            .save_file()
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
            MetadataState::Canceled => {
                ui.colored_label(
                    egui::Color32::from_rgb(170, 110, 30),
                    "Metadata load canceled.",
                );
            }
            MetadataState::Error(err) => {
                ui.colored_label(egui::Color32::from_rgb(180, 30, 30), err);
            }
        }
    }

    fn render_items_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Table Items");
        ui.separator();

        if self.state.selected_table.is_none() {
            ui.label("Select a table to view items.");
            return;
        }

        if let Some(err) = &self.state.items_state.error {
            ui.colored_label(egui::Color32::from_rgb(180, 30, 30), err);
        }

        if self.state.items_state.rows.is_empty() {
            if self.state.items_state.loading {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("Loading items...");
                });
                return;
            }

            if self.state.items_state.loaded_once {
                ui.label("No items in table.");
            } else {
                ui.label("No items loaded yet.");
            }
            return;
        }

        let columns = collect_item_columns(&self.state.items_state.rows);
        let mut request_more = false;
        let mut preview_content: Option<String> = None;

        egui::ScrollArea::both()
            .id_salt("table_items_grid")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                let table_top = ui.cursor().min.y;
                let mut separator_xs = Vec::new();
                let row_height = ui.spacing().interact_size.y;

                ui.scope(|ui| {
                    ui.spacing_mut().item_spacing.x = 0.0;

                    ui.horizontal(|ui| {
                        ui.add_sized(
                            [ROW_NUMBER_COLUMN_WIDTH, row_height],
                            egui::Label::new(egui::RichText::new("#").strong()),
                        );

                        let (first_sep_rect, _) = ui.allocate_exact_size(
                            egui::vec2(COLUMN_SEPARATOR_WIDTH, row_height),
                            egui::Sense::hover(),
                        );
                        separator_xs.push(first_sep_rect.center().x);

                        for (column_index, column) in columns.iter().enumerate() {
                            let width_entry = self
                                .column_widths
                                .entry(column.clone())
                                .or_insert(DEFAULT_CELL_COLUMN_WIDTH);
                            *width_entry = clamp_column_width(*width_entry);

                            ui.add_sized(
                                [*width_entry, row_height],
                                egui::Label::new(egui::RichText::new(column).strong()),
                            );

                            if column_index + 1 < columns.len() {
                                let (sep_rect, sep_response) = ui.allocate_exact_size(
                                    egui::vec2(COLUMN_SEPARATOR_WIDTH, row_height),
                                    egui::Sense::drag(),
                                );
                                separator_xs.push(sep_rect.center().x);
                                if sep_response.hovered() || sep_response.dragged() {
                                    ui.output_mut(|output| {
                                        output.cursor_icon = egui::CursorIcon::ResizeHorizontal;
                                    });
                                }
                                if sep_response.dragged() {
                                    let delta_x = ui.input(|i| i.pointer.delta().x);
                                    *width_entry = clamp_column_width(*width_entry + delta_x);
                                }
                            }
                        }
                    });

                    for (idx, row) in self.state.items_state.rows.iter().enumerate() {
                        let row_fill = if idx % 2 == 0 {
                            ui.visuals().faint_bg_color
                        } else {
                            ui.visuals().extreme_bg_color
                        };

                        egui::Frame::default()
                            .fill(row_fill)
                            .inner_margin(egui::Margin::same(0))
                            .outer_margin(egui::Margin::same(0))
                            .show(ui, |ui| {
                                ui.spacing_mut().item_spacing.x = 0.0;
                                ui.horizontal(|ui| {
                                    ui.add_sized(
                                        [ROW_NUMBER_COLUMN_WIDTH, row_height],
                                        egui::Label::new((idx + 1).to_string()),
                                    );

                                    ui.allocate_exact_size(
                                        egui::vec2(COLUMN_SEPARATOR_WIDTH, row_height),
                                        egui::Sense::hover(),
                                    );

                                    for (column_index, column) in columns.iter().enumerate() {
                                        let width = self
                                            .column_widths
                                            .get(column)
                                            .copied()
                                            .unwrap_or(DEFAULT_CELL_COLUMN_WIDTH);
                                        let value = value_for_column(row, column);

                                        ui.horizontal(|ui| {
                                            let button_width = 40.0;
                                            let (_, clipped_on_full_width) =
                                                truncate_cell_text_to_width(&value, width.max(1.0));
                                            let text_width = if clipped_on_full_width {
                                                (width - button_width).max(1.0)
                                            } else {
                                                width.max(1.0)
                                            };
                                            let (clipped, was_clipped) =
                                                truncate_cell_text_to_width(&value, text_width);

                                            ui.add_sized(
                                                [text_width, row_height],
                                                egui::Label::new(clipped),
                                            );

                                            if was_clipped {
                                                let preview_response = ui.add_sized(
                                                    [button_width, row_height],
                                                    egui::Button::new("(...)")
                                                        .sense(egui::Sense::click())
                                                        .small(),
                                                );
                                                if preview_response.clicked() {
                                                    preview_content = Some(value.clone());
                                                }
                                            }
                                        });

                                        if column_index + 1 < columns.len() {
                                            ui.allocate_exact_size(
                                                egui::vec2(COLUMN_SEPARATOR_WIDTH, row_height),
                                                egui::Sense::hover(),
                                            );
                                        }
                                    }
                                });
                            });
                    }
                });

                let table_bottom = ui.cursor().min.y;
                let separator_stroke =
                    egui::Stroke::new(1.0, ui.visuals().widgets.noninteractive.bg_stroke.color);
                for separator_x in separator_xs {
                    ui.painter().line_segment(
                        [
                            egui::pos2(separator_x, table_top),
                            egui::pos2(separator_x, table_bottom),
                        ],
                        separator_stroke,
                    );
                }

                ui.add_space(8.0);
                if self.state.items_state.loading {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label("Loading more items...");
                    });
                } else if self.state.items_state.has_more {
                    let sentinel = ui.label("Scroll to load more...");
                    request_more = ui.is_rect_visible(sentinel.rect);
                } else {
                    ui.label("End of results.");
                }
            });

        if request_more
            && !self.state.items_state.loading
            && self.state.items_state.has_more
            && self.state.items_state.error.is_none()
            && let Some(selected) = self.state.selected_table.clone()
        {
            self.request_items_next_page(selected);
        }

        if let Some(content) = preview_content {
            self.cell_preview_content = content;
            self.cell_preview_open = true;
        }
    }

    fn render_cell_preview_window(&mut self, ctx: &egui::Context) {
        if !self.cell_preview_open {
            return;
        }

        egui::Window::new("Cell content")
            .open(&mut self.cell_preview_open)
            .resizable(true)
            .vscroll(true)
            .show(ctx, |ui| {
                ui.set_min_width(720.0);
                ui.label("Full cell value");
                ui.separator();
                egui::ScrollArea::both().show(ui, |ui| {
                    ui.add(
                        egui::TextEdit::multiline(&mut self.cell_preview_content)
                            .interactive(false)
                            .desired_rows(24)
                            .desired_width(f32::INFINITY),
                    );
                });
            });
    }
}

fn collect_item_columns(rows: &[Value]) -> Vec<String> {
    let mut columns = BTreeSet::new();
    for row in rows {
        if let Value::Object(map) = row {
            for key in map.keys() {
                columns.insert(key.clone());
            }
        }
    }

    columns.into_iter().collect()
}

fn value_for_column(row: &Value, column: &str) -> String {
    let Some(value) = row.as_object().and_then(|obj| obj.get(column)) else {
        return String::new();
    };

    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| "<unprintable json>".to_string())
        }
    }
}

fn truncate_cell_text_to_width(value: &str, max_width_px: f32) -> (String, bool) {
    let max_chars = (max_width_px / APPROX_CHAR_WIDTH_PX).floor().max(1.0) as usize;
    let total_chars = value.chars().count();
    if total_chars <= max_chars {
        return (value.to_string(), false);
    }

    let clipped: String = value.chars().take(max_chars).collect();
    (format!("{clipped}..."), true)
}

fn clamp_column_width(width: f32) -> f32 {
    width.clamp(MIN_CELL_COLUMN_WIDTH, MAX_CELL_COLUMN_WIDTH)
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
                if pending_selection != previous_selection
                    && self.state.update_selected_table(pending_selection)
                    && let Some(selected) = self.state.selected_table.clone()
                {
                    info!(table_name = %selected, "ui: table selection changed");
                    self.request_metadata(selected.clone(), false);
                    self.request_items_reload(selected);
                }

                let mut pending_page_size = self.state.items_state.page_size;
                egui::ComboBox::from_label("Page size")
                    .selected_text(pending_page_size.to_string())
                    .show_ui(ui, |ui| {
                        for size in PAGE_SIZE_OPTIONS {
                            ui.selectable_value(&mut pending_page_size, size, size.to_string());
                        }
                    });
                if self.state.set_items_page_size(pending_page_size)
                    && let Some(selected) = self.state.selected_table.clone()
                {
                    self.request_items_reload(selected);
                }

                if ui
                    .add_enabled(
                        self.state.selected_table.is_some() && !self.state.items_state.loading,
                        egui::Button::new("Reload items"),
                    )
                    .clicked()
                    && let Some(selected) = self.state.selected_table.clone()
                {
                    self.request_items_reload(selected);
                }

                let recount_label = if self.confirm_exact_recount {
                    "Confirm exact recount"
                } else {
                    "Recount exactly"
                };

                if ui
                    .add_enabled(
                        self.state.selected_table.is_some()
                            && !matches!(self.state.metadata_state, MetadataState::Loading),
                        egui::Button::new(recount_label),
                    )
                    .clicked()
                    && let Some(selected) = self.state.selected_table.clone()
                {
                    if self.confirm_exact_recount {
                        info!(table_name = %selected, "ui: exact recount confirmed");
                        self.confirm_exact_recount = false;
                        self.request_metadata(selected, true);
                    } else {
                        self.confirm_exact_recount = true;
                    }
                }

                if ui
                    .add_enabled(
                        matches!(self.state.metadata_state, MetadataState::Loading),
                        egui::Button::new("Cancel load"),
                    )
                    .clicked()
                {
                    self.cancel_metadata_load();
                }

                if ui
                    .add_enabled(
                        self.state.selected_table.is_some()
                            && !matches!(self.state.export_state, ExportState::Exporting { .. }),
                        egui::Button::new("Export JSON"),
                    )
                    .clicked()
                    && let Some(selected) = self.state.selected_table.clone()
                    && let Some(output_path) = Self::pick_export_path(&selected)
                {
                    self.request_export(selected, output_path, true);
                }
            });

            if self.confirm_exact_recount {
                ui.colored_label(
                    egui::Color32::from_rgb(170, 110, 30),
                    "Exact recount scans every item and may be slow or costly. Click again to confirm.",
                );
            }

            if self.state.tables_loading {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("Loading table list...");
                });
            }

            if let Some(err) = &self.state.tables_error {
                ui.colored_label(egui::Color32::from_rgb(180, 30, 30), err);
            }

            match &self.state.export_state {
                ExportState::Idle => {}
                ExportState::Exporting {
                    table_name,
                    output_path,
                } => {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label(format!(
                            "Exporting '{table_name}' to {}",
                            output_path.display()
                        ));
                    });
                }
                ExportState::Success {
                    table_name,
                    output_path,
                    item_count,
                } => {
                    ui.colored_label(
                        egui::Color32::from_rgb(20, 120, 20),
                        format!(
                            "Exported '{table_name}' ({item_count} items) to {}",
                            output_path.display()
                        ),
                    );
                }
                ExportState::Error(err) => {
                    ui.colored_label(egui::Color32::from_rgb(180, 30, 30), err);
                }
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
            ui.add_space(12.0);
            self.render_items_panel(ui);
        });

        self.render_cell_preview_window(ctx);

        if events_processed || self.state.has_pending_requests() {
            ctx.request_repaint();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::mpsc;

    use crate::aws::dynamodb::TableMetadata;
    use crate::messages::{TableItemsPage, WorkerEvent};
    use serde_json::json;
    use tokio::sync::mpsc::error::TryRecvError;

    use super::{AppState, DbExplorerApp, ExportState, MetadataState};

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
        let current_request_id = state.begin_metadata_load("users");

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
        let request_id = state.begin_metadata_load("orders");

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
        let _request_id = state.begin_metadata_load("users");
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
        let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(1);
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
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(1);
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
        let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(1);
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
        let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(1);
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
        let _ = state.begin_metadata_load("orders");

        state.fail_metadata_load("worker down".to_string());

        assert_eq!(
            state.metadata_state,
            MetadataState::Error("worker down".to_string())
        );
        assert!(!state.has_pending_requests());
    }

    #[test]
    fn item_reload_replaces_rows() {
        let mut state = AppState::new();
        state.update_selected_table(Some("users".to_string()));

        let (request_id, _, _) = state.begin_items_load(super::ItemLoadMode::Replace);
        state.handle_event(WorkerEvent::TableItemsLoaded {
            request_id,
            table_name: "users".to_string(),
            result: Ok(TableItemsPage {
                items: vec![json!({"pk": "u#1"})],
                next_start_key: None,
                has_more: false,
            }),
        });

        assert_eq!(state.items_state.rows.len(), 1);
        assert!(state.items_state.loaded_once);
        assert!(!state.items_state.has_more);
    }

    #[test]
    fn item_append_extends_rows() {
        let mut state = AppState::new();
        state.update_selected_table(Some("users".to_string()));

        let (first_request, _, _) = state.begin_items_load(super::ItemLoadMode::Replace);
        state.handle_event(WorkerEvent::TableItemsLoaded {
            request_id: first_request,
            table_name: "users".to_string(),
            result: Ok(TableItemsPage {
                items: vec![json!({"pk": "u#1"})],
                next_start_key: Some(std::collections::HashMap::new()),
                has_more: true,
            }),
        });

        let (second_request, _, _) = state.begin_items_load(super::ItemLoadMode::Append);
        state.handle_event(WorkerEvent::TableItemsLoaded {
            request_id: second_request,
            table_name: "users".to_string(),
            result: Ok(TableItemsPage {
                items: vec![json!({"pk": "u#2"})],
                next_start_key: None,
                has_more: false,
            }),
        });

        assert_eq!(state.items_state.rows.len(), 2);
    }

    #[test]
    fn stale_item_response_is_ignored() {
        let mut state = AppState::new();
        state.update_selected_table(Some("users".to_string()));

        let (request_id, _, _) = state.begin_items_load(super::ItemLoadMode::Replace);
        state.handle_event(WorkerEvent::TableItemsLoaded {
            request_id: request_id.saturating_add(1),
            table_name: "users".to_string(),
            result: Ok(TableItemsPage {
                items: vec![json!({"pk": "u#1"})],
                next_start_key: None,
                has_more: false,
            }),
        });

        assert!(state.items_state.rows.is_empty());
        assert!(state.items_state.loading);
    }

    #[test]
    fn truncate_cell_text_keeps_short_values() {
        let (text, clipped) = super::truncate_cell_text_to_width("short", 80.0);
        assert_eq!(text, "short");
        assert!(!clipped);
    }

    #[test]
    fn truncate_cell_text_clips_long_values() {
        let (text, clipped) =
            super::truncate_cell_text_to_width("abcdefghijklmnopqrstuvwxyz", 56.0);
        assert_eq!(text, "abcdefgh...");
        assert!(clipped);
    }

    #[test]
    fn clamp_column_width_respects_bounds() {
        assert_eq!(
            super::clamp_column_width(10.0),
            super::MIN_CELL_COLUMN_WIDTH
        );
        assert_eq!(super::clamp_column_width(250.0), 250.0);
        assert_eq!(
            super::clamp_column_width(999.0),
            super::MAX_CELL_COLUMN_WIDTH
        );
    }

    #[test]
    fn cancel_metadata_load_clears_pending_state() {
        let mut state = AppState::new();
        let request_id = state.begin_metadata_load("orders");

        assert_eq!(state.cancel_metadata_load(), Some(request_id));
        assert_eq!(state.metadata_state, MetadataState::Canceled);
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

    #[test]
    fn export_success_updates_state() {
        let mut state = AppState::new();
        let request_id =
            state.begin_export("users".to_string(), PathBuf::from("/tmp/users_export.json"));

        state.handle_event(WorkerEvent::TableExported {
            request_id,
            table_name: "users".to_string(),
            output_path: PathBuf::from("/tmp/users_export.json"),
            result: Ok(crate::aws::dynamodb::TableExportSummary {
                table_name: "users".to_string(),
                output_path: "/tmp/users_export.json".to_string(),
                item_count: 3,
            }),
        });

        assert_eq!(
            state.export_state,
            ExportState::Success {
                table_name: "users".to_string(),
                output_path: PathBuf::from("/tmp/users_export.json"),
                item_count: 3,
            }
        );
    }

    #[test]
    fn stale_export_response_is_ignored() {
        let mut state = AppState::new();
        let request_id = state.begin_export("users".to_string(), PathBuf::from("/tmp/out.json"));

        state.handle_event(WorkerEvent::TableExported {
            request_id: request_id.saturating_add(1),
            table_name: "users".to_string(),
            output_path: PathBuf::from("/tmp/out.json"),
            result: Err("boom".to_string()),
        });

        assert_eq!(
            state.export_state,
            ExportState::Exporting {
                table_name: "users".to_string(),
                output_path: PathBuf::from("/tmp/out.json"),
            }
        );
    }
}
