use std::path::PathBuf;

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::Value;

use crate::aws::dynamodb::{TableExportSummary, TableMetadata};

pub type ScanStartKey = std::collections::HashMap<String, AttributeValue>;

#[derive(Debug, Clone)]
pub struct TableItemsPage {
    pub items: Vec<Value>,
    pub next_start_key: Option<ScanStartKey>,
    pub has_more: bool,
}

#[derive(Debug)]
pub enum WorkerCommand {
    LoadTables {
        request_id: u64,
    },
    LoadTableMetadata {
        request_id: u64,
        table_name: String,
        exact_item_count: bool,
    },
    CancelMetadataLoad {
        request_id: u64,
    },
    ExportTableToJson {
        request_id: u64,
        table_name: String,
        output_path: PathBuf,
        pretty_print: bool,
    },
    LoadTableItems {
        request_id: u64,
        table_name: String,
        page_size: u32,
        exclusive_start_key: Option<ScanStartKey>,
    },
}

#[derive(Debug)]
pub enum WorkerEvent {
    TablesLoaded {
        request_id: u64,
        result: Result<Vec<String>, String>,
    },
    TableMetadataLoaded {
        request_id: u64,
        table_name: String,
        result: Result<TableMetadata, String>,
    },
    TableExported {
        request_id: u64,
        table_name: String,
        output_path: PathBuf,
        result: Result<TableExportSummary, String>,
    },
    TableItemsLoaded {
        request_id: u64,
        table_name: String,
        result: Result<TableItemsPage, String>,
    },
}
