use std::path::PathBuf;

use crate::aws::dynamodb::{TableExportSummary, TableMetadata};

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
}
