use crate::aws::dynamodb::TableMetadata;

#[derive(Debug, Clone)]
pub enum WorkerCommand {
    LoadTables {
        request_id: u64,
    },
    LoadTableMetadata {
        request_id: u64,
        table_name: String,
        exact_item_count: bool,
    },
}

#[derive(Debug, Clone)]
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
}
