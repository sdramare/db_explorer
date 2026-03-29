use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::types::{AttributeValue, Select};
use aws_smithy_types_convert::date_time::DateTimeExt;
use base64::Engine;
use chrono::{DateTime, Local, Utc};
use serde_json::{Map, Number, Value};
use std::path::Path;
use thiserror::Error;
use tracing::{debug, info, instrument, warn};

use crate::messages::{ScanStartKey, TableItemsPage};

const EXACT_COUNT_WARN_THRESHOLD_PAGES: u64 = 100;
const EXACT_COUNT_MAX_PAGES: u64 = 10_000;
const EXPORT_WARN_THRESHOLD_PAGES: u64 = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMetadata {
    pub name: String,
    pub created_at: Option<DateTime<Utc>>,
    pub item_count: u64,
    pub item_count_is_exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableExportSummary {
    pub table_name: String,
    pub output_path: String,
    pub item_count: u64,
}

impl TableMetadata {
    pub fn formatted_creation_date(&self) -> String {
        format_creation_date(self.created_at)
    }
}

#[derive(Debug, Error)]
pub enum DynamoError {
    #[error("Describe table failed: missing table details in AWS response")]
    MissingTableDetails,
    #[error("{context} failed: AWS credentials are not available for the current profile")]
    CredentialsUnavailable { context: &'static str },
    #[error("{context} failed: access denied for the current profile")]
    AccessDenied { context: &'static str },
    #[error("{context} failed: AWS region is not configured")]
    RegionNotConfigured { context: &'static str },
    #[error("{context} failed: {details}")]
    Aws {
        context: &'static str,
        details: String,
    },
    #[error("{context} failed: {details}")]
    Local {
        context: &'static str,
        details: String,
    },
}

#[derive(Clone)]
pub struct DynamoDbService {
    client: aws_sdk_dynamodb::Client,
}

impl DynamoDbService {
    #[instrument(name = "dynamodb.init_client")]
    pub async fn new() -> Result<Self, DynamoError> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        if config.region().is_none() {
            return Err(DynamoError::RegionNotConfigured {
                context: "Initialize DynamoDB client",
            });
        }
        let client = aws_sdk_dynamodb::Client::new(&config);
        Ok(Self { client })
    }

    #[instrument(name = "dynamodb.list_tables", skip(self))]
    pub async fn list_tables(&self) -> Result<Vec<String>, DynamoError> {
        let mut all_tables = Vec::new();
        let mut start_name: Option<String> = None;
        let mut pages = 0_u64;

        loop {
            pages = pages.saturating_add(1);
            let mut request = self.client.list_tables();
            if let Some(start) = &start_name {
                request = request.exclusive_start_table_name(start);
            }

            let response = request
                .send()
                .await
                .map_err(|err| map_sdk_error("List tables", &err))?;

            if let Some(mut names) = response.table_names {
                all_tables.append(&mut names);
            }

            start_name = response.last_evaluated_table_name;
            if start_name.is_none() {
                break;
            }
        }

        all_tables.sort();
        info!(table_count = all_tables.len(), pages, "loaded table list");
        Ok(all_tables)
    }

    #[instrument(name = "dynamodb.load_table_metadata", skip(self), fields(table_name = %table_name, exact_item_count = exact_item_count))]
    pub async fn load_table_metadata(
        &self,
        table_name: &str,
        exact_item_count: bool,
    ) -> Result<TableMetadata, DynamoError> {
        let description = self
            .client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .map_err(|err| map_sdk_error("Describe table", &err))?;

        let table = description.table.ok_or(DynamoError::MissingTableDetails)?;

        let created_at = table.creation_date_time.and_then(smithy_to_chrono);
        let item_count = if exact_item_count {
            self.count_items_exact(table_name).await?
        } else {
            normalize_table_item_count(table.item_count)
        };

        info!(item_count, exact_item_count, "loaded table metadata");

        Ok(TableMetadata {
            name: table_name.to_string(),
            created_at,
            item_count,
            item_count_is_exact: exact_item_count,
        })
    }

    #[instrument(name = "dynamodb.count_items_exact", skip(self), fields(table_name = %table_name))]
    async fn count_items_exact(&self, table_name: &str) -> Result<u64, DynamoError> {
        let mut total = 0_u64;
        let mut last_key: Option<
            std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
        > = None;
        let mut pages = 0_u64;

        loop {
            pages = pages.saturating_add(1);
            if pages == EXACT_COUNT_WARN_THRESHOLD_PAGES {
                warn!(
                    table_name = %table_name,
                    pages,
                    "exact count is scanning many pages; this can be expensive"
                );
            }
            if pages > EXACT_COUNT_MAX_PAGES {
                return Err(DynamoError::Aws {
                    context: "Count table items",
                    details: format!(
                        "aborted exact count after {EXACT_COUNT_MAX_PAGES} pages to limit scan cost"
                    ),
                });
            }

            let mut request = self
                .client
                .scan()
                .table_name(table_name)
                .select(Select::Count);
            if let Some(key) = &last_key {
                request = request.set_exclusive_start_key(Some(key.clone()));
            }

            let response = request
                .send()
                .await
                .map_err(|err| map_sdk_error("Count table items", &err))?;

            total = total.saturating_add(normalize_page_count(response.count));
            debug!(
                pages,
                page_count = response.count,
                total,
                "counted table page"
            );

            let has_more = response
                .last_evaluated_key
                .as_ref()
                .map(|key| !key.is_empty())
                .unwrap_or(false);

            if !has_more {
                break;
            }

            last_key = response.last_evaluated_key;
        }

        info!(pages, total, "finished exact table count");
        Ok(total)
    }

    #[instrument(name = "dynamodb.export_table_to_json", skip(self), fields(table_name = %table_name, output_path = %output_path.display(), pretty_print = pretty_print))]
    pub async fn export_table_to_json_file(
        &self,
        table_name: &str,
        output_path: &Path,
        pretty_print: bool,
    ) -> Result<TableExportSummary, DynamoError> {
        let mut items = Vec::<Value>::new();
        let mut last_key: Option<std::collections::HashMap<String, AttributeValue>> = None;
        let mut pages = 0_u64;

        loop {
            pages = pages.saturating_add(1);
            if pages == EXPORT_WARN_THRESHOLD_PAGES {
                warn!(
                    table_name = %table_name,
                    pages,
                    "export is scanning many pages; this may be slow and memory-intensive"
                );
            }

            let mut request = self.client.scan().table_name(table_name);
            if let Some(key) = &last_key {
                request = request.set_exclusive_start_key(Some(key.clone()));
            }

            let response = request
                .send()
                .await
                .map_err(|err| map_sdk_error("Export table to JSON", &err))?;

            if let Some(page_items) = response.items {
                for item in page_items {
                    items.push(attribute_map_to_json(item));
                }
            }

            let has_more = response
                .last_evaluated_key
                .as_ref()
                .map(|key| !key.is_empty())
                .unwrap_or(false);

            if !has_more {
                break;
            }

            last_key = response.last_evaluated_key;
        }

        let payload = Value::Array(items);
        let json_bytes = if pretty_print {
            serde_json::to_vec_pretty(&payload).map_err(|err| DynamoError::Local {
                context: "Serialize export JSON",
                details: err.to_string(),
            })?
        } else {
            serde_json::to_vec(&payload).map_err(|err| DynamoError::Local {
                context: "Serialize export JSON",
                details: err.to_string(),
            })?
        };

        tokio::fs::write(output_path, json_bytes)
            .await
            .map_err(|err| DynamoError::Local {
                context: "Write export JSON file",
                details: err.to_string(),
            })?;

        let item_count = payload.as_array().map_or(0, |arr| arr.len() as u64);
        info!(table_name = %table_name, pages, item_count, output_path = %output_path.display(), "exported table to JSON file");

        Ok(TableExportSummary {
            table_name: table_name.to_string(),
            output_path: output_path.to_string_lossy().into_owned(),
            item_count,
        })
    }

    #[instrument(name = "dynamodb.load_table_items_page", skip(self, exclusive_start_key), fields(table_name = %table_name, page_size = page_size))]
    pub async fn load_table_items_page(
        &self,
        table_name: &str,
        page_size: u32,
        exclusive_start_key: Option<ScanStartKey>,
    ) -> Result<TableItemsPage, DynamoError> {
        let mut request = self
            .client
            .scan()
            .table_name(table_name)
            .limit(normalize_page_size(page_size));
        if let Some(key) = exclusive_start_key {
            request = request.set_exclusive_start_key(Some(key));
        }

        let response = request
            .send()
            .await
            .map_err(|err| map_sdk_error("Load table items", &err))?;

        let mut items = Vec::new();
        if let Some(page_items) = response.items {
            for item in page_items {
                items.push(attribute_map_to_json(item));
            }
        }

        let next_start_key = response.last_evaluated_key.filter(|key| !key.is_empty());

        Ok(TableItemsPage {
            has_more: next_start_key.is_some(),
            items,
            next_start_key,
        })
    }
}

fn attribute_map_to_json(item: std::collections::HashMap<String, AttributeValue>) -> Value {
    let mut object = Map::new();
    for (key, value) in item {
        object.insert(key, attribute_value_to_json(value));
    }
    Value::Object(object)
}

fn attribute_value_to_json(value: AttributeValue) -> Value {
    match value {
        AttributeValue::S(v) => Value::String(v),
        AttributeValue::N(v) => number_string_to_json(v),
        AttributeValue::Bool(v) => Value::Bool(v),
        AttributeValue::Null(_) => Value::Null,
        AttributeValue::L(values) => {
            Value::Array(values.into_iter().map(attribute_value_to_json).collect())
        }
        AttributeValue::M(map) => attribute_map_to_json(map),
        AttributeValue::Ss(values) => Value::Array(values.into_iter().map(Value::String).collect()),
        AttributeValue::Ns(values) => {
            Value::Array(values.into_iter().map(number_string_to_json).collect())
        }
        AttributeValue::Bs(values) => {
            let encoded = values
                .into_iter()
                .map(|blob| Value::String(base64_blob(blob)))
                .collect();
            Value::Array(encoded)
        }
        AttributeValue::B(blob) => Value::String(base64_blob(blob)),
        _ => Value::String("Unsupported DynamoDB value".to_string()),
    }
}

fn base64_blob(blob: aws_sdk_dynamodb::primitives::Blob) -> String {
    base64::engine::general_purpose::STANDARD.encode(blob.into_inner())
}

fn number_string_to_json(value: String) -> Value {
    if let Ok(parsed) = value.parse::<i64>() {
        return Value::Number(Number::from(parsed));
    }
    if let Ok(parsed) = value.parse::<u64>() {
        return Value::Number(Number::from(parsed));
    }
    if let Ok(parsed) = value.parse::<f64>()
        && let Some(number) = Number::from_f64(parsed)
    {
        return Value::Number(number);
    }

    Value::String(value)
}

fn smithy_to_chrono(timestamp: aws_sdk_dynamodb::primitives::DateTime) -> Option<DateTime<Utc>> {
    timestamp.to_chrono_utc().ok()
}

fn format_creation_date(created_at: Option<DateTime<Utc>>) -> String {
    match created_at {
        Some(ts) => ts
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M:%S %Z")
            .to_string(),
        None => "Unknown".to_string(),
    }
}

fn normalize_page_count(count: i32) -> u64 {
    count.max(0) as u64
}

fn normalize_table_item_count(item_count: Option<i64>) -> u64 {
    item_count.unwrap_or_default().max(0) as u64
}

fn normalize_page_size(page_size: u32) -> i32 {
    page_size.clamp(1, i32::MAX as u32) as i32
}

fn map_sdk_error<E, R>(context: &'static str, err: &SdkError<E, R>) -> DynamoError
where
    E: ProvideErrorMetadata,
{
    let (code, message) = if let Some(service_err) = err.as_service_error() {
        (service_err.code(), service_err.message())
    } else {
        (None, None)
    };

    if matches!(
        code,
        Some("UnrecognizedClientException") | Some("InvalidClientTokenId")
    ) {
        return DynamoError::CredentialsUnavailable { context };
    }

    if matches!(
        code,
        Some("AccessDeniedException") | Some("UnauthorizedOperation")
    ) {
        return DynamoError::AccessDenied { context };
    }

    let details = message.unwrap_or("Unknown AWS SDK error").to_string();

    // Fallback for SDK errors that do not expose structured service codes.
    let lower = details.to_lowercase();
    if lower.contains("credential") {
        return DynamoError::CredentialsUnavailable { context };
    }
    if lower.contains("region") && lower.contains("not") && lower.contains("configured") {
        return DynamoError::RegionNotConfigured { context };
    }

    DynamoError::Aws {
        context,
        details: if details == "Unknown AWS SDK error" {
            err.to_string()
        } else {
            details
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DynamoError, attribute_map_to_json, attribute_value_to_json, format_creation_date,
        normalize_page_count, normalize_page_size, normalize_table_item_count,
    };
    use aws_sdk_dynamodb::types::AttributeValue;
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;

    #[test]
    fn normalize_page_count_single_page() {
        assert_eq!(normalize_page_count(42), 42);
    }

    #[test]
    fn normalize_page_count_multi_page_accumulate() {
        let pages = vec![10, 5, 20];
        let total = pages
            .into_iter()
            .fold(0_u64, |acc, page| acc + normalize_page_count(page));
        assert_eq!(total, 35);
    }

    #[test]
    fn normalize_page_count_empty_page() {
        assert_eq!(normalize_page_count(0), 0);
        assert_eq!(normalize_page_count(-4), 0);
    }

    #[test]
    fn normalize_table_item_count_non_negative() {
        assert_eq!(normalize_table_item_count(Some(12)), 12);
        assert_eq!(normalize_table_item_count(Some(-1)), 0);
        assert_eq!(normalize_table_item_count(None), 0);
    }

    #[test]
    fn normalize_page_size_is_clamped() {
        assert_eq!(normalize_page_size(0), 1);
        assert_eq!(normalize_page_size(20), 20);
    }

    #[test]
    fn typed_error_for_missing_table_details() {
        assert_eq!(
            DynamoError::MissingTableDetails.to_string(),
            "Describe table failed: missing table details in AWS response"
        );
    }

    #[test]
    fn typed_error_for_access_denied() {
        let msg = DynamoError::AccessDenied {
            context: "Describe table",
        }
        .to_string();
        assert!(msg.contains("access denied"));
    }

    #[test]
    fn format_creation_date_unknown() {
        assert_eq!(format_creation_date(None), "Unknown");
    }

    #[test]
    fn format_creation_date_is_stable() {
        let timestamp = DateTime::parse_from_rfc3339("2024-08-21T09:10:11Z")
            .expect("valid RFC3339")
            .with_timezone(&Utc);
        let formatted = format_creation_date(Some(timestamp));
        let parts: Vec<&str> = formatted.split_whitespace().collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].len(), 10);
        assert!(parts[0].chars().enumerate().all(|(idx, ch)| match idx {
            4 | 7 => ch == '-',
            _ => ch.is_ascii_digit(),
        }));
        assert_eq!(parts[1].len(), 8);
        assert_eq!(parts[1].chars().filter(|ch| *ch == ':').count(), 2);
        assert!(!parts[2].is_empty());
    }

    #[test]
    fn converts_simple_attribute_value_to_json() {
        assert_eq!(
            attribute_value_to_json(AttributeValue::S("abc".to_string())).to_string(),
            "\"abc\""
        );
        assert_eq!(
            attribute_value_to_json(AttributeValue::N("42".to_string())).to_string(),
            "42"
        );
        assert_eq!(
            attribute_value_to_json(AttributeValue::Bool(true)).to_string(),
            "true"
        );
    }

    #[test]
    fn converts_nested_attribute_value_to_json() {
        let mut inner = HashMap::new();
        inner.insert("count".to_string(), AttributeValue::N("7".to_string()));
        inner.insert("name".to_string(), AttributeValue::S("users".to_string()));

        let value = attribute_value_to_json(AttributeValue::M(inner));
        let object = value.as_object().expect("should be object");
        assert_eq!(object.get("count").and_then(|v| v.as_i64()), Some(7));
        assert_eq!(object.get("name").and_then(|v| v.as_str()), Some("users"));
    }

    #[test]
    fn converts_attribute_map_to_json_object() {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("A#1".to_string()));
        item.insert("active".to_string(), AttributeValue::Bool(false));

        let value = attribute_map_to_json(item);
        let object = value.as_object().expect("should be object");
        assert_eq!(object.get("pk").and_then(|v| v.as_str()), Some("A#1"));
        assert_eq!(object.get("active").and_then(|v| v.as_bool()), Some(false));
    }
}
