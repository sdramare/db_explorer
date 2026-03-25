use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::types::Select;
use aws_smithy_types_convert::date_time::DateTimeExt;
use chrono::{DateTime, Local, Utc};
use thiserror::Error;
use tracing::{debug, info, instrument};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMetadata {
    pub name: String,
    pub created_at: Option<DateTime<Utc>>,
    pub item_count: u64,
    pub item_count_is_exact: bool,
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
}

pub struct DynamoDbService {
    client: aws_sdk_dynamodb::Client,
}

impl DynamoDbService {
    #[instrument(name = "dynamodb.init_client")]
    pub async fn new() -> Self {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let client = aws_sdk_dynamodb::Client::new(&config);
        Self { client }
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
        let approximate_count = table.item_count.unwrap_or_default().max(0) as u64;
        let item_count = if exact_item_count {
            self.count_items_exact(table_name).await?
        } else {
            approximate_count
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
}

fn smithy_to_chrono(timestamp: aws_sdk_dynamodb::primitives::DateTime) -> Option<DateTime<Utc>> {
    timestamp.to_chrono_utc().ok()
}

pub fn format_creation_date(created_at: Option<DateTime<Utc>>) -> String {
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
    use super::{DynamoError, format_creation_date, normalize_page_count};
    use chrono::{DateTime, Utc};

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
}
