use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::types::Select;
use chrono::{DateTime, Local, Utc};
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
    pub async fn list_tables(&self) -> Result<Vec<String>, String> {
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
                .map_err(|err| map_service_error("List tables", &err.to_string()))?;

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
    ) -> Result<TableMetadata, String> {
        let description = self
            .client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .map_err(|err| map_service_error("Describe table", &err.to_string()))?;

        let table = description.table.ok_or_else(|| {
            "Describe table failed: missing table details in AWS response".to_string()
        })?;

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
    async fn count_items_exact(&self, table_name: &str) -> Result<u64, String> {
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
                .map_err(|err| map_service_error("Count table items", &err.to_string()))?;

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
    let seconds = timestamp.as_secs_f64();
    let whole_seconds = seconds.floor() as i64;
    let nanos = ((seconds - whole_seconds as f64) * 1_000_000_000.0)
        .clamp(0.0, 999_999_999.0)
        .round() as u32;
    DateTime::<Utc>::from_timestamp(whole_seconds, nanos)
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

fn map_service_error(context: &str, raw: &str) -> String {
    let lower = raw.to_lowercase();

    if lower.contains("credential")
        || lower.contains("invalidclienttokenid")
        || lower.contains("unrecognizedclient")
    {
        return format!(
            "{context} failed: AWS credentials are not available for the current profile"
        );
    }

    if lower.contains("accessdenied") || lower.contains("not authorized") {
        return format!("{context} failed: access denied for the current profile");
    }

    if lower.contains("region") && lower.contains("not") && lower.contains("configured") {
        return format!("{context} failed: AWS region is not configured");
    }

    format!("{context} failed: {raw}")
}

#[cfg(test)]
mod tests {
    use super::{format_creation_date, map_service_error, normalize_page_count};
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
    fn map_service_error_credentials() {
        let msg = map_service_error("List tables", "UnrecognizedClientException");
        assert!(msg.contains("credentials are not available"));
    }

    #[test]
    fn map_service_error_access_denied() {
        let msg = map_service_error("Describe table", "AccessDeniedException");
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
