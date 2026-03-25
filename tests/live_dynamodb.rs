#![cfg(feature = "live-aws-tests")]

use db_explorer::aws::dynamodb::DynamoDbService;
use serial_test::serial;

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn can_list_tables_with_live_aws() {
    if std::env::var("AWS_PROFILE").is_err() {
        eprintln!("skipping live AWS test: AWS_PROFILE is not set");
        return;
    }

    let result = async {
        let service = DynamoDbService::new()
            .await
            .map_err(|err| err.to_string())?;
        service.list_tables().await.map_err(|err| err.to_string())
    }
    .await;

    assert!(result.is_ok(), "list_tables failed: {result:?}");
}

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn can_describe_first_table_with_live_aws() {
    if std::env::var("AWS_PROFILE").is_err() {
        eprintln!("skipping live AWS test: AWS_PROFILE is not set");
        return;
    }

    let result = async {
        let service = DynamoDbService::new()
            .await
            .map_err(|err| err.to_string())?;
        let tables = service.list_tables().await.map_err(|err| err.to_string())?;
        if let Some(first) = tables.first() {
            let metadata = service
                .load_table_metadata(first, false)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(metadata.name, *first);
        }
        Ok::<(), String>(())
    }
    .await;

    assert!(result.is_ok(), "describe failed: {result:?}");
}
