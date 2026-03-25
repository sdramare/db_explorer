#![cfg(feature = "live-aws-tests")]

use db_explorer::aws::dynamodb::DynamoDbService;
use serial_test::serial;

#[test]
#[serial]
fn can_list_tables_with_live_aws() {
    if std::env::var("AWS_PROFILE").is_err() {
        return;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let result = runtime.block_on(async {
        let service = DynamoDbService::new().await;
        service.list_tables().await
    });

    assert!(result.is_ok(), "list_tables failed: {result:?}");
}

#[test]
#[serial]
fn can_describe_first_table_with_live_aws() {
    if std::env::var("AWS_PROFILE").is_err() {
        return;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let result = runtime.block_on(async {
        let service = DynamoDbService::new().await;
        let tables = service.list_tables().await?;
        if let Some(first) = tables.first() {
            let metadata = service.load_table_metadata(first, false).await?;
            assert_eq!(metadata.name, *first);
        }
        Ok::<(), String>(())
    });

    assert!(result.is_ok(), "describe failed: {result:?}");
}
