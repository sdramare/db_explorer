# db_explorer

Desktop DynamoDB explorer built with Rust, egui/eframe, and the AWS SDK for Rust.

## What It Does

- Lists DynamoDB tables for the active AWS profile and region.
- Lets you select a table and view metadata:
  - Table name
  - Creation date
  - Item count from `DescribeTable` (approximate)
  - Optional exact recount (computed by paginated `Scan` with `Select::Count`)
- Exports the full contents of the selected table to a local JSON file via a Save As dialog.
- Shows loading states and user-friendly AWS error messages.

## Architecture

The app uses a two-part runtime design:

- UI thread: egui app state and rendering
- Async worker task: executes AWS API calls

Communication is message-based and unidirectional:

- UI -> worker via Tokio unbounded channel (`WorkerCommand`)
- Worker -> UI via std MPSC channel (`WorkerEvent`)

Request IDs are attached to every command/event pair so stale responses can be ignored safely.

### High-Level Flow

1. App starts and initializes tracing.
2. Main creates channels and spawns a Tokio worker.
3. UI sends `LoadTables`.
4. Worker calls DynamoDB API and sends `TablesLoaded`.
5. User selects a table.
6. UI sends `LoadTableMetadata`.
7. Worker calls `DescribeTable` and sends `TableMetadataLoaded`.
8. User can trigger an explicit exact recount for the selected table.
9. User can trigger `Export JSON` and choose a local output path.
10. Worker scans the entire table and writes pretty-printed JSON locally.
11. UI state updates and re-renders.

### Sequence Diagram (Runtime)

```text
+-------------+        WorkerCommand         +----------------+      AWS SDK Calls      +-----------+
| egui UI App | ---------------------------> | Tokio Worker   | ----------------------> | DynamoDB  |
| (DbExplorer)|   LoadTables {request_id}   | (background)   |   ListTables            | Service   |
+-------------+                              +----------------+                         +-----------+
  ^                                            |
  |               WorkerEvent                 |
  | <-----------------------------------------+
  |      TablesLoaded {request_id, result}
  |
  | (user selects table)
  |
  |               WorkerCommand
  +------------------------------------------>
      LoadTableMetadata {request_id, table_name, exact_item_count=false}

    | (optional exact recount)
    |
    |               WorkerCommand
    +------------------------------------------>
      LoadTableMetadata {request_id, table_name, exact_item_count=true}

  ^                                            |
  |               WorkerEvent                 |
  | <-----------------------------------------+
  | TableMetadataLoaded {request_id, table_name, result}

Note: The UI applies events only when `request_id` matches the active request.
```

## Module Map

- `src/main.rs`
  - Entry point
  - Tracing setup
  - Channel wiring and worker task loop
  - eframe app startup

- `src/app.rs`
  - `DbExplorerApp`: egui UI + interaction handling
  - `AppState`: state machine for tables and metadata
  - Stale response protection via active request IDs

- `src/messages.rs`
  - Message contract between UI and worker:
    - `WorkerCommand`
    - `WorkerEvent`

- `src/aws/dynamodb.rs`
  - `DynamoDbService`: async AWS DynamoDB operations
    - `list_tables` with pagination
    - `load_table_metadata`
    - `count_items_exact` via paginated scan (on explicit recount)
  - Error mapping for common AWS credential/permission/region failures
  - `TableMetadata` domain model

- `src/lib.rs`
  - Public module exports

- `tests/live_dynamodb.rs`
  - Optional live integration tests behind `live-aws-tests` feature

## Concurrency Model

- UI never blocks on AWS calls.
- Worker performs all network operations asynchronously.
- UI drains events each frame with `try_recv`.
- UI requests repaint while requests are pending.

This keeps the UI responsive while requests are in flight.

## Error Handling Strategy

- Service layer converts raw AWS errors into actionable strings.
- UI layer stores errors in state and renders them directly.
- Request ID matching prevents out-of-order results from corrupting state.

## Running

Prerequisites:

- Rust toolchain
- Valid AWS credentials/profile
- AWS region configured

Run app:

```bash
cargo run
```

Run unit tests:

```bash
cargo test
```

Run live AWS tests (opt-in):

```bash
cargo test --features live-aws-tests --test live_dynamodb
```

## Logging

Tracing is enabled by default and can be configured with `RUST_LOG`.

Example:

```bash
RUST_LOG=db_explorer=debug cargo run
```