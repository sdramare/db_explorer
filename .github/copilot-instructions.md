# Project Guidelines

## Code Style
- Use idiomatic Rust and keep modules focused by responsibility:
  - UI state and rendering in `src/app.rs`
  - AWS integration in `src/aws/dynamodb.rs`
  - Cross-thread message contracts in `src/messages.rs`
  - Worker orchestration in `src/worker.rs`
- Prefer explicit domain enums/structs for UI state transitions (follow patterns in `MetadataState`, `ExportState`, and `ItemsState`).
- Keep async network/file operations in the worker layer; avoid blocking or awaiting SDK calls from UI code.

## Architecture
- The app uses a split runtime model:
  - UI thread (egui/eframe app state + rendering)
  - Background Tokio worker (AWS DynamoDB calls and export I/O)
- Communication is command/event based:
  - UI -> worker: `WorkerCommand` via Tokio MPSC channel
  - Worker -> UI: `WorkerEvent` via std MPSC channel
- Preserve request-id matching semantics when adding new commands/events so stale responses are ignored.
- See `README.md` for a full module map and runtime flow.

## Build and Test
- Build and run the desktop app: `cargo run`
- Run tests: `cargo test`
- Run live AWS integration tests (opt-in): `cargo test --features live-aws-tests --test live_dynamodb`
- Useful debug logging while running: `RUST_LOG=db_explorer=debug cargo run`

## Conventions
- AWS credentials/profile and region must be configured for runtime and live tests.
- Keep user-facing AWS errors actionable (service layer maps SDK failures to readable messages).
- UI should remain responsive:
  - Drain worker events non-blockingly each frame.
  - Request repaint while operations are pending.
- For DynamoDB metadata:
  - Treat `DescribeTable.item_count` as approximate.
  - Use explicit recount (`Scan` + `Select::Count`) only when requested.

## References
- Primary project documentation: `README.md`
