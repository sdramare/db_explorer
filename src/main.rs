use std::sync::mpsc;

use db_explorer::app::DbExplorerApp;
use db_explorer::messages::{WorkerCommand, WorkerEvent};
use db_explorer::worker::spawn_worker;
use tracing::info;
use tracing_subscriber::EnvFilter;

const COMMAND_CHANNEL_CAPACITY: usize = 32;

#[tokio::main]
async fn main() -> Result<(), eframe::Error> {
    init_tracing();
    info!("starting DynamoDB Explorer");

    configure_process_env_for_wsl();

    let (command_tx, command_rx) =
        tokio::sync::mpsc::channel::<WorkerCommand>(COMMAND_CHANNEL_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>();

    let mut options = eframe::NativeOptions::default();
    configure_native_options(&mut options);

    spawn_worker(command_rx, event_tx);

    eframe::run_native(
        "DynamoDB Explorer",
        options,
        Box::new(move |_cc| Ok(Box::new(DbExplorerApp::new(command_tx, event_rx)))),
    )
}

#[cfg(target_os = "linux")]
fn configure_native_options(options: &mut eframe::NativeOptions) {
    if !is_wsl() {
        return;
    }

    if std::env::var_os("DISPLAY").is_some() {
        info!("wsl detected with DISPLAY; forcing winit X11 backend");
        options.event_loop_builder = Some(Box::new(|builder| {
            use winit::platform::x11::EventLoopBuilderExtX11;

            builder.with_x11();
        }));
    }
}

#[cfg(not(target_os = "linux"))]
fn configure_native_options(_options: &mut eframe::NativeOptions) {}

#[cfg(target_os = "linux")]
fn configure_process_env_for_wsl() {
    if !is_wsl() || std::env::var_os("LIBGL_ALWAYS_SOFTWARE").is_some() {
        return;
    }

    info!("wsl detected; enabling software OpenGL for startup stability");
    // Safety: this runs before the worker thread is spawned, so no other thread is
    // concurrently reading or writing process environment variables.
    unsafe {
        std::env::set_var("LIBGL_ALWAYS_SOFTWARE", "1");
    }
}

#[cfg(not(target_os = "linux"))]
fn configure_process_env_for_wsl() {}

#[cfg(target_os = "linux")]
fn is_wsl() -> bool {
    if std::env::var_os("WSL_DISTRO_NAME").is_some() {
        return true;
    }

    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|value| value.to_ascii_lowercase().contains("microsoft"))
        .unwrap_or(false)
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("db_explorer=info,warn"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();
}
