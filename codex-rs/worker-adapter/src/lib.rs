mod app_server;
mod config;
mod control_plane;
mod http_server;

use crate::app_server::AppServerHandle;
use crate::config::Config;
use crate::control_plane::ControlPlaneClient;
use crate::control_plane::LeaseMonitorResult;
use crate::control_plane::discover_instance_ip;
use crate::http_server::WorkerState;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use clap::Parser;
use codex_arg0::Arg0DispatchPaths;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::info;
use tracing::warn;
use tracing_subscriber::EnvFilter;

/// State claimed from the control plane before the multithreaded worker runtime starts.
pub struct WorkerBootstrap {
    config: Config,
    instance_ip: std::net::IpAddr,
    control_plane: ControlPlaneClient,
    lease: control_plane::Lease,
    codex_home: std::path::PathBuf,
}

impl WorkerBootstrap {
    /// Returns the leased home path that must be installed as the process `CODEX_HOME`.
    pub fn codex_home(&self) -> &std::path::Path {
        &self.codex_home
    }
}

/// Claims a home shard before arg0 dispatch and the multithreaded runtime are initialized.
pub async fn bootstrap() -> Result<WorkerBootstrap> {
    let config = Config::parse();
    config.validate()?;
    let instance_ip = discover_instance_ip(&config.control_plane_url).await?;
    let control_plane = ControlPlaneClient::new(&config)?;
    let lease = control_plane.claim(instance_ip).await?;
    let codex_home = match config.codex_home(&lease.home_shard_id) {
        Ok(codex_home) => codex_home,
        Err(error) => {
            release_after_startup_failure(&control_plane, &lease).await;
            return Err(error);
        }
    };
    if let Err(error) = tokio::fs::create_dir_all(&codex_home).await {
        release_after_startup_failure(&control_plane, &lease).await;
        return Err(error)
            .with_context(|| format!("failed to create CODEX_HOME {}", codex_home.display()));
    }

    Ok(WorkerBootstrap {
        config,
        instance_ip,
        control_plane,
        lease,
        codex_home,
    })
}

/// Runs the worker adapter until it receives a shutdown signal or loses its lease.
pub async fn run(arg0_paths: Arg0DispatchPaths, bootstrap: WorkerBootstrap) -> Result<()> {
    init_tracing();
    let WorkerBootstrap {
        config,
        instance_ip,
        control_plane,
        lease,
        codex_home,
    } = bootstrap;
    let (lease_stop_tx, lease_stop_rx) = watch::channel(false);
    let mut lease_task = tokio::spawn({
        let control_plane = control_plane.clone();
        let lease = lease.clone();
        let config = config.clone();
        async move { control_plane.monitor(&config, &lease, lease_stop_rx).await }
    });
    let app_server = match AppServerHandle::spawn(arg0_paths, &codex_home).await {
        Ok(app_server) => app_server,
        Err(error) => {
            let _ = lease_stop_tx.send(true);
            lease_task.abort();
            release_after_startup_failure(&control_plane, &lease).await;
            return Err(error);
        }
    };
    let ready = Arc::new(AtomicBool::new(false));
    let state = WorkerState::new(app_server.clone(), ready.clone());
    let bind_addr = config.bind_addr();
    let listener = match tokio::net::TcpListener::bind(bind_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            let _ = lease_stop_tx.send(true);
            lease_task.abort();
            app_server.shutdown(config.shutdown_grace()).await;
            release_after_startup_failure(&control_plane, &lease).await;
            return Err(error)
                .with_context(|| format!("failed to bind worker adapter to {bind_addr}"));
        }
    };
    let (http_stop_tx, http_stop_rx) = oneshot::channel();
    let mut http_task = tokio::spawn(async move {
        axum::serve(listener, http_server::router(state))
            .with_graceful_shutdown(async {
                let _ = http_stop_rx.await;
            })
            .await
    });

    ready.store(true, Ordering::Release);
    info!(
        home_shard_id = %lease.home_shard_id,
        generation = lease.generation,
        %instance_ip,
        codex_home = %codex_home.display(),
        "worker adapter is ready"
    );

    let reason = tokio::select! {
        _ = shutdown_signal() => ShutdownReason::Signal,
        result = &mut lease_task => match result {
            Ok(LeaseMonitorResult::Lost) => ShutdownReason::LeaseLost,
            Ok(LeaseMonitorResult::Stopped) => ShutdownReason::LeaseMonitorStopped,
            Err(error) => ShutdownReason::BackgroundTaskFailed(error.to_string()),
        },
        reason = app_server.wait_for_exit() => ShutdownReason::AppServerExited(reason),
        result = &mut http_task => ShutdownReason::HttpServerExited(
            result
                .map_err(anyhow::Error::from)
                .and_then(|result| result.map_err(anyhow::Error::from))
                .err()
                .map(|error| error.to_string()),
        ),
    };

    ready.store(false, Ordering::Release);
    let _ = lease_stop_tx.send(true);
    lease_task.abort();
    let _ = http_stop_tx.send(());
    app_server.shutdown(config.shutdown_grace()).await;

    if !matches!(reason, ShutdownReason::LeaseLost)
        && let Err(error) = control_plane.release(&lease).await
    {
        warn!(%error, "failed to release home-shard lease during shutdown");
    }

    match reason {
        ShutdownReason::Signal => {
            info!("worker adapter stopped");
            Ok(())
        }
        ShutdownReason::LeaseLost => bail!("home-shard lease was fenced or lost"),
        ShutdownReason::LeaseMonitorStopped => bail!("home-shard lease monitor stopped"),
        ShutdownReason::AppServerExited(reason) => bail!("{reason}"),
        ShutdownReason::HttpServerExited(error) => {
            bail!(
                "worker HTTP server exited: {}",
                error.unwrap_or_else(|| "clean exit".to_string())
            )
        }
        ShutdownReason::BackgroundTaskFailed(error) => bail!("background task failed: {error}"),
    }
}

async fn release_after_startup_failure(
    control_plane: &ControlPlaneClient,
    lease: &control_plane::Lease,
) {
    if let Err(error) = control_plane.release(lease).await {
        warn!(%error, "failed to release home-shard lease after startup failure");
    }
}

#[derive(Debug)]
enum ShutdownReason {
    Signal,
    LeaseLost,
    LeaseMonitorStopped,
    AppServerExited(String),
    HttpServerExited(Option<String>),
    BackgroundTaskFailed(String),
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::SignalKind;
    if let Ok(mut terminate) = tokio::signal::unix::signal(SignalKind::terminate()) {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    } else {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
