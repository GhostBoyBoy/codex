use anyhow::Result;
use anyhow::bail;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

const CODEX_HOME_ROOT: &str = "/codex-home";
const WORKER_PORT: u16 = 8080;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
pub(crate) const MAX_MISSED_HEARTBEATS: u32 = 3;

#[derive(Clone, Debug, Parser)]
#[command(version, about = "Supervise a leased Codex app-server worker")]
pub(crate) struct Config {
    #[arg(long, env = "CONTROL_PLANE_URL")]
    pub(crate) control_plane_url: String,
}

impl Config {
    pub(crate) fn validate(&self) -> Result<()> {
        let url = reqwest::Url::parse(&self.control_plane_url)
            .map_err(|error| anyhow::anyhow!("invalid CONTROL_PLANE_URL: {error}"))?;
        if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
            bail!("CONTROL_PLANE_URL must be an absolute HTTP(S) URL");
        }
        Ok(())
    }

    pub(crate) fn codex_home(&self, home_shard_id: &str) -> Result<PathBuf> {
        validate_home_shard_id(home_shard_id)?;
        Ok(PathBuf::from(CODEX_HOME_ROOT).join(home_shard_id))
    }

    pub(crate) fn bind_addr(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], WORKER_PORT))
    }

    pub(crate) fn heartbeat_interval(&self) -> Duration {
        HEARTBEAT_INTERVAL
    }

    pub(crate) fn control_plane_timeout(&self) -> Duration {
        CONTROL_PLANE_TIMEOUT
    }

    pub(crate) fn shutdown_grace(&self) -> Duration {
        SHUTDOWN_GRACE
    }
}

fn validate_home_shard_id(value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    if !valid {
        bail!("control plane returned invalid homeShardId {value:?}");
    }
    Ok(())
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
