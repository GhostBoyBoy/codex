use crate::config::Config;
use crate::config::MAX_MISSED_HEARTBEATS;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;
use tracing::warn;

#[derive(Clone)]
pub(crate) struct ControlPlaneClient {
    base_url: String,
    http: reqwest::Client,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Lease {
    pub(crate) home_shard_id: String,
    pub(crate) lease_token: String,
    pub(crate) generation: u64,
    pub(crate) lease_ttl_seconds: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClaimRequest<'a> {
    instance_ip: &'a IpAddr,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LeaseRequest<'a> {
    lease_token: &'a str,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeaseMonitorResult {
    Stopped,
    Lost,
}

impl ControlPlaneClient {
    pub(crate) fn new(config: &Config) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(config.control_plane_timeout())
            .build()
            .context("failed to build control-plane HTTP client")?;
        Ok(Self {
            base_url: config.control_plane_url.trim_end_matches('/').to_string(),
            http,
        })
    }

    pub(crate) async fn claim(&self, instance_ip: IpAddr) -> Result<Lease> {
        let response = self
            .http
            .post(self.endpoint("/api/v1/home-shards/claim"))
            .json(&ClaimRequest {
                instance_ip: &instance_ip,
            })
            .send()
            .await
            .context("failed to claim a home shard")?
            .error_for_status()
            .context("control plane rejected the home-shard claim")?;
        let lease = response
            .json::<Lease>()
            .await
            .context("control plane returned an invalid home-shard lease")?;
        if lease.lease_ttl_seconds == 0 {
            bail!("control plane returned a zero-second lease TTL");
        }
        Ok(lease)
    }

    pub(crate) async fn monitor(
        &self,
        config: &Config,
        lease: &Lease,
        mut stop_rx: watch::Receiver<bool>,
    ) -> LeaseMonitorResult {
        let interval = config.heartbeat_interval();
        let ttl = Duration::from_secs(lease.lease_ttl_seconds);
        if interval >= ttl {
            warn!(
                ?interval,
                ?ttl,
                "heartbeat interval is not shorter than the lease TTL"
            );
            return LeaseMonitorResult::Lost;
        }
        let mut last_success = Instant::now();
        let mut failures = 0;

        loop {
            tokio::select! {
                result = stop_rx.changed() => {
                    if result.is_err() || *stop_rx.borrow() {
                        return LeaseMonitorResult::Stopped;
                    }
                }
                _ = tokio::time::sleep(interval) => {
                    match self.renew(lease).await {
                        Ok(RenewResult::Renewed) => {
                            failures = 0;
                            last_success = Instant::now();
                        }
                        Ok(RenewResult::Fenced) => return LeaseMonitorResult::Lost,
                        Err(error) => {
                            failures += 1;
                            warn!(failures, %error, "home-shard heartbeat failed");
                            if failures >= MAX_MISSED_HEARTBEATS
                                || last_success.elapsed() >= ttl
                            {
                                return LeaseMonitorResult::Lost;
                            }
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn release(&self, lease: &Lease) -> Result<()> {
        self.lease_request(lease, "release")
            .send()
            .await
            .context("failed to release home-shard lease")?
            .error_for_status()
            .context("control plane rejected the home-shard release")?;
        Ok(())
    }

    async fn renew(&self, lease: &Lease) -> Result<RenewResult> {
        let response = self
            .lease_request(lease, "renew")
            .send()
            .await
            .context("failed to renew home-shard lease")?;
        if matches!(
            response.status(),
            StatusCode::CONFLICT | StatusCode::GONE | StatusCode::LOCKED
        ) {
            return Ok(RenewResult::Fenced);
        }
        response
            .error_for_status()
            .context("control plane rejected the home-shard heartbeat")?;
        Ok(RenewResult::Renewed)
    }

    fn lease_request(&self, lease: &Lease, action: &str) -> reqwest::RequestBuilder {
        self.http
            .post(self.endpoint(&format!(
                "/api/v1/home-shards/{}/{action}",
                lease.home_shard_id
            )))
            .json(&LeaseRequest {
                lease_token: &lease.lease_token,
                generation: lease.generation,
            })
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

pub(crate) async fn discover_instance_ip(control_plane_url: &str) -> Result<IpAddr> {
    let url = reqwest::Url::parse(control_plane_url).context("invalid control-plane URL")?;
    let host = url.host_str().context("control-plane URL has no host")?;
    let port = url
        .port_or_known_default()
        .context("control-plane URL has no known port")?;
    let target = tokio::net::lookup_host((host, port))
        .await
        .context("failed to resolve control-plane host")?
        .next()
        .context("control-plane host resolved to no addresses")?;
    let bind_addr = if target.is_ipv4() {
        SocketAddr::from(([0, 0, 0, 0], 0))
    } else {
        SocketAddr::from(([0; 8], 0))
    };
    let socket = tokio::net::UdpSocket::bind(bind_addr)
        .await
        .context("failed to create Pod IP discovery socket")?;
    socket
        .connect(target)
        .await
        .context("failed to select network route to control plane")?;
    let instance_ip = socket
        .local_addr()
        .context("failed to inspect Pod IP discovery socket")?
        .ip();
    if instance_ip.is_loopback() || instance_ip.is_unspecified() {
        bail!("resolved Pod IP {instance_ip} is not routable");
    }
    Ok(instance_ip)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RenewResult {
    Renewed,
    Fenced,
}

#[cfg(test)]
#[path = "control_plane_tests.rs"]
mod tests;
