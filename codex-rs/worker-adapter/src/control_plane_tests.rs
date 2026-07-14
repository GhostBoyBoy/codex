use super::*;
use crate::config::Config;
use pretty_assertions::assert_eq;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::body_json;
use wiremock::matchers::method;
use wiremock::matchers::path;

#[tokio::test]
async fn claim_reports_the_instance_ip_and_deserializes_the_lease() {
    let server = MockServer::start().await;
    let config = test_config(server.uri());
    Mock::given(method("POST"))
        .and(path("/api/v1/home-shards/claim"))
        .and(body_json(serde_json::json!({
            "instanceIp": "10.0.0.8"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "homeShardId": "home-1",
            "leaseToken": "lease-1",
            "generation": 4,
            "leaseTtlSeconds": 30
        })))
        .expect(1)
        .mount(&server)
        .await;

    let lease = ControlPlaneClient::new(&config)
        .unwrap()
        .claim("10.0.0.8".parse().unwrap())
        .await
        .unwrap();

    assert_eq!(
        lease,
        Lease {
            home_shard_id: "home-1".to_string(),
            lease_token: "lease-1".to_string(),
            generation: 4,
            lease_ttl_seconds: 30,
        }
    );
}

#[tokio::test]
async fn renew_treats_a_generation_conflict_as_fencing() {
    let server = MockServer::start().await;
    let config = test_config(server.uri());
    let lease = Lease {
        home_shard_id: "home-1".to_string(),
        lease_token: "lease-1".to_string(),
        generation: 4,
        lease_ttl_seconds: 30,
    };
    Mock::given(method("POST"))
        .and(path("/api/v1/home-shards/home-1/renew"))
        .and(body_json(serde_json::json!({
            "leaseToken": "lease-1",
            "generation": 4
        })))
        .respond_with(ResponseTemplate::new(409))
        .expect(1)
        .mount(&server)
        .await;

    let result = ControlPlaneClient::new(&config)
        .unwrap()
        .renew(&lease)
        .await
        .unwrap();

    assert_eq!(result, RenewResult::Fenced);
}

fn test_config(control_plane_url: String) -> Config {
    Config { control_plane_url }
}
