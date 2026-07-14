use anyhow::Context;
use anyhow::Result;
use codex_app_server_client::DEFAULT_IN_PROCESS_CHANNEL_CAPACITY;
use codex_app_server_client::EnvironmentManager;
use codex_app_server_client::ExecServerRuntimePaths;
use codex_app_server_client::InProcessAppServerClient;
use codex_app_server_client::InProcessClientStartArgs;
use codex_app_server_client::InProcessServerEvent;
use codex_app_server_client::legacy_core::config::ConfigBuilder;
use codex_app_server_protocol::ClientNotification;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ConfigWarningNotification;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCNotification;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_feedback::CodexFeedback;
use codex_login::default_client::get_codex_user_agent;
use codex_protocol::protocol::SessionSource;
use serde::Serialize;
use serde_json::json;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::warn;

const INPUT_CAPACITY: usize = 256;
const OUTPUT_CAPACITY: usize = 1024;
const INVALID_REQUEST_CODE: i64 = -32600;
const INTERNAL_ERROR_CODE: i64 = -32603;

#[derive(Clone)]
pub(crate) struct AppServerHandle {
    input_tx: mpsc::Sender<JSONRPCMessage>,
    output_tx: broadcast::Sender<String>,
    stop_tx: watch::Sender<bool>,
    exit_rx: watch::Receiver<Option<String>>,
}

impl AppServerHandle {
    pub(crate) async fn spawn(arg0_paths: Arg0DispatchPaths, codex_home: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(codex_home)
            .await
            .with_context(|| format!("failed to create CODEX_HOME {}", codex_home.display()))?;

        let loader_overrides = LoaderOverrides::default();
        let cloud_config_bundle = CloudConfigBundleLoader::default();
        let config = ConfigBuilder::default()
            .codex_home(codex_home.to_path_buf())
            .loader_overrides(loader_overrides.clone())
            .cloud_config_bundle(cloud_config_bundle.clone())
            .build()
            .await
            .context("failed to load app-server config")?;
        let config_warnings = config
            .startup_warnings
            .iter()
            .map(|summary| ConfigWarningNotification {
                summary: summary.clone(),
                details: None,
                path: None,
                range: None,
            })
            .collect();
        let state_db = codex_rollout::state_db::init(&config).await;
        let runtime_paths = ExecServerRuntimePaths::from_optional_paths(
            arg0_paths.codex_self_exe.clone(),
            arg0_paths.codex_linux_sandbox_exe.clone(),
        )
        .context("failed to resolve embedded app-server runtime paths")?;
        let environment_manager =
            EnvironmentManager::from_codex_home(config.codex_home.clone(), Some(runtime_paths))
                .await
                .context("failed to initialize app-server environments")?;
        let client = InProcessAppServerClient::start(InProcessClientStartArgs {
            arg0_paths,
            config: Arc::new(config),
            cli_overrides: Vec::new(),
            loader_overrides,
            strict_config: false,
            cloud_config_bundle,
            feedback: CodexFeedback::new(),
            log_db: None,
            state_db,
            environment_manager: Arc::new(environment_manager),
            config_warnings: Vec::new(),
            session_source: SessionSource::Custom("worker-adapter".to_string()),
            enable_codex_api_key_env: false,
            client_name: "codex_worker_adapter".to_string(),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            experimental_api: true,
            mcp_server_openai_form_elicitation: false,
            opt_out_notification_methods: Vec::new(),
            channel_capacity: DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
        })
        .await
        .context("failed to start embedded app-server")?;
        let initialize_result = json!({
            "userAgent": get_codex_user_agent(),
            "codexHome": codex_home,
            "platformFamily": std::env::consts::FAMILY,
            "platformOs": std::env::consts::OS,
        });

        let (input_tx, input_rx) = mpsc::channel(INPUT_CAPACITY);
        let (output_tx, _) = broadcast::channel(OUTPUT_CAPACITY);
        let (stop_tx, stop_rx) = watch::channel(false);
        let (exit_tx, exit_rx) = watch::channel(None);
        tokio::spawn(run_embedded_app_server(
            client,
            input_rx,
            output_tx.clone(),
            stop_rx,
            exit_tx,
            initialize_result,
            config_warnings,
        ));

        Ok(Self {
            input_tx,
            output_tx,
            stop_tx,
            exit_rx,
        })
    }

    pub(crate) async fn send(&self, message: String) -> Result<()> {
        let message = serde_json::from_str(&message).context("invalid JSON-RPC message")?;
        self.input_tx
            .send(message)
            .await
            .context("embedded app-server input channel is closed")
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<String> {
        self.output_tx.subscribe()
    }

    pub(crate) async fn wait_for_exit(&self) -> String {
        let mut exit_rx = self.exit_rx.clone();
        loop {
            if let Some(reason) = exit_rx.borrow().clone() {
                return reason;
            }
            if exit_rx.changed().await.is_err() {
                return "embedded app-server stopped without an exit status".to_string();
            }
        }
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) {
        let _ = self.stop_tx.send(true);
        if tokio::time::timeout(timeout, self.wait_for_exit())
            .await
            .is_err()
        {
            warn!("timed out waiting for embedded app-server");
        }
    }
}

async fn run_embedded_app_server(
    mut client: InProcessAppServerClient,
    mut input_rx: mpsc::Receiver<JSONRPCMessage>,
    output_tx: broadcast::Sender<String>,
    mut stop_rx: watch::Receiver<bool>,
    exit_tx: watch::Sender<Option<String>>,
    initialize_result: serde_json::Value,
    config_warnings: Vec<ConfigWarningNotification>,
) {
    let reason = loop {
        tokio::select! {
            changed = stop_rx.changed() => {
                let _ = changed;
                break "embedded app-server stopped".to_string();
            }
            input = input_rx.recv() => {
                let Some(input) = input else {
                    break "embedded app-server input channel closed".to_string();
                };
                handle_client_message(
                    &client,
                    input,
                    &output_tx,
                    &initialize_result,
                    &config_warnings,
                )
                .await;
            }
            event = client.next_event() => {
                match event {
                    Some(InProcessServerEvent::ServerNotification(notification)) => {
                        send_serialized(&output_tx, &notification);
                    }
                    Some(InProcessServerEvent::ServerRequest(request)) => {
                        send_serialized(&output_tx, &request);
                    }
                    Some(InProcessServerEvent::Lagged { skipped }) => {
                        break format!("embedded app-server event stream lost {skipped} event(s)");
                    }
                    None => break "embedded app-server exited".to_string(),
                }
            }
        }
    };

    if let Err(error) = client.shutdown().await {
        warn!(%error, "failed to shut down embedded app-server");
    }
    let _ = exit_tx.send(Some(reason));
}

async fn handle_client_message(
    client: &InProcessAppServerClient,
    message: JSONRPCMessage,
    output_tx: &broadcast::Sender<String>,
    initialize_result: &serde_json::Value,
    config_warnings: &[ConfigWarningNotification],
) {
    match message {
        JSONRPCMessage::Request(request) if request.method == "initialize" => {
            match external_initialize_response(request, initialize_result) {
                Ok(response) => {
                    send_serialized(output_tx, &response);
                    for warning in config_warnings {
                        send_serialized(
                            output_tx,
                            &ServerNotification::ConfigWarning(warning.clone()),
                        );
                    }
                }
                Err(request_id) => {
                    send_invalid_request(output_tx, request_id, "invalid initialize request");
                }
            }
        }
        JSONRPCMessage::Request(request) => {
            let request_id = request.id.clone();
            match client_request_from_jsonrpc(request) {
                Ok(request) => {
                    let request_handle = client.request_handle();
                    let output_tx = output_tx.clone();
                    tokio::spawn(async move {
                        let response = match request_handle.request(request).await {
                            Ok(Ok(result)) => JSONRPCMessage::Response(JSONRPCResponse {
                                id: request_id,
                                result,
                            }),
                            Ok(Err(error)) => JSONRPCMessage::Error(JSONRPCError {
                                error,
                                id: request_id,
                            }),
                            Err(error) => JSONRPCMessage::Error(JSONRPCError {
                                error: JSONRPCErrorError {
                                    code: INTERNAL_ERROR_CODE,
                                    message: error.to_string(),
                                    data: None,
                                },
                                id: request_id,
                            }),
                        };
                        send_serialized(&output_tx, &response);
                    });
                }
                Err(error) => {
                    warn!(%error, "rejecting invalid or unsupported app-server request");
                    send_invalid_request(
                        output_tx,
                        request_id,
                        "invalid or unsupported app-server request",
                    );
                }
            }
        }
        JSONRPCMessage::Notification(notification) if notification.method == "initialized" => {}
        JSONRPCMessage::Notification(notification) => {
            match client_notification_from_jsonrpc(notification) {
                Ok(notification) => {
                    if let Err(error) = client.notify(notification).await {
                        warn!(%error, "failed to forward app-server notification");
                    }
                }
                Err(error) => warn!(%error, "ignoring invalid app-server notification"),
            }
        }
        JSONRPCMessage::Response(response) => {
            if let Err(error) = client
                .resolve_server_request(response.id, response.result)
                .await
            {
                warn!(%error, "failed to resolve app-server request");
            }
        }
        JSONRPCMessage::Error(error) => {
            if let Err(send_error) = client.reject_server_request(error.id, error.error).await {
                warn!(%send_error, "failed to reject app-server request");
            }
        }
    }
}

fn client_request_from_jsonrpc(request: JSONRPCRequest) -> serde_json::Result<ClientRequest> {
    serde_json::from_value(serde_json::to_value(request)?)
}

fn client_notification_from_jsonrpc(
    notification: JSONRPCNotification,
) -> serde_json::Result<ClientNotification> {
    serde_json::from_value(serde_json::to_value(notification)?)
}

fn external_initialize_response(
    request: JSONRPCRequest,
    initialize_result: &serde_json::Value,
) -> std::result::Result<JSONRPCResponse, RequestId> {
    let request_id = request.id.clone();
    match client_request_from_jsonrpc(request) {
        Ok(ClientRequest::Initialize { .. }) => Ok(JSONRPCResponse {
            id: request_id,
            result: initialize_result.clone(),
        }),
        Ok(_) | Err(_) => Err(request_id),
    }
}

fn send_invalid_request(output_tx: &broadcast::Sender<String>, id: RequestId, message: &str) {
    send_serialized(
        output_tx,
        &JSONRPCError {
            error: JSONRPCErrorError {
                code: INVALID_REQUEST_CODE,
                message: message.to_string(),
                data: None,
            },
            id,
        },
    );
}

fn send_serialized(output_tx: &broadcast::Sender<String>, message: &impl Serialize) {
    match serde_json::to_string(message) {
        Ok(message) => {
            let _ = output_tx.send(message);
        }
        Err(error) => warn!(%error, "failed to serialize app-server message"),
    }
}

#[cfg(test)]
#[path = "app_server_tests.rs"]
mod tests;
