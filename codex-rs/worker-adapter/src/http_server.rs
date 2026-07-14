use crate::app_server::AppServerHandle;
use axum::Router;
use axum::extract::State;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use futures::SinkExt;
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;
use tracing::warn;

#[derive(Clone)]
pub(crate) struct WorkerState {
    pub(crate) app_server: AppServerHandle,
    pub(crate) ready: Arc<AtomicBool>,
    connection_slot: Arc<Semaphore>,
}

impl WorkerState {
    pub(crate) fn new(app_server: AppServerHandle, ready: Arc<AtomicBool>) -> Self {
        Self {
            app_server,
            ready,
            connection_slot: Arc::new(Semaphore::new(1)),
        }
    }
}

pub(crate) fn router(state: WorkerState) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/rpc", get(rpc))
        .with_state(state)
}

async fn live() -> StatusCode {
    StatusCode::OK
}

async fn ready(State(state): State<WorkerState>) -> StatusCode {
    if state.ready.load(Ordering::Acquire) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn rpc(State(state): State<WorkerState>, upgrade: WebSocketUpgrade) -> Response {
    if !state.ready.load(Ordering::Acquire) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Ok(permit) = state.connection_slot.clone().try_acquire_owned() else {
        return StatusCode::CONFLICT.into_response();
    };
    upgrade
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            bridge(socket, state.app_server).await;
        })
        .into_response()
}

async fn bridge(socket: WebSocket, app_server: AppServerHandle) {
    let (mut websocket_tx, mut websocket_rx) = socket.split();
    let mut app_server_rx = app_server.subscribe();
    loop {
        tokio::select! {
            incoming = websocket_rx.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if serde_json::from_str::<serde_json::Value>(&text).is_err() {
                            warn!("closing worker connection after invalid JSON-RPC frame");
                            break;
                        }
                        if app_server.send(text.to_string()).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if websocket_tx.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(Message::Binary(_))) => {
                        warn!("closing worker connection after unsupported binary frame");
                        break;
                    }
                }
            }
            outgoing = app_server_rx.recv() => {
                match outgoing {
                    Ok(message) => {
                        if websocket_tx.send(Message::Text(message.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "worker connection lagged behind app-server events");
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = app_server.wait_for_exit() => break,
        }
    }
}
