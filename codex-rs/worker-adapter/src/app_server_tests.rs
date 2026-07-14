use super::*;
use codex_app_server_protocol::ServerNotification;
use pretty_assertions::assert_eq;

#[test]
fn converts_jsonrpc_request_to_typed_client_request() {
    let request = JSONRPCRequest {
        id: RequestId::Integer(42),
        method: "thread/list".to_string(),
        params: Some(json!({})),
        trace: None,
    };

    let request = client_request_from_jsonrpc(request).expect("valid thread/list request");

    assert_eq!(request.id(), &RequestId::Integer(42));
    assert_eq!(request.method(), "thread/list");
}

#[test]
fn initialize_is_terminated_by_adapter() {
    let initialize_result = json!({"codexHome": "/codex-home/home-1"});
    let request = JSONRPCRequest {
        id: RequestId::String("init-1".to_string()),
        method: "initialize".to_string(),
        params: Some(json!({
            "clientInfo": {
                "name": "platform",
                "title": null,
                "version": "1.0.0"
            },
            "capabilities": null
        })),
        trace: None,
    };

    let response = external_initialize_response(request, &initialize_result)
        .expect("valid initialize request");

    assert_eq!(
        response,
        JSONRPCResponse {
            id: RequestId::String("init-1".to_string()),
            result: initialize_result,
        }
    );
}

#[test]
fn serializes_typed_server_notification_for_websocket() {
    let (output_tx, mut output_rx) = broadcast::channel(1);
    send_serialized(
        &output_tx,
        &ServerNotification::ConfigWarning(ConfigWarningNotification {
            summary: "warning".to_string(),
            details: None,
            path: None,
            range: None,
        }),
    );

    assert_eq!(
        output_rx.try_recv().expect("notification"),
        r#"{"method":"configWarning","params":{"summary":"warning","details":null}}"#
    );
}
