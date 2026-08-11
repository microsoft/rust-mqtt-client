// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Verifies transport setup, security parameters, and handshake failures.

use std::time::Duration;

use async_tungstenite::tungstenite::client::IntoClientRequest as _;
use ms_mqtt_client::client::{
    ClientOptions, ConnectResult, DisconnectedEvent, KeepAliveConfig, new_client,
};
use ms_mqtt_client::error::ConnectError;
use ms_mqtt_client::packet::ConnectProperties;
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};

use crate::common::fixture::FixtureCapability;
use crate::common::{
    ENV_MQTT_MTLS_PORT, ENV_MQTT_PORT, ENV_MQTT_TLS_PORT, ENV_MQTT_WS_PORT, ENV_MQTT_WSS_PORT,
    MTLS_PORT, TCP_PORT, TLS_PORT, WS_PORT, WSS_PORT, connect_with_transport, empty_tls_config,
    mutual_tls_config, mutual_tls_config_with_server_only_certificate,
    mutual_tls_config_with_untrusted_client, port_from_env, tls_config,
};

async fn connect_and_expect_failure(
    transport_type: ConnectionTransportType,
    client_id: &str,
) -> ConnectError {
    let options = ClientOptions {
        client_id: Some(client_id.to_string()),
        ..Default::default()
    };
    let (_client, connect_handle, _receiver) = new_client(options);
    match connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type,
                timeout: Some(Duration::from_secs(2)),
                proxy: None,
                tcp_nodelay: false,
            },
            true,
            KeepAliveConfig::Infinite,
            None,
            None,
            None,
            ConnectProperties::default(),
            Some(Duration::from_secs(2)),
        )
        .await
    {
        ConnectResult::Failure(_, error) => error,
        ConnectResult::Success(..) => panic!("connection unexpectedly succeeded"),
    }
}

async fn connect_and_expect_application_disconnect(
    transport_type: ConnectionTransportType,
    client_id: &str,
) {
    let connection =
        connect_with_transport(transport_type, client_id, KeepAliveConfig::Infinite).await;
    assert!(matches!(
        connection.disconnect().await,
        DisconnectedEvent::ApplicationDisconnect
    ));
}

/// Verifies that TLS accepts a server certificate signed by the configured trusted CA.
#[tokio::test]
async fn tls_accepts_trusted_server_certificate() {
    connect_and_expect_application_disconnect(
        ConnectionTransportType::Tls {
            hostname: "localhost".to_string(),
            port: port_from_env(ENV_MQTT_TLS_PORT, TLS_PORT),
            tls_config: tls_config(),
        },
        "transport_tls_trusted",
    )
    .await;
}

/// Verifies that secure WebSocket accepts a server certificate signed by the configured trusted
/// CA and completes the HTTP Upgrade handshake.
#[tokio::test]
async fn secure_websocket_accepts_trusted_server_certificate() {
    connect_and_expect_application_disconnect(
        ConnectionTransportType::Ws {
            request: format!(
                "wss://localhost:{}/mqtt",
                port_from_env(ENV_MQTT_WSS_PORT, WSS_PORT)
            )
            .into_client_request()
            .expect("secure WebSocket URL should be valid"),
            tls_config: Some(tls_config()),
        },
        "transport_wss_trusted",
    )
    .await;
}

/// Verifies that a client certificate accepted by the mTLS fixture can connect and disconnect
/// cleanly.
#[tokio::test]
async fn mutual_tls_connect_disconnect() {
    crate::require_fixture_capability!(FixtureCapability::MutualTls);
    let connection = connect_with_transport(
        ConnectionTransportType::Tls {
            hostname: "localhost".to_string(),
            port: port_from_env(ENV_MQTT_MTLS_PORT, MTLS_PORT),
            tls_config: mutual_tls_config(),
        },
        "transport_mutual_tls",
        KeepAliveConfig::Infinite,
    )
    .await;
    assert!(matches!(
        connection.disconnect().await,
        DisconnectedEvent::ApplicationDisconnect
    ));
}

/// Verifies that TLS connection establishment rejects a server certificate whose issuer is not
/// trusted by the client.
#[tokio::test]
async fn tls_rejects_untrusted_server_certificate() {
    let error = connect_and_expect_failure(
        ConnectionTransportType::Tls {
            hostname: "localhost".to_string(),
            port: port_from_env(ENV_MQTT_TLS_PORT, TLS_PORT),
            tls_config: empty_tls_config(),
        },
        "transport_tls_untrusted",
    )
    .await;
    assert!(
        matches!(error, ConnectError::Io(_)),
        "unexpected error: {error}"
    );
}

/// Verifies that TLS hostname validation rejects a certificate whose subject alternative names
/// do not match the requested host.
#[tokio::test]
async fn tls_rejects_hostname_mismatch() {
    let error = connect_and_expect_failure(
        ConnectionTransportType::Tls {
            hostname: "127.0.0.1".to_string(),
            port: port_from_env(ENV_MQTT_TLS_PORT, TLS_PORT),
            tls_config: tls_config(),
        },
        "transport_tls_hostname_mismatch",
    )
    .await;
    assert!(
        matches!(error, ConnectError::Io(_)),
        "unexpected error: {error}"
    );
}

/// Verifies that an mTLS listener rejects a client that does not present a certificate.
#[tokio::test]
async fn mutual_tls_requires_client_certificate() {
    crate::require_fixture_capability!(FixtureCapability::MutualTls);
    let error = connect_and_expect_failure(
        ConnectionTransportType::Tls {
            hostname: "localhost".to_string(),
            port: port_from_env(ENV_MQTT_MTLS_PORT, MTLS_PORT),
            tls_config: tls_config(),
        },
        "transport_mutual_tls_missing_identity",
    )
    .await;
    assert!(
        matches!(error, ConnectError::Io(_)),
        "unexpected error: {error}"
    );
}

/// Verifies that an mTLS listener rejects a client certificate signed by an untrusted CA.
#[tokio::test]
async fn mutual_tls_rejects_untrusted_client_certificate() {
    crate::require_fixture_capability!(FixtureCapability::MutualTls);
    let error = connect_and_expect_failure(
        ConnectionTransportType::Tls {
            hostname: "localhost".to_string(),
            port: port_from_env(ENV_MQTT_MTLS_PORT, MTLS_PORT),
            tls_config: mutual_tls_config_with_untrusted_client(),
        },
        "transport_mutual_tls_untrusted_client",
    )
    .await;
    assert!(
        matches!(error, ConnectError::Io(_)),
        "unexpected error: {error}"
    );
}

/// Verifies that an mTLS listener rejects a certificate limited to server authentication rather
/// than client authentication.
#[tokio::test]
async fn mutual_tls_rejects_certificate_without_client_authentication_eku() {
    crate::require_fixture_capability!(FixtureCapability::MutualTls);
    let error = connect_and_expect_failure(
        ConnectionTransportType::Tls {
            hostname: "localhost".to_string(),
            port: port_from_env(ENV_MQTT_MTLS_PORT, MTLS_PORT),
            tls_config: mutual_tls_config_with_server_only_certificate(),
        },
        "transport_mutual_tls_wrong_eku",
    )
    .await;
    assert!(
        matches!(error, ConnectError::Io(_)),
        "unexpected error: {error}"
    );
}

/// Verifies that a WebSocket listener rejects an HTTP Upgrade request for an unconfigured path.
#[tokio::test]
async fn websocket_rejects_wrong_path() {
    crate::require_fixture_capability!(FixtureCapability::WebSocketPathValidation);
    let error = connect_and_expect_failure(
        ConnectionTransportType::Ws {
            request: format!(
                "ws://localhost:{}/not-mqtt",
                port_from_env(ENV_MQTT_WS_PORT, WS_PORT)
            )
            .into_client_request()
            .expect("WebSocket URL should be valid"),
            tls_config: None,
        },
        "transport_websocket_wrong_path",
    )
    .await;
    assert!(
        matches!(error, ConnectError::Io(_)),
        "unexpected error: {error}"
    );
}

/// Verifies that a WebSocket client rejects a plain MQTT endpoint that cannot complete the HTTP
/// Upgrade handshake.
#[tokio::test]
async fn websocket_rejects_plain_mqtt_endpoint() {
    let error = connect_and_expect_failure(
        ConnectionTransportType::Ws {
            request: format!(
                "ws://localhost:{}/mqtt",
                port_from_env(ENV_MQTT_PORT, TCP_PORT)
            )
            .into_client_request()
            .expect("WebSocket URL should be valid"),
            tls_config: None,
        },
        "transport_websocket_wrong_endpoint",
    )
    .await;
    assert!(
        matches!(error, ConnectError::Io(_)),
        "unexpected error: {error}"
    );
}
