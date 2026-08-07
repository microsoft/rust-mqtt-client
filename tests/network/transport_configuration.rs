// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Verifies transport setup, security parameters, and handshake failures.

use std::time::Duration;

use async_tungstenite::tungstenite::client::IntoClientRequest as _;
use ms_mqtt_client::client::{
    ClientOptions, ConnectResult, ConnectionTransportConfig, ConnectionTransportType,
    DisconnectedEvent, KeepAliveConfig, new_client,
};
use ms_mqtt_client::error::ConnectError;
use ms_mqtt_client::packet::ConnectProperties;

use crate::common::fixtures::{FixtureCapability, FixtureQuirk};
use crate::common::{
    MTLS_PORT, TCP_PORT, TLS_PORT, acquire_fixture_guard_if_necessary, connect_with_transport, empty_tls_config,
    mutual_tls_config, port_from_env, tls_config,
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

/// Verifies that a client certificate accepted by the mTLS fixture can connect and disconnect
/// cleanly.
#[tokio::test]
async fn mutual_tls_connect_disconnect() {
    crate::require_fixture_capability!(FixtureCapability::MutualTls);
    let _guard = acquire_fixture_guard_if_necessary().await;
    let connection = connect_with_transport(
        ConnectionTransportType::Tls {
            hostname: "localhost".to_string(),
            port: port_from_env("MQTT_MTLS_PORT", MTLS_PORT),
            config: mutual_tls_config(),
        },
        "transport_mutual_tls",
        KeepAliveConfig::Infinite,
    )
    .await
    .start();
    assert!(matches!(
        connection.disconnect().await,
        DisconnectedEvent::ApplicationDisconnect
    ));
}

/// Verifies that TLS connection establishment rejects a server certificate whose issuer is not
/// trusted by the client.
#[tokio::test]
async fn tls_rejects_untrusted_server_certificate() {
    crate::skip_for_fixture_quirk!(FixtureQuirk::FailedTlsHandshakeDestabilizesServer);
    let _guard = acquire_fixture_guard_if_necessary().await;
    let error = connect_and_expect_failure(
        ConnectionTransportType::Tls {
            hostname: "localhost".to_string(),
            port: port_from_env("MQTT_TLS_PORT", TLS_PORT),
            config: empty_tls_config(),
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
    crate::skip_for_fixture_quirk!(FixtureQuirk::FailedTlsHandshakeDestabilizesServer);
    let _guard = acquire_fixture_guard_if_necessary().await;
    let error = connect_and_expect_failure(
        ConnectionTransportType::Tls {
            hostname: "127.0.0.1".to_string(),
            port: port_from_env("MQTT_TLS_PORT", TLS_PORT),
            config: tls_config(),
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
    let _guard = acquire_fixture_guard_if_necessary().await;
    let error = connect_and_expect_failure(
        ConnectionTransportType::Tls {
            hostname: "localhost".to_string(),
            port: port_from_env("MQTT_MTLS_PORT", MTLS_PORT),
            config: tls_config(),
        },
        "transport_mutual_tls_missing_identity",
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
    let _guard = acquire_fixture_guard_if_necessary().await;
    let error = connect_and_expect_failure(
        ConnectionTransportType::Ws {
            request: format!(
                "ws://localhost:{}/mqtt",
                port_from_env("MQTT_PORT", TCP_PORT)
            )
            .into_client_request()
            .expect("WebSocket URL should be valid"),
            tls_config: empty_tls_config(),
        },
        "transport_websocket_wrong_endpoint",
    )
    .await;
    assert!(
        matches!(error, ConnectError::Io(_)),
        "unexpected error: {error}"
    );
}
