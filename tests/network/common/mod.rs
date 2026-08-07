// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Shared helpers for the server-agnostic live network suite.
//!
//! These live in the main package because they need nothing beyond the crate's existing
//! dev-dependencies. Cargo does not allow optional dev-dependencies, so the first server
//! that needs one of its own is the signal to move this suite into a detached crate.

#![allow(unused)] // Not every test uses every helper.

pub(crate) mod capabilities;
pub(crate) mod fixtures;

use std::time::Duration;

use ms_mqtt_client::client::{
    Client, ClientOptions, ConnectHandle, ConnectResult, Connection, ConnectionTransportConfig,
    ConnectionTransportTlsConfig, ConnectionTransportType, DisconnectHandle, DisconnectedEvent,
    KeepAliveConfig, Receiver, new_client,
};
use ms_mqtt_client::packet::{ConnAck, ConnectProperties, DisconnectProperties};

use fixtures::{FixtureQuirk, has_quirk};

pub(crate) const TCP_PORT: u16 = 1883;
pub(crate) const TLS_PORT: u16 = 8883;
pub(crate) const MTLS_PORT: u16 = 8884;
pub(crate) const WS_PORT: u16 = 8083;
pub(crate) const WSS_PORT: u16 = 8084;

static TRANSPORT_TEST_LOCK: futures_util::lock::Mutex<()> = futures_util::lock::Mutex::new(());

/// Default deadline for live network test bodies.
pub(crate) const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Runs a network-test body with the shared timeout and boxes its large future.
/// TODO: make into an attribute proc-macro after sequential test issue is sorted out
#[macro_export]
macro_rules! test_timeout {
    ($($body:tt)*) => {
        tokio::time::timeout(
            $crate::common::TEST_TIMEOUT,
            std::boxed::Box::pin(async move { $($body)* }),
        )
        .await
        .expect("live network test exceeded its timeout")
    };
}

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn port_from_env(name: &str, default: u16) -> u16 {
    std::env::var(name)
        .map(|port| port.parse().expect("transport port must be a valid u16"))
        .unwrap_or(default)
}

fn certificate(name: &str) -> Vec<u8> {
    let directory = std::env::var("MQTT_CERT_DIR")
        .unwrap_or_else(|_| "tests/network/brokers/certs".to_string());
    let path = format!("{directory}/{name}");
    std::fs::read(&path)
        .unwrap_or_else(|err| panic!("failed to read test certificate {path}: {err}"))
}

pub(crate) fn tls_config() -> ConnectionTransportTlsConfig {
    ConnectionTransportTlsConfig::from_pem(None, &certificate("ca.crt"))
        .expect("test CA certificate should be valid")
}

pub(crate) fn empty_tls_config() -> ConnectionTransportTlsConfig {
    ConnectionTransportTlsConfig::from_pem(None, &[])
        .expect("an empty trust bundle should be valid")
}

pub(crate) fn mutual_tls_config() -> ConnectionTransportTlsConfig {
    ConnectionTransportTlsConfig::from_pem(
        Some((&certificate("client.crt"), &certificate("client.key"))),
        &certificate("ca.crt"),
    )
    .expect("test client identity and CA certificate should be valid")
}

pub(crate) async fn acquire_fixture_guard_if_necessary() -> Option<futures_util::lock::MutexGuard<'static, ()>> {
    if has_quirk(FixtureQuirk::RequiresSerialTransportTests) {
        Some(TRANSPORT_TEST_LOCK.lock().await)
    } else {
        None
    }
}

/// Where a suite should look for its MQTT server.
pub(crate) struct Endpoint {
    pub(crate) hostname: String,
    pub(crate) port: u16,
}

impl Endpoint {
    /// Reads `MQTT_HOST`/`MQTT_PORT`, which select the server endpoint under test.
    pub(crate) fn from_env(default_port: u16) -> Self {
        let hostname = std::env::var("MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = match std::env::var("MQTT_PORT") {
            Ok(port) => port.parse().expect("MQTT_PORT must be a valid port number"),
            Err(_) => default_port,
        };
        Self { hostname, port }
    }
}

/// A live session and every handle needed to drive it.
pub(crate) struct LiveConnection {
    pub(crate) client: Client,
    pub(crate) receiver: Receiver,
    pub(crate) connection: Connection,
    pub(crate) connack: ConnAck,
    pub(crate) disconnect_handle: DisconnectHandle,
}

/// A live connection whose packet loop is running in a test task.
pub(crate) struct RunningConnection {
    pub(crate) client: Client,
    pub(crate) receiver: Receiver,
    pub(crate) connack: ConnAck,
    disconnect_handle: DisconnectHandle,
    runner: tokio::task::JoinHandle<(ConnectHandle, DisconnectedEvent)>,
}

impl LiveConnection {
    pub(crate) fn start(self) -> RunningConnection {
        let Self {
            client,
            receiver,
            connection,
            connack,
            disconnect_handle,
        } = self;
        let runner = tokio::spawn(async move { connection.run_until_disconnect().await });
        RunningConnection {
            client,
            receiver,
            connack,
            disconnect_handle,
            runner,
        }
    }
}

impl RunningConnection {
    pub(crate) async fn disconnect(self) -> DisconnectedEvent {
        let (_, _, _, event) = self.disconnect_for_reconnect().await;
        event
    }

    pub(crate) async fn disconnect_for_reconnect(
        self,
    ) -> (Client, Receiver, ConnectHandle, DisconnectedEvent) {
        self.disconnect_handle
            .disconnect(&DisconnectProperties::default())
            .expect("connection should still be running");
        let (connect_handle, event) = self
            .runner
            .await
            .expect("connection runner should not panic");
        (self.client, self.receiver, connect_handle, event)
    }
}

/// Performs a plaintext TCP CONNECT, panicking with the server's failure reason.
///
/// `client_id` must be unique per test: tests run concurrently, and a server is required to
/// evict the existing session when a second connection reuses its identifier.
pub(crate) async fn connect_tcp(endpoint: &Endpoint, client_id: &str) -> LiveConnection {
    connect_with_transport(
        ConnectionTransportType::Tcp {
            hostname: endpoint.hostname.clone(),
            port: endpoint.port,
        },
        client_id,
        KeepAliveConfig::Infinite,
    )
    .await
}

/// Connects with the selected transport and starts a clean MQTT session.
pub(crate) async fn connect_with_transport(
    transport_type: ConnectionTransportType,
    client_id: &str,
    keep_alive: KeepAliveConfig,
) -> LiveConnection {
    let options = ClientOptions {
        client_id: Some(client_id.to_string()),
        ..Default::default()
    };
    let (client, connect_handle, receiver) = new_client(options);

    reconnect_with_transport(client, connect_handle, receiver, transport_type, keep_alive).await
}

/// Reconnects an existing client with the selected transport and a clean MQTT session.
pub(crate) async fn reconnect_with_transport(
    client: Client,
    connect_handle: ConnectHandle,
    receiver: Receiver,
    transport_type: ConnectionTransportType,
    keep_alive: KeepAliveConfig,
) -> LiveConnection {
    match connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type,
                timeout: Some(RESPONSE_TIMEOUT),
            },
            true,
            keep_alive,
            None,
            None,
            None,
            ConnectProperties::default(),
            Some(RESPONSE_TIMEOUT),
        )
        .await
    {
        ConnectResult::Success(connection, connack, disconnect_handle) => LiveConnection {
            client,
            receiver,
            connection,
            connack,
            disconnect_handle,
        },
        ConnectResult::Failure(_, err) => panic!("MQTT CONNECT failed: {err}"),
    }
}
