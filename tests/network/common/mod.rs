// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Shared helpers for the server-agnostic live network suite.
//!
//! These live in the main package because they need nothing beyond the crate's existing
//! dev-dependencies. Cargo does not allow optional dev-dependencies, so the first server
//! that needs one of its own is the signal to move this suite into a detached crate.

#![allow(unused)] // Not every test uses every helper.

pub(crate) mod fixture;
pub(crate) mod server;

use std::time::Duration;

use ms_mqtt_client::client::{
    Client, ClientOptions, ConnectHandle, ConnectResult, ConnectionTransportConfig,
    ConnectionTransportTlsConfig, ConnectionTransportType, DisconnectHandle, DisconnectedEvent,
    KeepAliveConfig, Receiver, new_client,
};
use ms_mqtt_client::packet::{ConnAck, ConnectProperties, DisconnectProperties, Will};

pub(crate) const ENV_MQTT_CERT_DIR: &str = "MQTT_CERT_DIR";
pub(crate) const ENV_MQTT_HOST: &str = "MQTT_HOST";
pub(crate) const ENV_MQTT_MTLS_PORT: &str = "MQTT_MTLS_PORT";
pub(crate) const ENV_MQTT_PORT: &str = "MQTT_PORT";
pub(crate) const ENV_MQTT_SERVER: &str = "MQTT_SERVER";
pub(crate) const ENV_MQTT_TLS_PORT: &str = "MQTT_TLS_PORT";
pub(crate) const ENV_MQTT_WS_PORT: &str = "MQTT_WS_PORT";
pub(crate) const ENV_MQTT_WSS_PORT: &str = "MQTT_WSS_PORT";

pub(crate) const TCP_PORT: u16 = 1883;
pub(crate) const TLS_PORT: u16 = 8883;
pub(crate) const MTLS_PORT: u16 = 8884;
pub(crate) const WS_PORT: u16 = 8083;
pub(crate) const WSS_PORT: u16 = 8084;

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
    let directory = std::env::var(ENV_MQTT_CERT_DIR)
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
    mutual_tls_config_with_identity("client.crt", "client.key")
}

pub(crate) fn mutual_tls_config_with_untrusted_client() -> ConnectionTransportTlsConfig {
    mutual_tls_config_with_identity("untrusted-client.crt", "untrusted-client.key")
}

pub(crate) fn mutual_tls_config_with_server_only_certificate() -> ConnectionTransportTlsConfig {
    mutual_tls_config_with_identity("server.crt", "server.key")
}

fn mutual_tls_config_with_identity(
    certificate_name: &str,
    key_name: &str,
) -> ConnectionTransportTlsConfig {
    ConnectionTransportTlsConfig::from_pem(
        Some((&certificate(certificate_name), &certificate(key_name))),
        &certificate("ca.crt"),
    )
    .expect("test client identity and CA certificate should be valid")
}

/// Where a suite should look for its MQTT server.
pub(crate) struct Endpoint {
    pub(crate) hostname: String,
    pub(crate) port: u16,
}

impl Endpoint {
    /// Reads `MQTT_HOST`/`MQTT_PORT`, which select the server endpoint under test.
    pub(crate) fn from_env(default_port: u16) -> Self {
        let hostname = std::env::var(ENV_MQTT_HOST).unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = match std::env::var(ENV_MQTT_PORT) {
            Ok(port) => port
                .parse()
                .unwrap_or_else(|_| panic!("{ENV_MQTT_PORT} must be a valid port number")),
            Err(_) => default_port,
        };
        Self { hostname, port }
    }
}

/// A connected test client whose packet loop runs in an abort-on-drop task.
pub(crate) struct TestConnection {
    pub(crate) client: Client,
    pub(crate) receiver: Receiver,
    pub(crate) connack: ConnAck,
    disconnect_handle: DisconnectHandle,
    runner: AbortOnDropTask<(ConnectHandle, DisconnectedEvent)>,
}

pub(crate) struct SessionOptions {
    pub(crate) clean_start: bool,
    pub(crate) properties: ConnectProperties,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            clean_start: true,
            properties: ConnectProperties::default(),
        }
    }
}

struct AbortOnDropTask<T>(Option<tokio::task::JoinHandle<T>>);

impl<T> AbortOnDropTask<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self(Some(handle))
    }

    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        self.0.take().expect("task already joined").await
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
}

impl TestConnection {
    pub(crate) async fn disconnect(self) -> DisconnectedEvent {
        let (_, _, _, event) = self.disconnect_for_reconnect().await;
        event
    }

    pub(crate) async fn disconnect_with_properties(
        self,
        properties: DisconnectProperties,
    ) -> DisconnectedEvent {
        let (_, _, _, event) = self
            .disconnect_for_reconnect_with_properties(properties)
            .await;
        event
    }

    pub(crate) async fn disconnect_for_reconnect(
        self,
    ) -> (Client, Receiver, ConnectHandle, DisconnectedEvent) {
        self.disconnect_for_reconnect_with_properties(DisconnectProperties::default())
            .await
    }

    pub(crate) async fn disconnect_for_reconnect_with_properties(
        self,
        properties: DisconnectProperties,
    ) -> (Client, Receiver, ConnectHandle, DisconnectedEvent) {
        self.disconnect_handle
            .disconnect(&properties)
            .expect("connection should still be running");
        let (connect_handle, event) = self
            .runner
            .join()
            .await
            .expect("connection runner should not panic");
        (self.client, self.receiver, connect_handle, event)
    }
}

/// Performs a plaintext TCP CONNECT, panicking with the server's failure reason.
///
/// `client_id` must be unique per test: tests run concurrently, and a server is required to
/// evict the existing session when a second connection reuses its identifier.
pub(crate) async fn connect_tcp(endpoint: &Endpoint, client_id: &str) -> TestConnection {
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

pub(crate) async fn connect_tcp_with_session(
    endpoint: &Endpoint,
    client_id: &str,
    session: SessionOptions,
) -> TestConnection {
    connect_new_client(
        ConnectionTransportType::Tcp {
            hostname: endpoint.hostname.clone(),
            port: endpoint.port,
        },
        client_id,
        KeepAliveConfig::Infinite,
        None,
        session,
    )
    .await
}

pub(crate) async fn connect_tcp_with_will(
    endpoint: &Endpoint,
    client_id: &str,
    will: Will,
) -> TestConnection {
    connect_new_client(
        ConnectionTransportType::Tcp {
            hostname: endpoint.hostname.clone(),
            port: endpoint.port,
        },
        client_id,
        KeepAliveConfig::Infinite,
        Some(will),
        SessionOptions::default(),
    )
    .await
}

/// Connects with the selected transport and starts a clean MQTT session.
pub(crate) async fn connect_with_transport(
    transport_type: ConnectionTransportType,
    client_id: &str,
    keep_alive: KeepAliveConfig,
) -> TestConnection {
    connect_new_client(
        transport_type,
        client_id,
        keep_alive,
        None,
        SessionOptions::default(),
    )
    .await
}

async fn connect_new_client(
    transport_type: ConnectionTransportType,
    client_id: &str,
    keep_alive: KeepAliveConfig,
    will: Option<Will>,
    session: SessionOptions,
) -> TestConnection {
    let options = ClientOptions {
        client_id: Some(client_id.to_string()),
        ..Default::default()
    };
    let (client, connect_handle, receiver) = new_client(options);

    establish_connection(
        client,
        connect_handle,
        receiver,
        transport_type,
        keep_alive,
        will,
        session,
    )
    .await
}

/// Reconnects an existing client with the selected transport and a clean MQTT session.
pub(crate) async fn reconnect_with_transport(
    client: Client,
    connect_handle: ConnectHandle,
    receiver: Receiver,
    transport_type: ConnectionTransportType,
    keep_alive: KeepAliveConfig,
) -> TestConnection {
    establish_connection(
        client,
        connect_handle,
        receiver,
        transport_type,
        keep_alive,
        None,
        SessionOptions::default(),
    )
    .await
}

pub(crate) async fn reconnect_tcp_with_session(
    client: Client,
    connect_handle: ConnectHandle,
    receiver: Receiver,
    endpoint: &Endpoint,
    session: SessionOptions,
) -> TestConnection {
    establish_connection(
        client,
        connect_handle,
        receiver,
        ConnectionTransportType::Tcp {
            hostname: endpoint.hostname.clone(),
            port: endpoint.port,
        },
        KeepAliveConfig::Infinite,
        None,
        session,
    )
    .await
}

async fn establish_connection(
    client: Client,
    connect_handle: ConnectHandle,
    receiver: Receiver,
    transport_type: ConnectionTransportType,
    keep_alive: KeepAliveConfig,
    will: Option<Will>,
    session: SessionOptions,
) -> TestConnection {
    match connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type,
                timeout: Some(RESPONSE_TIMEOUT),
            },
            session.clean_start,
            keep_alive,
            will,
            None,
            None,
            session.properties,
            Some(RESPONSE_TIMEOUT),
        )
        .await
    {
        ConnectResult::Success(connection, connack, disconnect_handle) => {
            let runner = AbortOnDropTask::new(tokio::spawn(async move {
                connection.run_until_disconnect().await
            }));
            TestConnection {
                client,
                receiver,
                connack,
                disconnect_handle,
                runner,
            }
        }
        ConnectResult::Failure(_, err) => panic!("MQTT CONNECT failed: {err}"),
    }
}
