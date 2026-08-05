// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Shared helpers for the broker-agnostic live network suite.
//!
//! These live in the main package because they need nothing beyond the crate's existing
//! dev-dependencies. Cargo does not allow optional dev-dependencies, so the first broker
//! that needs one of its own is the signal to move this suite into a detached crate.

#![allow(unused)] // Not every test uses every helper.

pub(crate) mod capabilities;

use std::future::Future;
use std::time::Duration;

use ms_mqtt_client::client::{
    Client, ClientOptions, ConnectResult, Connection, ConnectionTransportConfig,
    ConnectionTransportType, DisconnectHandle, DisconnectedEvent, KeepAliveConfig, Receiver,
    new_client,
};
use ms_mqtt_client::packet::{ConnAck, ConnectProperties, DisconnectProperties};

/// Bounds each test so an unreachable broker fails fast rather than hanging the suite.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Where a suite should look for its broker.
pub(crate) struct Endpoint {
    pub(crate) hostname: String,
    pub(crate) port: u16,
}

impl Endpoint {
    /// Reads `MQTT_HOST`/`MQTT_PORT`, which is what selects the broker under test.
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
    runner: tokio::task::JoinHandle<DisconnectedEvent>,
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
        let runner = tokio::spawn(async move {
            let (_connect_handle, event) = connection.run_until_disconnect().await;
            event
        });
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
        self.disconnect_handle
            .disconnect(&DisconnectProperties::default())
            .expect("connection should still be running");
        self.runner
            .await
            .expect("connection runner should not panic")
    }
}

pub(crate) async fn with_timeout<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(TEST_TIMEOUT, f)
        .await
        .expect("timed out waiting on the broker -- is one running?")
}

/// Performs a plaintext TCP CONNECT, panicking with the broker's failure reason.
///
/// `client_id` must be unique per test: tests run concurrently, and a broker is required to
/// evict the existing session when a second connection reuses its identifier.
pub(crate) async fn connect_tcp(endpoint: &Endpoint, client_id: &str) -> LiveConnection {
    let options = ClientOptions {
        client_id: Some(client_id.to_string()),
        ..Default::default()
    };
    let (client, connect_handle, receiver) = new_client(options);

    match connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Tcp {
                    hostname: endpoint.hostname.clone(),
                    port: endpoint.port,
                },
                timeout: Some(RESPONSE_TIMEOUT),
            },
            true,
            KeepAliveConfig::Infinite,
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
        ConnectResult::Failure(_, err) => panic!(
            "CONNECT to {}:{} failed: {err}",
            endpoint.hostname, endpoint.port
        ),
    }
}
