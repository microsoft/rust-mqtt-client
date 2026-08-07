// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Verifies that each transport moves MQTT data correctly in both directions.

use std::num::NonZeroU16;
use std::time::Duration;

use async_tungstenite::tungstenite::client::IntoClientRequest as _;
use bytes::Bytes;
use ms_mqtt_client::client::{
    Client, ConnectionTransportType, DisconnectedEvent, KeepAliveConfig, ManualAcknowledgement,
};
use ms_mqtt_client::packet::{
    PayloadFormatIndicator, PubAckProperties, Publish, PublishProperties, QoS, RetainOptions,
    SubscribeProperties,
};
use ms_mqtt_client::topic::{TopicFilter, TopicName};
use test_case::{test_case, test_matrix};

use crate::common::{
    ENV_MQTT_HOST, ENV_MQTT_PORT, ENV_MQTT_TLS_PORT, ENV_MQTT_WS_PORT, ENV_MQTT_WSS_PORT, TCP_PORT,
    TLS_PORT, TestConnection, WS_PORT, WSS_PORT, acquire_fixture_guard_if_necessary,
    connect_with_transport, empty_tls_config, port_from_env, reconnect_with_transport, tls_config,
};

#[derive(Clone, Copy)]
enum ConnectionProfile {
    Tcp,
    Tls,
    WebSocket,
    SecureWebSocket,
}

#[derive(Clone, Copy)]
enum PublicationShape {
    EmptyPayload,
    OneBytePayload,
    RemainingLength127,
    RemainingLength128,
    RemainingLength16383,
    RemainingLength16384,
    LargePayload,
    LargeProperties,
}

#[derive(Clone, Copy)]
enum BurstSize {
    Short,
    Sustained,
}

impl BurstSize {
    fn name(self) -> &'static str {
        match self {
            Self::Short => "short",
            Self::Sustained => "sustained",
        }
    }

    fn message_count(self) -> u32 {
        match self {
            Self::Short => 32,
            Self::Sustained => 1_000,
        }
    }
}

struct Publication {
    payload: Bytes,
    properties: PublishProperties,
}

impl PublicationShape {
    fn name(self) -> &'static str {
        match self {
            Self::EmptyPayload => "empty_payload",
            Self::OneBytePayload => "one_byte_payload",
            Self::RemainingLength127 => "remaining_length_127",
            Self::RemainingLength128 => "remaining_length_128",
            Self::RemainingLength16383 => "remaining_length_16383",
            Self::RemainingLength16384 => "remaining_length_16384",
            Self::LargePayload => "large_payload",
            Self::LargeProperties => "large_properties",
        }
    }

    fn topic_code(self) -> &'static str {
        match self {
            Self::EmptyPayload => "e0",
            Self::OneBytePayload => "e1",
            Self::RemainingLength127 => "r1",
            Self::RemainingLength128 => "r2",
            Self::RemainingLength16383 => "r3",
            Self::RemainingLength16384 => "r4",
            Self::LargePayload => "lg",
            Self::LargeProperties => "lp",
        }
    }

    fn publication(self, topic_len: usize) -> Publication {
        // MQTT 5 QoS 1 variable header: topic-length prefix + topic + packet ID
        // + zero-length property field.
        let variable_header_len = 2 + topic_len + 2 + 1;
        let payload_len = match self {
            Self::EmptyPayload => 0,
            Self::OneBytePayload | Self::LargeProperties => 1,
            Self::RemainingLength127 => 127 - variable_header_len,
            Self::RemainingLength128 => 128 - variable_header_len,
            Self::RemainingLength16383 => 16_383 - variable_header_len,
            Self::RemainingLength16384 => 16_384 - variable_header_len,
            Self::LargePayload => 256 * 1024,
        };
        let properties = if matches!(self, Self::LargeProperties) {
            PublishProperties {
                payload_format_indicator: PayloadFormatIndicator::UTF8,
                response_topic: Some(
                    TopicName::new("ms-mqtt-client/network/transport-io/response").unwrap(),
                ),
                correlation_data: Some(Bytes::from(vec![0x42; 1024])),
                user_properties: (0..64)
                    .map(|index| (format!("key-{index:02}"), "x".repeat(512)))
                    .collect(),
                content_type: Some("application/octet-stream".to_string()),
                ..Default::default()
            }
        } else {
            PublishProperties::default()
        };
        Publication {
            payload: Bytes::from(vec![0x5a; payload_len]),
            properties,
        }
    }

    fn remaining_length_boundary(self) -> Option<usize> {
        match self {
            Self::RemainingLength127 => Some(127),
            Self::RemainingLength128 => Some(128),
            Self::RemainingLength16383 => Some(16_383),
            Self::RemainingLength16384 => Some(16_384),
            Self::EmptyPayload
            | Self::OneBytePayload
            | Self::LargePayload
            | Self::LargeProperties => None,
        }
    }
}

impl ConnectionProfile {
    fn name(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Tls => "tls",
            Self::WebSocket => "websocket",
            Self::SecureWebSocket => "secure_websocket",
        }
    }

    fn topic_code(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Tls => "tls",
            Self::WebSocket => "wsc",
            Self::SecureWebSocket => "wss",
        }
    }

    fn transport(self) -> ConnectionTransportType {
        let hostname = std::env::var(ENV_MQTT_HOST).unwrap_or_else(|_| "localhost".to_string());
        match self {
            Self::Tcp => ConnectionTransportType::Tcp {
                hostname,
                port: port_from_env(ENV_MQTT_PORT, TCP_PORT),
            },
            Self::Tls => ConnectionTransportType::Tls {
                hostname,
                port: port_from_env(ENV_MQTT_TLS_PORT, TLS_PORT),
                config: tls_config(),
            },
            Self::WebSocket => ConnectionTransportType::Ws {
                request: format!(
                    "ws://{hostname}:{}/mqtt",
                    port_from_env(ENV_MQTT_WS_PORT, WS_PORT)
                )
                .into_client_request()
                .expect("WebSocket URL should be valid"),
                tls_config: empty_tls_config(),
            },
            Self::SecureWebSocket => ConnectionTransportType::Ws {
                request: format!(
                    "wss://{hostname}:{}/mqtt",
                    port_from_env(ENV_MQTT_WSS_PORT, WSS_PORT)
                )
                .into_client_request()
                .expect("secure WebSocket URL should be valid"),
                tls_config: tls_config(),
            },
        }
    }
}

async fn connect(
    profile: ConnectionProfile,
    role: &str,
    keep_alive: KeepAliveConfig,
) -> TestConnection {
    connect_with_transport(
        profile.transport(),
        &format!("transport_io_{}_{}", profile.name(), role),
        keep_alive,
    )
    .await
}

async fn subscribe_and_expect_success(connection: &TestConnection, topic: &str) {
    let suback = connection
        .client
        .subscribe(
            TopicFilter::new(topic).unwrap(),
            QoS::AtLeastOnce,
            false,
            RetainOptions::default(),
            SubscribeProperties::default(),
        )
        .await
        .expect("client should still be attached")
        .await
        .expect("SUBSCRIBE should complete");
    assert!(suback.is_success(), "server rejected SUBSCRIBE: {suback:?}");
}

async fn receive_qos1_and_acknowledge(connection: &mut TestConnection) -> Publish {
    let (publish, acknowledgement) = connection
        .receiver
        .recv()
        .await
        .expect("client should receive the PUBLISH");
    let ManualAcknowledgement::QoS1(acknowledgement) = acknowledgement else {
        panic!("QoS 1 delivery should require a PUBACK");
    };
    acknowledgement
        .accept(PubAckProperties::default())
        .await
        .expect("client should still be attached")
        .await
        .expect("PUBACK should be sent");
    publish
}

async fn publish_qos1_and_expect_success(
    publisher: &Client,
    topic: &str,
    publication: &Publication,
) {
    let puback = publisher
        .publish_qos1(
            TopicName::new(topic).unwrap(),
            publication.payload.clone(),
            false,
            publication.properties.clone(),
        )
        .await
        .expect("publisher should still be attached")
        .await
        .expect("PUBLISH should complete");
    assert!(puback.is_success(), "server rejected PUBLISH: {puback:?}");
}

fn assert_publication_matches(publish: &Publish, topic: &str, publication: &Publication) {
    assert_eq!(publish.topic_name, TopicName::new(topic).unwrap());
    assert_eq!(publish.payload, publication.payload);
    assert_eq!(publish.properties, publication.properties);
}

async fn disconnect_pair_and_expect_application_disconnect(
    first: TestConnection,
    second: TestConnection,
) {
    let (first_event, second_event) = tokio::join!(first.disconnect(), second.disconnect());
    assert!(matches!(
        first_event,
        DisconnectedEvent::ApplicationDisconnect
    ));
    assert!(matches!(
        second_event,
        DisconnectedEvent::ApplicationDisconnect
    ));
}

/// Verifies bidirectional MQTT I/O for each connection-profile and publication-shape combination
/// by transferring and acknowledging a QoS 1 publication, then disconnecting cleanly.
#[test_matrix(
    [
        ConnectionProfile::Tcp,
        ConnectionProfile::Tls,
        ConnectionProfile::WebSocket,
        ConnectionProfile::SecureWebSocket,
    ],
    [
        PublicationShape::EmptyPayload,
        PublicationShape::OneBytePayload,
        PublicationShape::RemainingLength127,
        PublicationShape::RemainingLength128,
        PublicationShape::RemainingLength16383,
        PublicationShape::RemainingLength16384,
        PublicationShape::LargePayload,
        PublicationShape::LargeProperties,
    ]
)]
#[tokio::test]
async fn read_write(profile: ConnectionProfile, publication_shape: PublicationShape) {
    let _guard = acquire_fixture_guard_if_necessary().await;
    crate::test_timeout! {
        let subscriber_role = format!("{}_subscriber", publication_shape.name());
        let publisher_role = format!("{}_publisher", publication_shape.name());
        let mut subscriber =
            connect(profile, &subscriber_role, KeepAliveConfig::Infinite).await;
        let publisher =
            connect(profile, &publisher_role, KeepAliveConfig::Infinite).await;
        let topic = format!(
            "ms-mqtt-client/network/transport-io/{}/{}",
            profile.topic_code(),
            publication_shape.topic_code()
        );

        subscribe_and_expect_success(&subscriber, &topic).await;

        let publication = publication_shape.publication(topic.len());
        if let Some(expected_remaining_length) = publication_shape.remaining_length_boundary() {
            // Boundary shapes have no properties, so the complete Remaining Length is the
            // variable header (topic, packet ID, zero property length) plus payload.
            assert_eq!(
                2 + topic.len() + 2 + 1 + publication.payload.len(),
                expected_remaining_length
            );
        }
        publish_qos1_and_expect_success(&publisher.client, &topic, &publication).await;
        let publish = receive_qos1_and_acknowledge(&mut subscriber).await;
        assert_publication_matches(&publish, &topic, &publication);

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}

/// Verifies repeated transport setup, MQTT I/O, clean shutdown, and reconnection using the same
/// client and returned reconnect handle for the selected connection profile.
#[test_case(ConnectionProfile::Tcp; "tcp")]
#[test_case(ConnectionProfile::Tls; "tls")]
#[test_case(ConnectionProfile::WebSocket; "websocket")]
#[test_case(ConnectionProfile::SecureWebSocket; "secure_websocket")]
#[tokio::test]
async fn reconnect_cycles(profile: ConnectionProfile) {
    const CYCLE_COUNT: u32 = 5;

    let _guard = acquire_fixture_guard_if_necessary().await;
    crate::test_timeout! {
        let mut connection =
            connect(profile, "reconnect", KeepAliveConfig::Infinite).await;

        for cycle in 0..CYCLE_COUNT {
            let topic = format!(
                "ms-mqtt-client/network/transport-io/{}/reconnect/{cycle}",
                profile.topic_code()
            );
            subscribe_and_expect_success(&connection, &topic).await;
            let publication = PublicationShape::OneBytePayload.publication(topic.len());
            let publisher = connection.client.clone();
            publish_qos1_and_expect_success(&publisher, &topic, &publication).await;
            let publish = receive_qos1_and_acknowledge(&mut connection).await;
            assert_publication_matches(&publish, &topic, &publication);

            let (client, receiver, connect_handle, event) =
                connection.disconnect_for_reconnect().await;
            assert!(matches!(event, DisconnectedEvent::ApplicationDisconnect));

            if cycle + 1 == CYCLE_COUNT {
                break;
            }
            connection = reconnect_with_transport(
                client,
                connect_handle,
                receiver,
                profile.transport(),
                KeepAliveConfig::Infinite,
            )
            .await;
        }
    }
}

/// Verifies that one long-lived connection can alternate among every publication shape without
/// retaining stale payload, property, or buffer state from the previous packet.
#[test_case(ConnectionProfile::Tcp; "tcp")]
#[test_case(ConnectionProfile::Tls; "tls")]
#[test_case(ConnectionProfile::WebSocket; "websocket")]
#[test_case(ConnectionProfile::SecureWebSocket; "secure_websocket")]
#[tokio::test]
async fn mixed_publication_shapes(profile: ConnectionProfile) {
    const SHAPES: &[PublicationShape] = &[
        PublicationShape::EmptyPayload,
        PublicationShape::LargeProperties,
        PublicationShape::RemainingLength127,
        PublicationShape::LargePayload,
        PublicationShape::OneBytePayload,
        PublicationShape::RemainingLength16384,
        PublicationShape::RemainingLength128,
        PublicationShape::RemainingLength16383,
    ];

    let _guard = acquire_fixture_guard_if_necessary().await;
    crate::test_timeout! {
        let mut subscriber =
            connect(profile, "mixed_subscriber", KeepAliveConfig::Infinite).await;
        let publisher =
            connect(profile, "mixed_publisher", KeepAliveConfig::Infinite).await;
        let topic = format!(
            "ms-mqtt-client/network/transport-io/{}/mixed",
            profile.topic_code()
        );
        subscribe_and_expect_success(&subscriber, &topic).await;

        for &shape in SHAPES {
            let publication = shape.publication(topic.len());
            publish_qos1_and_expect_success(&publisher.client, &topic, &publication).await;
            let publish = receive_qos1_and_acknowledge(&mut subscriber).await;
            assert_publication_matches(&publish, &topic, &publication);
        }

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}

/// Verifies that short and sustained bursts of small QoS 1 publications are delivered completely
/// and in order for each connection profile.
#[test_matrix(
    [
        ConnectionProfile::Tcp,
        ConnectionProfile::Tls,
        ConnectionProfile::WebSocket,
        ConnectionProfile::SecureWebSocket,
    ],
    [BurstSize::Short, BurstSize::Sustained]
)]
#[tokio::test]
async fn back_to_back_ordering(profile: ConnectionProfile, burst_size: BurstSize) {
    let _guard = acquire_fixture_guard_if_necessary().await;
    crate::test_timeout! {
        let subscriber_role = format!("{}_burst_subscriber", burst_size.name());
        let publisher_role = format!("{}_burst_publisher", burst_size.name());
        let mut subscriber =
            connect(profile, &subscriber_role, KeepAliveConfig::Infinite).await;
        let publisher =
            connect(profile, &publisher_role, KeepAliveConfig::Infinite).await;
        let topic = format!(
            "ms-mqtt-client/network/transport-io/{}/burst/{}",
            profile.topic_code(),
            burst_size.name()
        );
        subscribe_and_expect_success(&subscriber, &topic).await;

        let message_count = burst_size.message_count();
        let mut completion_tokens = Vec::with_capacity(message_count as usize);
        for sequence in 0..message_count {
            // Keep each payload to the four-byte sequence number so this test isolates packet
            // buffering, completeness, and ordering from size/property behavior covered by
            // `read_write`.
            let token = publisher
                .client
                .publish_qos1(
                    TopicName::new(&topic).unwrap(),
                    Bytes::copy_from_slice(&sequence.to_be_bytes()),
                    false,
                    PublishProperties::default(),
                )
                .await
                .expect("publisher should still be attached");
            completion_tokens.push(token);
        }

        for token in completion_tokens {
            let puback = token.await.expect("PUBLISH should complete");
            assert!(puback.is_success(), "server rejected PUBLISH: {puback:?}");
        }

        for expected_sequence in 0..message_count {
            let publish = receive_qos1_and_acknowledge(&mut subscriber).await;
            assert_eq!(publish.payload, expected_sequence.to_be_bytes().as_slice());
        }

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}

/// Verifies simultaneous asymmetric bidirectional QoS 1 traffic: one direction carries a minimal
/// payload while the other carries a small payload with large properties.
#[test_case(ConnectionProfile::Tcp; "tcp")]
#[test_case(ConnectionProfile::Tls; "tls")]
#[test_case(ConnectionProfile::WebSocket; "websocket")]
#[test_case(ConnectionProfile::SecureWebSocket; "secure_websocket")]
#[tokio::test]
async fn concurrent_bidirectional(profile: ConnectionProfile) {
    let _guard = acquire_fixture_guard_if_necessary().await;
    crate::test_timeout! {
        let mut first =
            connect(profile, "concurrent_first", KeepAliveConfig::Infinite).await;
        let mut second =
            connect(profile, "concurrent_second", KeepAliveConfig::Infinite).await;
        let first_topic = format!(
            "ms-mqtt-client/network/transport-io/{}/concurrent/first",
            profile.topic_code()
        );
        let second_topic = format!(
            "ms-mqtt-client/network/transport-io/{}/concurrent/second",
            profile.topic_code()
        );
        subscribe_and_expect_success(&first, &first_topic).await;
        subscribe_and_expect_success(&second, &second_topic).await;

        let first_to_second =
            PublicationShape::OneBytePayload.publication(second_topic.len());
        let second_to_first = PublicationShape::LargeProperties.publication(first_topic.len());

        let (first_token, second_token) = tokio::join!(
            first.client.publish_qos1(
                TopicName::new(&second_topic).unwrap(),
                first_to_second.payload.clone(),
                false,
                first_to_second.properties.clone(),
            ),
            second.client.publish_qos1(
                TopicName::new(&first_topic).unwrap(),
                second_to_first.payload.clone(),
                false,
                second_to_first.properties.clone(),
            ),
        );
        let first_token = first_token.expect("first client should still be attached");
        let second_token = second_token.expect("second client should still be attached");
        let (first_puback, second_puback) = tokio::join!(first_token, second_token);
        assert!(
            first_puback.expect("first PUBLISH should complete").is_success(),
            "server rejected first PUBLISH"
        );
        assert!(
            second_puback
                .expect("second PUBLISH should complete")
                .is_success(),
            "server rejected second PUBLISH"
        );

        let (first_publish, second_publish) = tokio::join!(
            receive_qos1_and_acknowledge(&mut first),
            receive_qos1_and_acknowledge(&mut second),
        );
        assert_publication_matches(&first_publish, &first_topic, &second_to_first);
        assert_publication_matches(&second_publish, &second_topic, &first_to_second);

        disconnect_pair_and_expect_application_disconnect(first, second).await;
    }
}

/// Verifies that an idle connection remains usable after a keepalive interval and PINGREQ/PINGRESP
/// exchange for the selected connection profile.
#[test_case(ConnectionProfile::Tcp; "tcp")]
#[test_case(ConnectionProfile::Tls; "tls")]
#[test_case(ConnectionProfile::WebSocket; "websocket")]
#[test_case(ConnectionProfile::SecureWebSocket; "secure_websocket")]
#[tokio::test]
async fn keepalive(profile: ConnectionProfile) {
    let _guard = acquire_fixture_guard_if_necessary().await;
    crate::test_timeout! {
        let keep_alive = KeepAliveConfig::Duration {
            ping_after: NonZeroU16::new(5).unwrap(),
            response_timeout: Duration::from_secs(2),
        };
        let mut connection = connect(profile, "keepalive", keep_alive).await;
        let topic = format!(
            "ms-mqtt-client/network/transport-io/{}/keepalive",
            profile.name()
        );

        let suback = connection
            .client
            .subscribe(
                TopicFilter::new(&topic).unwrap(),
                QoS::AtLeastOnce,
                false,
                RetainOptions::default(),
                SubscribeProperties::default(),
            )
            .await
            .expect("client should still be attached")
            .await
            .expect("SUBSCRIBE should complete");
        assert!(suback.is_success(), "server rejected SUBSCRIBE: {suback:?}");

        tokio::time::sleep(Duration::from_secs(6)).await;

        let publication = Publication {
            payload: Bytes::from_static(b"after keepalive"),
            properties: PublishProperties::default(),
        };
        publish_qos1_and_expect_success(&connection.client, &topic, &publication).await;
        let publish = receive_qos1_and_acknowledge(&mut connection).await;
        assert_publication_matches(&publish, &topic, &publication);

        assert!(matches!(
            connection.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
    }
}

// TODO: Test abrupt transport closure followed by reconnect once a controllable socket/proxy
// fixture can terminate an individual client connection without stopping the server.
// TODO: Test backpressure and slow reads/writes once a controllable socket/proxy fixture can
// pause traffic deterministically.
// TODO: Test transport half-close behavior once a socket-level fixture can close one direction
// independently.
// TODO: Test WebSocket frame fragmentation once a scripted WebSocket server/proxy can control
// frame boundaries; live servers do not expose that behavior to the test suite.
