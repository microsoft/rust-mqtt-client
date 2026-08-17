// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::num::NonZeroU16;
use std::pin::pin;
use std::time::Duration;

use bytes::Bytes;
use matches::assert_matches;
use ms_mqtt_client::client::token::completion::CompletionError;
use ms_mqtt_client::client::{
    ClientOptions, ConnectEnhancedAuthResult, ConnectResult, DisconnectedEvent, KeepAliveConfig,
    new_client,
};
use ms_mqtt_client::mqtt_proto::{
    self, AuthenticateReasonCode, Authentication, ConnectReasonCode, Packet, PacketIdentifierDupQoS,
};
use ms_mqtt_client::packet::{
    AuthenticationInfo, ConnectProperties, QoS, RetainOptions, SessionExpiryInterval,
    SubscribeProperties,
};
use ms_mqtt_client::topic::{TopicFilter, TopicName};
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

const SESSION_EXPIRY: u32 = 60;

fn connect_properties() -> ConnectProperties {
    ConnectProperties {
        session_expiry_interval: SessionExpiryInterval::Duration(SESSION_EXPIRY),
        ..Default::default()
    }
}

enum ConnectKind {
    Standard,
    EnhancedAuth,
}

fn connack(session_present: bool) -> Packet<Bytes> {
    Packet::ConnAck(mqtt_proto::ConnAck {
        reason_code: ConnectReasonCode::Success { session_present },
        other_properties: Default::default(),
    })
}

async fn expect_connect_with_session_expiry(
    outgoing_packets_rx: &mut UnboundedReceiver<Packet<Bytes>>,
) {
    assert_matches!(
        outgoing_packets_rx.recv().await,
        Some(Packet::Connect(mqtt_proto::Connect {
            other_properties: mqtt_proto::ConnectOtherProperties {
                session_expiry_interval: SessionExpiryInterval::Duration(SESSION_EXPIRY),
                ..
            },
            ..
        }))
    );
}

async fn assert_omitted_connack_session_expiry_uses_connect_value(connect_kind: ConnectKind) {
    let options = ClientOptions {
        client_id: Some("omitted-connack-session-expiry".to_string()),
        ..Default::default()
    };
    let (client, connect_handle, _receiver) = new_client(options);

    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();
    let transport = ConnectionTransportConfig {
        transport_type: ConnectionTransportType::Test {
            incoming_packets: incoming_packets_rx,
            outgoing_packets: outgoing_packets_tx,
        },
        timeout: None,
        proxy: None,
        tcp_nodelay: false,
    };

    let connection = match connect_kind {
        ConnectKind::Standard => {
            incoming_packets_tx.send(connack(false)).unwrap();
            let ConnectResult::Success(connection, _, _) = connect_handle
                .connect(
                    transport,
                    true,
                    KeepAliveConfig::Infinite,
                    None,
                    None,
                    None,
                    connect_properties(),
                    None,
                )
                .await
            else {
                panic!("expected successful connect");
            };
            expect_connect_with_session_expiry(&mut outgoing_packets_rx).await;
            connection
        }
        ConnectKind::EnhancedAuth => {
            incoming_packets_tx
                .send(Packet::Auth(mqtt_proto::Auth {
                    reason_code: AuthenticateReasonCode::ContinueAuthentication,
                    authentication: Some(Authentication {
                        method: "test method".into(),
                        data: Some(b"server challenge".into()),
                    }),
                    reason_string: None,
                    user_properties: Default::default(),
                }))
                .unwrap();
            let ConnectEnhancedAuthResult::Continue(_, auth_handle) = connect_handle
                .connect_enhanced_auth(
                    transport,
                    true,
                    KeepAliveConfig::Infinite,
                    None,
                    None,
                    None,
                    connect_properties(),
                    AuthenticationInfo {
                        method: "test method".to_string(),
                        data: Some(Bytes::from_static(b"client initial response")),
                    },
                    None,
                )
                .await
            else {
                panic!("expected enhanced authentication challenge");
            };
            expect_connect_with_session_expiry(&mut outgoing_packets_rx).await;

            incoming_packets_tx.send(connack(false)).unwrap();
            let ConnectEnhancedAuthResult::Success(connection, _, _, _) = auth_handle
                .continue_auth(
                    Some(Bytes::from_static(b"client challenge response")),
                    Default::default(),
                    None,
                )
                .await
            else {
                panic!("expected successful enhanced-auth connect");
            };
            assert_matches!(
                outgoing_packets_rx.recv().await,
                Some(Packet::Auth(mqtt_proto::Auth { .. }))
            );
            connection
        }
    };

    let mut connection = pin!(connection.run_until_disconnect());
    let _publish_ct = client
        .publish_qos1(
            TopicName::new("connect/session-expiry").unwrap(),
            Bytes::from_static(b"pending"),
            false,
            Default::default(),
        )
        .await
        .unwrap();

    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let Some(Packet::Publish(mqtt_proto::Publish {
        packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, false),
        ..
    })) = outgoing_packets_rx.recv().await
    else {
        panic!("expected outbound QoS 1 PUBLISH");
    };

    drop(incoming_packets_tx);
    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::IoError(_));

    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();
    incoming_packets_tx.send(connack(true)).unwrap();

    let ConnectResult::Success(connection, _, _) = connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Test {
                    incoming_packets: incoming_packets_rx,
                    outgoing_packets: outgoing_packets_tx,
                },
                timeout: None,
                proxy: None,
                tcp_nodelay: false,
            },
            false,
            KeepAliveConfig::Infinite,
            None,
            None,
            None,
            connect_properties(),
            None,
        )
        .await
    else {
        panic!("expected successful reconnect");
    };

    assert_matches!(
        outgoing_packets_rx.recv().await,
        Some(Packet::Connect(mqtt_proto::Connect { .. }))
    );

    let mut connection = pin!(connection.run_until_disconnect());
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().await,
        Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                replayed_packet_identifier,
                true
            ),
            ..
        })) if replayed_packet_identifier == packet_identifier
    );

    drop(incoming_packets_tx);
    let (_, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::IoError(_));
}

#[tokio::test(start_paused = true)]
async fn omitted_connack_session_expiry_uses_connect_value() {
    Box::pin(assert_omitted_connack_session_expiry_uses_connect_value(
        ConnectKind::Standard,
    ))
    .await;
}

#[tokio::test(start_paused = true)]
async fn enhanced_auth_omitted_connack_session_expiry_uses_connect_value() {
    Box::pin(assert_omitted_connack_session_expiry_uses_connect_value(
        ConnectKind::EnhancedAuth,
    ))
    .await;
}

#[tokio::test(start_paused = true)]
async fn ping_timeout_preserves_session_for_reconnect() {
    let options = ClientOptions {
        client_id: Some("ping-timeout-session".to_string()),
        ..Default::default()
    };
    let (client, connect_handle, _receiver) = new_client(options);

    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();
    incoming_packets_tx.send(connack(false)).unwrap();

    let ConnectResult::Success(connection, _, _) = connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Test {
                    incoming_packets: incoming_packets_rx,
                    outgoing_packets: outgoing_packets_tx,
                },
                timeout: None,
                proxy: None,
                tcp_nodelay: false,
            },
            true,
            KeepAliveConfig::Duration {
                ping_after: NonZeroU16::new(5).unwrap(),
                response_timeout: Duration::from_secs(2),
            },
            None,
            None,
            None,
            connect_properties(),
            None,
        )
        .await
    else {
        panic!("expected successful connect");
    };
    expect_connect_with_session_expiry(&mut outgoing_packets_rx).await;

    let mut connection = pin!(connection.run_until_disconnect());
    let _publish_ct = client
        .publish_qos1(
            TopicName::new("connect/ping-timeout").unwrap(),
            Bytes::from_static(b"pending"),
            false,
            Default::default(),
        )
        .await
        .unwrap();

    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let Some(Packet::Publish(mqtt_proto::Publish {
        packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, false),
        ..
    })) = outgoing_packets_rx.recv().await
    else {
        panic!("expected outbound QoS 1 PUBLISH");
    };

    let subscribe_ct = client
        .subscribe(
            TopicFilter::new("connect/ping-timeout-subscription").unwrap(),
            QoS::AtLeastOnce,
            false,
            RetainOptions::default(),
            SubscribeProperties::default(),
        )
        .await
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().await, Some(Packet::Subscribe(_)));

    assert_matches!(
        tokio::time::timeout(Duration::from_secs(5), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().await,
        Some(Packet::PingReq(mqtt_proto::PingReq))
    );

    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::PingTimeout);
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), subscribe_ct).await,
        Ok(Err(CompletionError::Canceled(_)))
    );

    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();
    incoming_packets_tx.send(connack(true)).unwrap();

    let ConnectResult::Success(connection, _, _) = connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Test {
                    incoming_packets: incoming_packets_rx,
                    outgoing_packets: outgoing_packets_tx,
                },
                timeout: None,
                proxy: None,
                tcp_nodelay: false,
            },
            false,
            KeepAliveConfig::Infinite,
            None,
            None,
            None,
            connect_properties(),
            None,
        )
        .await
    else {
        panic!("expected successful reconnect");
    };
    expect_connect_with_session_expiry(&mut outgoing_packets_rx).await;

    let mut connection = pin!(connection.run_until_disconnect());
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().await,
        Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                replayed_packet_identifier,
                true
            ),
            ..
        })) if replayed_packet_identifier == packet_identifier
    );

    drop(incoming_packets_tx);
    let (_, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::IoError(_));
}
