// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::pin::pin;
use std::time::Duration;

use azure_mqtt::client::{
    ClientOptions, ConnectResult, ConnectionTransportConfig, ConnectionTransportType,
    DisconnectedEvent, KeepAliveConfig, new_client,
};
use azure_mqtt::mqtt_proto::{
    self, ConnectReasonCode, Packet, PacketIdentifier, PacketIdentifierDupQoS, PubAckReasonCode,
};
use azure_mqtt::packet::{ConnAck, ConnectProperties};
use azure_mqtt::topic::TopicName;
use bytes::Bytes;
use futures_util::future::FutureExt;
use matches::assert_matches;
use tokio::sync::mpsc::unbounded_channel;

#[tokio::test(start_paused = true)]
async fn publish() {
    let options = ClientOptions {
        client_id: Some("foo".to_string()),
        ..Default::default()
    };
    let (client, connect_handle, _receiver) = new_client(options);

    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();

    incoming_packets_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success {
                session_present: false,
            },
            other_properties: Default::default(),
        }))
        .unwrap();

    let ConnectResult::Success(connection, connack, _disconnect_handle) = connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Test {
                    incoming_packets: incoming_packets_rx,
                    outgoing_packets: outgoing_packets_tx,
                },
                timeout: None,
            },
            false,
            KeepAliveConfig::Infinite,
            None,
            None,
            None,
            ConnectProperties::default(),
            None,
        )
        .await
    else {
        panic!("expected successful connect")
    };
    let mut connection = pin!(connection.run_until_disconnect());

    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let server_connect = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(server_connect, Packet::Connect(mqtt_proto::Connect { .. }));
    assert_matches!(connack, ConnAck { .. });

    // Send three PUBLISHes to the server.
    client
        .publish_qos1(
            TopicName::new("foo").unwrap(),
            Bytes::from_static(b"payload1"),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    client
        .publish_qos1(
            TopicName::new("foo").unwrap(),
            Bytes::from_static(b"payload2"),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    client
        .publish_qos1(
            TopicName::new("foo").unwrap(),
            Bytes::from_static(b"payload3"),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, false),
            payload,
            ..
        }))) if packet_identifier.get() == 1 && &*payload == b"payload1"
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, false),
            payload,
            ..
        }))) if packet_identifier.get() == 2 && &*payload == b"payload2"
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, false),
            payload,
            ..
        }))) if packet_identifier.get() == 3 && &*payload == b"payload3"
    );

    // Server acks the first PUBLISH.
    incoming_packets_tx
        .send(Packet::PubAck(mqtt_proto::PubAck {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubAckReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();

    // Server EOF
    drop(incoming_packets_tx);

    let (connect_handle, disconnected_event) = connection.await;
    assert_matches!(disconnected_event, DisconnectedEvent::IoError(_));

    // Reconnect.
    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();

    // Server sends CONNACK with session present = true.
    incoming_packets_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success {
                session_present: true,
            },
            other_properties: Default::default(),
        }))
        .unwrap();

    let ConnectResult::Success(connection, connack, _disconnect_handle) = connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Test {
                    incoming_packets: incoming_packets_rx,
                    outgoing_packets: outgoing_packets_tx,
                },
                timeout: None,
            },
            false,
            KeepAliveConfig::Infinite,
            None,
            None,
            None,
            ConnectProperties::default(),
            None,
        )
        .await
    else {
        panic!("expected successful connect")
    };
    let mut connection = pin!(connection.run_until_disconnect());

    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let server_connect = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(server_connect, Packet::Connect(mqtt_proto::Connect { .. }));
    assert_matches!(connack, ConnAck { .. });

    // Since session was present, client should resend second and third PUBLISHes with dup=true
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, true),
            payload,
            ..
        }))) if packet_identifier.get() == 2 && &*payload == b"payload2"
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, true),
            payload,
            ..
        }))) if packet_identifier.get() == 3 && &*payload == b"payload3"
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    // Server acks the second PUBLISH.
    incoming_packets_tx
        .send(Packet::PubAck(mqtt_proto::PubAck {
            packet_identifier: PacketIdentifier::new(2).unwrap(),
            reason_code: PubAckReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();

    // Server EOF
    drop(incoming_packets_tx);

    let (connect_handle, disconnected_event) = connection.await;
    assert_matches!(disconnected_event, DisconnectedEvent::IoError(_));

    // Reconnect.
    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();

    // Server sends CONNACK with session present = false.
    incoming_packets_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success {
                session_present: false,
            },
            other_properties: Default::default(),
        }))
        .unwrap();

    let ConnectResult::Success(connection, connack, _disconnect_handle) = connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Test {
                    incoming_packets: incoming_packets_rx,
                    outgoing_packets: outgoing_packets_tx,
                },
                timeout: None,
            },
            false,
            KeepAliveConfig::Infinite,
            None,
            None,
            None,
            ConnectProperties::default(),
            None,
        )
        .await
    else {
        panic!("expected successful connect")
    };
    let mut connection = pin!(connection.run_until_disconnect());

    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let server_connect = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(server_connect, Packet::Connect(mqtt_proto::Connect { .. }));
    assert_matches!(connack, ConnAck { .. });

    // Since session was not present, client should not resend any PUBLISHes
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);
}

#[tokio::test(start_paused = true)]
async fn wait_for_packet_id_available() {
    // Set max packet ID to 1, so that client is expected to send only one PUBLISH at a time before waiting for PUBACK.
    let options = ClientOptions {
        client_id: Some("foo".to_string()),
        max_packet_identifier: PacketIdentifier::new(1).unwrap(),
        ..Default::default()
    };
    let (client, connect_handle, _receiver) = new_client(options);

    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();

    incoming_packets_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success {
                session_present: false,
            },
            other_properties: Default::default(),
        }))
        .unwrap();

    let ConnectResult::Success(connection, connack, _disconnect_handle) = connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Test {
                    incoming_packets: incoming_packets_rx,
                    outgoing_packets: outgoing_packets_tx,
                },
                timeout: None,
            },
            false,
            KeepAliveConfig::Infinite,
            None,
            None,
            None,
            ConnectProperties::default(),
            None,
        )
        .await
    else {
        panic!("expected successful connect")
    };
    let mut connection = pin!(connection.run_until_disconnect());

    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let server_connect = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(server_connect, Packet::Connect(mqtt_proto::Connect { .. }));
    assert_matches!(connack, ConnAck { .. });

    // Send three PUBLISHes to the server.
    client
        .publish_qos1(
            TopicName::new("foo").unwrap(),
            Bytes::from_static(b"payload1"),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    client
        .publish_qos1(
            TopicName::new("foo").unwrap(),
            Bytes::from_static(b"payload2"),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    client
        .publish_qos1(
            TopicName::new("foo").unwrap(),
            Bytes::from_static(b"payload3"),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    // Server only receives one of them because of max packet ID in ClientOptions.
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, false),
            payload,
            ..
        }))) if packet_identifier.get() == 1 && &*payload == b"payload1"
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    // Server acks the first PUBLISH...
    incoming_packets_tx
        .send(Packet::PubAck(mqtt_proto::PubAck {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubAckReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    // ... and receives the second PUBLISH.
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, false),
            payload,
            ..
        }))) if packet_identifier.get() == 1 && &*payload == b"payload2"
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    // Server EOF
    drop(incoming_packets_tx);

    let (connect_handle, disconnected_event) = connection.await;
    assert_matches!(disconnected_event, DisconnectedEvent::IoError(_));

    // Reconnect.
    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();

    // Server sends CONNACK with session present = true.
    incoming_packets_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success {
                session_present: true,
            },
            other_properties: Default::default(),
        }))
        .unwrap();

    let ConnectResult::Success(connection, connack, _disconnect_handle) = connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Test {
                    incoming_packets: incoming_packets_rx,
                    outgoing_packets: outgoing_packets_tx,
                },
                timeout: None,
            },
            false,
            KeepAliveConfig::Infinite,
            None,
            None,
            None,
            ConnectProperties::default(),
            None,
        )
        .await
    else {
        panic!("expected successful connect")
    };
    let mut connection = pin!(connection.run_until_disconnect());

    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let server_connect = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(server_connect, Packet::Connect(mqtt_proto::Connect { .. }));
    assert_matches!(connack, ConnAck { .. });

    // Since session was present, client should resend second PUBLISH with dup=true
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, true),
            payload,
            ..
        }))) if packet_identifier.get() == 1 && &*payload == b"payload2"
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    // Server acks the second PUBLISH...
    incoming_packets_tx
        .send(Packet::PubAck(mqtt_proto::PubAck {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubAckReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    // ... and receives the third PUBLISH. It has dup=false because client has not attempted to send it until now.
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, false),
            payload,
            ..
        }))) if packet_identifier.get() == 1 && &*payload == b"payload3"
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    // Server EOF
    drop(incoming_packets_tx);

    let (connect_handle, disconnected_event) = connection.await;
    assert_matches!(disconnected_event, DisconnectedEvent::IoError(_));

    // Reconnect.
    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();

    // Server sends CONNACK with session present = false.
    incoming_packets_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success {
                session_present: false,
            },
            other_properties: Default::default(),
        }))
        .unwrap();

    let ConnectResult::Success(connection, connack, _disconnect_handle) = connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Test {
                    incoming_packets: incoming_packets_rx,
                    outgoing_packets: outgoing_packets_tx,
                },
                timeout: None,
            },
            false,
            KeepAliveConfig::Infinite,
            None,
            None,
            None,
            ConnectProperties::default(),
            None,
        )
        .await
    else {
        panic!("expected successful connect")
    };
    let mut connection = pin!(connection.run_until_disconnect());

    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let server_connect = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(server_connect, Packet::Connect(mqtt_proto::Connect { .. }));
    assert_matches!(connack, ConnAck { .. });

    // Since session was not present, client should not resend any PUBLISHes
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);
}
