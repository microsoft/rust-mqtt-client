// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::pin::pin;
use std::time::Duration;

use azure_mqtt::client::token::acknowledgement::PubAckToken;
use azure_mqtt::client::{
    ClientOptions, ConnectResult, ConnectionTransportConfig, ConnectionTransportType,
    DisconnectedEvent, KeepAliveConfig, ManualAcknowledgement, Receiver, new_client,
};
use azure_mqtt::mqtt_proto::{
    self, ConnectReasonCode, Packet, PacketIdentifier, PacketIdentifierDupQoS, PubAckReasonCode,
    topic,
};
use azure_mqtt::packet::{ConnAck, ConnectProperties, PubAckProperties, Publish};
use bytes::Bytes;
use futures_util::future::{Either, FutureExt, select};
use matches::assert_matches;
use tokio::sync::mpsc::unbounded_channel;

#[tokio::test(start_paused = true)]
async fn puback() {
    let options = ClientOptions {
        client_id: Some("foo".to_string()),
        ..Default::default()
    };
    let (_client, connect_handle, mut receiver) = new_client(options);

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

    // Receive three PUBLISHes from the server.
    incoming_packets_tx
        .send(Packet::Publish(mqtt_proto::Publish {
            topic_name: topic("foo"),
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                PacketIdentifier::new(5).unwrap(),
                false,
            ),
            retain: false,
            payload: Bytes::from_static(b"payload1"),
            other_properties: Default::default(),
        }))
        .unwrap();
    incoming_packets_tx
        .send(Packet::Publish(mqtt_proto::Publish {
            topic_name: topic("foo"),
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                PacketIdentifier::new(6).unwrap(),
                false,
            ),
            retain: false,
            payload: Bytes::from_static(b"payload2"),
            other_properties: Default::default(),
        }))
        .unwrap();
    incoming_packets_tx
        .send(Packet::Publish(mqtt_proto::Publish {
            topic_name: topic("foo"),
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                PacketIdentifier::new(7).unwrap(),
                false,
            ),
            retain: false,
            payload: Bytes::from_static(b"payload3"),
            other_properties: Default::default(),
        }))
        .unwrap();
    let (received_publish1, ManualAcknowledgement::QoS1(ack_token1)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("did not receive expected PUBLISH and ack token");
    };
    assert_eq!(received_publish1.payload, Bytes::from_static(b"payload1"));
    let (received_publish2, ManualAcknowledgement::QoS1(ack_token2)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("did not receive expected PUBLISH and ack token");
    };
    assert_eq!(received_publish2.payload, Bytes::from_static(b"payload2"));
    let (received_publish3, ManualAcknowledgement::QoS1(ack_token3)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("did not receive expected PUBLISH and ack token");
    };
    assert_eq!(received_publish3.payload, Bytes::from_static(b"payload3"));

    // Ack the second PUBLISH. There should be no outgoing PUBACK yet,
    // because PUBACKs must be emitted in order of incoming PUBLISHes
    accept_publish(&mut connection, ack_token2, Default::default()).await;
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    // Ack the first PUBLISH. Both PUBACKs must appear in order.
    accept_publish(&mut connection, ack_token1, Default::default()).await;
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubAck(mqtt_proto::PubAck {
            packet_identifier,
            reason_code: PubAckReasonCode::Success,
            ..
        }))) if packet_identifier.get() == 5
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubAck(mqtt_proto::PubAck {
            packet_identifier,
            reason_code: PubAckReasonCode::Success,
            ..
        }))) if packet_identifier.get() == 6
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    // Server EOF
    drop(incoming_packets_tx);

    let (connect_handle, disconnected_event) = connection.await;
    assert_matches!(disconnected_event, DisconnectedEvent::IoError(_));

    // Reconnect.
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

    // Receive fourth PUBLISH from the server and ack it immediately.
    incoming_packets_tx
        .send(Packet::Publish(mqtt_proto::Publish {
            topic_name: topic("foo"),
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                PacketIdentifier::new(8).unwrap(),
                false,
            ),
            retain: false,
            payload: Bytes::from_static(b"payload4"),
            other_properties: Default::default(),
        }))
        .unwrap();
    let (received_publish4, ManualAcknowledgement::QoS1(ack_token4)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("did not receive expected PUBLISH and ack token");
    };
    assert_eq!(received_publish4.payload, Bytes::from_static(b"payload4"));
    accept_publish(&mut connection, ack_token4, Default::default()).await;

    // Ack the third PUBLISH. The PUBACK for this must not be sent because
    // it belongs to the previous connection epoch. Expect to receive the PUBACK for
    // the fourth PUBLISH.
    accept_publish(&mut connection, ack_token3, Default::default()).await;
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubAck(mqtt_proto::PubAck {
            packet_identifier,
            reason_code: PubAckReasonCode::Success,
            ..
        }))) if packet_identifier.get() == 8
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);
}

async fn run_with_connection<F>(connection: impl Future + Unpin, f: F) -> Option<F::Output>
where
    F: Future + Unpin,
{
    match select(f, connection).await {
        Either::Left((result, _)) => Some(result),
        Either::Right(_) => None,
    }
}

async fn receive_publish(
    connection: impl Future + Unpin,
    receiver: &mut Receiver,
) -> (Publish, ManualAcknowledgement) {
    let f = pin!(receiver.recv());
    match run_with_connection(connection, f).await {
        Some(Some((publish, manual_ack))) => (publish, manual_ack),
        _ => panic!("did not receive expected PUBLISH and ack token"),
    }
}

async fn accept_publish(
    connection: impl Future + Unpin,
    ack_token: PubAckToken,
    properties: PubAckProperties,
) {
    let f = pin!(ack_token.accept(properties));
    match run_with_connection(connection, f).await {
        Some(_) => (),
        _ => panic!("did not manage to ack PUBLISH"),
    }
}
