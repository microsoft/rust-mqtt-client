// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::pin::pin;
use std::time::Duration;

use bytes::Bytes;
use futures_util::FutureExt as _;
use matches::assert_matches;
use ms_mqtt_client::client::token::acknowledgement::{PubCompToken, PubRelToken};
use ms_mqtt_client::client::token::completion::CompletionError;
use ms_mqtt_client::client::{
    Client, ClientOptions, ConnectHandle, ConnectResult, Connection, DisconnectedEvent,
    KeepAliveConfig, ManualAcknowledgement, Receiver, new_client,
};
use ms_mqtt_client::mqtt_proto::{
    self, ConnectReasonCode, Packet, PacketIdentifier, PacketIdentifierDupQoS, PubAckReasonCode,
    PubCompReasonCode, PubRecReasonCode, PubRelReasonCode, topic,
};
use ms_mqtt_client::packet::{
    ConnectProperties, PubCompProperties, PubRecProperties, PubRejectReason, PubRelProperties,
};
use ms_mqtt_client::topic::TopicName;
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

mod common;
use common::receive_publish;

async fn connect() -> (
    Client,
    Receiver,
    Connection,
    UnboundedSender<Packet<Bytes>>,
    UnboundedReceiver<Packet<Bytes>>,
) {
    let options = ClientOptions {
        client_id: Some("qos2-test".to_string()),
        ..Default::default()
    };
    connect_with_options(options).await
}

async fn connect_with_options(
    options: ClientOptions,
) -> (
    Client,
    Receiver,
    Connection,
    UnboundedSender<Packet<Bytes>>,
    UnboundedReceiver<Packet<Bytes>>,
) {
    let (client, connect_handle, receiver) = new_client(options);
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
            ConnectProperties::default(),
            None,
        )
        .await
    else {
        panic!("expected successful connect");
    };

    assert_matches!(
        outgoing_packets_rx.recv().await,
        Some(Packet::Connect(mqtt_proto::Connect { .. }))
    );

    (
        client,
        receiver,
        connection,
        incoming_packets_tx,
        outgoing_packets_rx,
    )
}

async fn reconnect(
    connect_handle: ConnectHandle,
    session_present: bool,
) -> (
    Connection,
    UnboundedSender<Packet<Bytes>>,
    UnboundedReceiver<Packet<Bytes>>,
) {
    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();
    incoming_packets_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success { session_present },
            other_properties: Default::default(),
        }))
        .unwrap();

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
            ConnectProperties::default(),
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
    (connection, incoming_packets_tx, outgoing_packets_rx)
}

fn assert_publish_qos2_output(_: &(ms_mqtt_client::packet::PubRec, Option<PubRelToken>)) {}

fn assert_pubrec_accept_output(_: &(ms_mqtt_client::packet::PubRel, PubCompToken)) {}

#[tokio::test(start_paused = true)]
async fn outbound_qos2_success() {
    let (client, _receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());

    let publish_ct = client
        .publish_qos2(
            TopicName::new("qos2/outbound").unwrap(),
            Bytes::from_static(b"payload"),
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
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, false),
            payload,
            ..
        }))) if packet_identifier.get() == 1 && &*payload == b"payload"
    );

    incoming_packets_tx
        .send(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubRecReasonCode::Success,
            other_properties: mqtt_proto::PubRecOtherProperties {
                reason_string: Some("received".into()),
                user_properties: vec![("phase".into(), "pubrec".into())],
            },
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );

    let output = publish_ct.await.unwrap();
    assert_publish_qos2_output(&output);
    let (pubrec, Some(pubrel_token)) = output else {
        panic!("expected successful PUBREC token");
    };
    assert_eq!(pubrec.properties.reason_string.as_deref(), Some("received"));
    assert_eq!(
        pubrec.properties.user_properties,
        vec![("phase".to_string(), "pubrec".to_string())]
    );

    let pubrel_ct = pubrel_token
        .confirm(PubRelProperties {
            reason_string: Some("release".to_string()),
            user_properties: vec![("phase".to_string(), "pubrel".to_string())],
        })
        .await
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier,
            reason_code: PubRelReasonCode::Success,
            other_properties: mqtt_proto::PubRelOtherProperties {
                reason_string: Some(reason_string),
                user_properties,
            },
        }))) if packet_identifier.get() == 1
            && reason_string == "release"
            && user_properties.len() == 1
    );

    incoming_packets_tx
        .send(Packet::PubComp(mqtt_proto::PubComp {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubCompReasonCode::Success,
            other_properties: mqtt_proto::PubCompOtherProperties {
                reason_string: Some("complete".into()),
                user_properties: vec![("phase".into(), "pubcomp".into())],
            },
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );

    let pubcomp = pubrel_ct.await.unwrap();
    assert_eq!(
        pubcomp.properties.reason_string.as_deref(),
        Some("complete")
    );
    assert_eq!(pubcomp.packet_identifier.get(), 1);
}

#[tokio::test(start_paused = true)]
async fn inbound_qos2_success_and_duplicates() {
    let (_client, mut receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());
    let packet_identifier = PacketIdentifier::new(5).unwrap();
    let publish = mqtt_proto::Publish {
        topic_name: topic("qos2/inbound"),
        packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, false),
        retain: false,
        payload: Bytes::from_static(b"payload"),
        other_properties: Default::default(),
    };

    incoming_packets_tx
        .send(Packet::Publish(publish.clone()))
        .unwrap();
    let (delivered, ManualAcknowledgement::QoS2(pubrec_token)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("expected incoming QoS 2 PUBLISH");
    };
    assert_eq!(delivered.payload, Bytes::from_static(b"payload"));

    let pubrec_ct = pubrec_token
        .accept(PubRecProperties {
            reason_string: Some("received".to_string()),
            user_properties: vec![("phase".to_string(), "pubrec".to_string())],
        })
        .await
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier: outgoing_identifier,
            reason_code: PubRecReasonCode::Success,
            ..
        }))) if outgoing_identifier == packet_identifier
    );

    let mut duplicate = publish;
    duplicate.packet_identifier_dup_qos =
        PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, true);
    incoming_packets_tx
        .send(Packet::Publish(duplicate))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier: outgoing_identifier,
            reason_code: PubRecReasonCode::Success,
            ..
        }))) if outgoing_identifier == packet_identifier
    );
    assert!(receiver.recv().now_or_never().is_none());

    incoming_packets_tx
        .send(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier,
            reason_code: PubRelReasonCode::Success,
            other_properties: mqtt_proto::PubRelOtherProperties {
                reason_string: Some("release".into()),
                user_properties: vec![("phase".into(), "pubrel".into())],
            },
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );

    let output = pubrec_ct.await.unwrap();
    assert_pubrec_accept_output(&output);
    let (pubrel, pubcomp_token) = output;
    assert_eq!(pubrel.properties.reason_string.as_deref(), Some("release"));

    incoming_packets_tx
        .send(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier,
            reason_code: PubRelReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    let pubcomp_ct = pubcomp_token
        .confirm(PubCompProperties {
            reason_string: Some("complete".to_string()),
            user_properties: vec![("phase".to_string(), "pubcomp".to_string())],
        })
        .await
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubComp(mqtt_proto::PubComp {
            packet_identifier: outgoing_identifier,
            reason_code: PubCompReasonCode::Success,
            ..
        }))) if outgoing_identifier == packet_identifier
    );
    pubcomp_ct.await.unwrap();

    incoming_packets_tx
        .send(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier,
            reason_code: PubRelReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubComp(mqtt_proto::PubComp {
            packet_identifier: outgoing_identifier,
            reason_code: PubCompReasonCode::PacketIdentifierNotFound,
            ..
        }))) if outgoing_identifier == packet_identifier
    );
}

#[tokio::test(start_paused = true)]
async fn rejected_pubrec_releases_packet_identifier() {
    let options = ClientOptions {
        client_id: Some("qos2-rejection".to_string()),
        max_packet_identifier: PacketIdentifier::new(1).unwrap(),
        ..Default::default()
    };
    let (client, _receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect_with_options(options).await;
    let mut connection = pin!(connection.run_until_disconnect());

    let rejected_ct = client
        .publish_qos2(
            TopicName::new("qos2/rejected").unwrap(),
            Bytes::from_static(b"rejected"),
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
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, false),
            ..
        }))) if packet_identifier.get() == 1
    );

    incoming_packets_tx
        .send(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubRecReasonCode::NotAuthorized,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let (pubrec, token) = rejected_ct.await.unwrap();
    assert!(!pubrec.is_success());
    assert!(token.is_none());

    client
        .publish_qos2(
            TopicName::new("qos2/reused").unwrap(),
            Bytes::from_static(b"reused"),
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
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, false),
            payload,
            ..
        }))) if packet_identifier.get() == 1 && &*payload == b"reused"
    );
}

#[tokio::test(start_paused = true)]
async fn pubrel_follows_pubrec_receive_order() {
    let (client, _receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());

    let first_ct = client
        .publish_qos2(
            TopicName::new("qos2/first").unwrap(),
            Bytes::from_static(b"first"),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let second_ct = client
        .publish_qos2(
            TopicName::new("qos2/second").unwrap(),
            Bytes::from_static(b"second"),
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
        Some(Some(Packet::Publish(_)))
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(_)))
    );

    for packet_identifier in [2, 1] {
        incoming_packets_tx
            .send(Packet::PubRec(mqtt_proto::PubRec {
                packet_identifier: PacketIdentifier::new(packet_identifier).unwrap(),
                reason_code: PubRecReasonCode::Success,
                other_properties: Default::default(),
            }))
            .unwrap();
    }
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );

    let (_, Some(first_token)) = first_ct.await.unwrap() else {
        panic!("first PUBLISH should be accepted");
    };
    let (_, Some(second_token)) = second_ct.await.unwrap() else {
        panic!("second PUBLISH should be accepted");
    };

    let first_pubrel_ct = first_token.confirm(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    let second_pubrel_ct = second_token.confirm(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRel(mqtt_proto::PubRel { packet_identifier, .. })))
            if packet_identifier.get() == 2
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRel(mqtt_proto::PubRel { packet_identifier, .. })))
            if packet_identifier.get() == 1
    );
    drop(first_pubrel_ct);
    drop(second_pubrel_ct);
}

#[tokio::test(start_paused = true)]
async fn dropping_tokens_uses_default_qos2_progression() {
    let (client, mut receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());

    let publish_ct = client
        .publish_qos2(
            TopicName::new("qos2/drop/outbound").unwrap(),
            Bytes::new(),
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
        Some(Some(Packet::Publish(_)))
    );
    incoming_packets_tx
        .send(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubRecReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let (_, Some(pubrel_token)) = publish_ct.await.unwrap() else {
        panic!("PUBLISH should be accepted");
    };
    drop(pubrel_token);
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier,
            reason_code: PubRelReasonCode::Success,
            other_properties: mqtt_proto::PubRelOtherProperties { reason_string: None, user_properties },
        }))) if packet_identifier.get() == 1 && user_properties.is_empty()
    );

    let incoming_identifier = PacketIdentifier::new(8).unwrap();
    incoming_packets_tx
        .send(Packet::Publish(mqtt_proto::Publish {
            topic_name: topic("qos2/drop/inbound"),
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(
                incoming_identifier,
                false,
            ),
            retain: false,
            payload: Bytes::new(),
            other_properties: Default::default(),
        }))
        .unwrap();
    let (_, ManualAcknowledgement::QoS2(inbound_token)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("expected incoming QoS 2 PUBLISH");
    };
    drop(inbound_token);
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier,
            reason_code: PubRecReasonCode::Success,
            ..
        }))) if packet_identifier == incoming_identifier
    );

    incoming_packets_tx
        .send(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier: incoming_identifier,
            reason_code: PubRelReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubComp(mqtt_proto::PubComp {
            packet_identifier,
            reason_code: PubCompReasonCode::Success,
            ..
        }))) if packet_identifier == incoming_identifier
    );
}

#[tokio::test(start_paused = true)]
async fn unknown_pubrec_uses_terminal_recovery() {
    let (client, _receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());
    let packet_identifier = PacketIdentifier::new(99).unwrap();

    incoming_packets_tx
        .send(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier,
            reason_code: PubRecReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier: outgoing_identifier,
            reason_code: PubRelReasonCode::PacketIdentifierNotFound,
            ..
        }))) if outgoing_identifier == packet_identifier
    );

    incoming_packets_tx
        .send(Packet::PubComp(mqtt_proto::PubComp {
            packet_identifier,
            reason_code: PubCompReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    client
        .publish_qos0(
            TopicName::new("qos2/recovery-complete").unwrap(),
            Bytes::new(),
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
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtMostOnce,
            ..
        })))
    );
}

#[tokio::test(start_paused = true)]
async fn outbound_qos2_recovers_each_session_phase() {
    let (client, _receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());
    let publish_ct = client
        .publish_qos2(
            TopicName::new("qos2/recovery").unwrap(),
            Bytes::from_static(b"recover"),
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
        Some(Some(Packet::Publish(_)))
    );

    drop(incoming_packets_tx);
    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::IoError(_));

    let (reconnected, incoming_packets_tx, mut outgoing_packets_rx) =
        reconnect(connect_handle, true).await;
    let mut connection = pin!(reconnected.run_until_disconnect());
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, true),
            ..
        }))) if packet_identifier.get() == 1
    );

    incoming_packets_tx
        .send(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubRecReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let (_, Some(pubrel_token)) = publish_ct.await.unwrap() else {
        panic!("PUBLISH should be accepted");
    };

    drop(incoming_packets_tx);
    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::IoError(_));

    let (reconnected, incoming_packets_tx, mut outgoing_packets_rx) =
        reconnect(connect_handle, true).await;
    let mut connection = pin!(reconnected.run_until_disconnect());
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    let pubrel_ct = pubrel_token.confirm(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRel(mqtt_proto::PubRel { packet_identifier, .. })))
            if packet_identifier.get() == 1
    );

    drop(incoming_packets_tx);
    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::IoError(_));

    let (reconnected, incoming_packets_tx, mut outgoing_packets_rx) =
        reconnect(connect_handle, true).await;
    let mut connection = pin!(reconnected.run_until_disconnect());
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRel(mqtt_proto::PubRel { packet_identifier, .. })))
            if packet_identifier.get() == 1
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    incoming_packets_tx
        .send(Packet::PubComp(mqtt_proto::PubComp {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubCompReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    pubrel_ct.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn reconnect_preserves_cross_qos_publish_order() {
    let (client, _receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());

    client
        .publish_qos2(
            TopicName::new("qos2/order/first").unwrap(),
            Bytes::from_static(b"qos2-first"),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    client
        .publish_qos1(
            TopicName::new("qos2/order/second").unwrap(),
            Bytes::from_static(b"qos1-second"),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    client
        .publish_qos2(
            TopicName::new("qos2/order/third").unwrap(),
            Bytes::from_static(b"qos2-third"),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    for _ in 0..3 {
        assert_matches!(
            outgoing_packets_rx.recv().now_or_never(),
            Some(Some(Packet::Publish(_)))
        );
    }

    drop(incoming_packets_tx);
    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::IoError(_));
    let (reconnected, _incoming_packets_tx, mut outgoing_packets_rx) =
        reconnect(connect_handle, true).await;
    let mut connection = pin!(reconnected.run_until_disconnect());
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );

    for (qos, expected_payload) in [
        (2, b"qos2-first".as_slice()),
        (1, b"qos1-second".as_slice()),
        (2, b"qos2-third".as_slice()),
    ] {
        let packet = outgoing_packets_rx.recv().now_or_never().flatten().unwrap();
        let Packet::Publish(publish) = packet else {
            panic!("expected replayed PUBLISH");
        };
        assert_eq!(&*publish.payload, expected_payload);
        match publish.packet_identifier_dup_qos {
            PacketIdentifierDupQoS::AtLeastOnce(_, true) => assert_eq!(qos, 1),
            PacketIdentifierDupQoS::ExactlyOnce(_, true) => assert_eq!(qos, 2),
            _ => panic!("expected duplicate QoS 1 or QoS 2 PUBLISH"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn mismatched_pubrec_preserves_qos1_publish_for_replay() {
    let (client, _receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());
    let publish_ct = client
        .publish_qos1(
            TopicName::new("qos2/mismatched/pubrec").unwrap(),
            Bytes::new(),
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
            ..
        }))) if packet_identifier.get() == 1
    );

    incoming_packets_tx
        .send(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubRecReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::ProtocolError(_));

    let (reconnected, incoming_packets_tx, mut outgoing_packets_rx) =
        reconnect(connect_handle, true).await;
    let mut connection = pin!(reconnected.run_until_disconnect());
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, true),
            ..
        }))) if packet_identifier.get() == 1
    );

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
    publish_ct.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn mismatched_puback_preserves_qos2_publish_for_replay() {
    let (client, _receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());
    let publish_ct = client
        .publish_qos2(
            TopicName::new("qos2/mismatched/puback").unwrap(),
            Bytes::new(),
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
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, false),
            ..
        }))) if packet_identifier.get() == 1
    );

    incoming_packets_tx
        .send(Packet::PubAck(mqtt_proto::PubAck {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubAckReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::ProtocolError(_));

    let (reconnected, incoming_packets_tx, mut outgoing_packets_rx) =
        reconnect(connect_handle, true).await;
    let mut connection = pin!(reconnected.run_until_disconnect());
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::Publish(mqtt_proto::Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, true),
            ..
        }))) if packet_identifier.get() == 1
    );

    incoming_packets_tx
        .send(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubRecReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let (_, Some(pubrel_token)) = publish_ct.await.unwrap() else {
        panic!("PUBLISH should be accepted");
    };
    let pubrel_ct = pubrel_token.confirm(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRel(mqtt_proto::PubRel { packet_identifier, .. })))
            if packet_identifier.get() == 1
    );
    incoming_packets_tx
        .send(Packet::PubComp(mqtt_proto::PubComp {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubCompReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    pubrel_ct.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn session_expiry_invalidates_qos2_token_and_releases_identifier() {
    let options = ClientOptions {
        client_id: Some("qos2-expiry".to_string()),
        max_packet_identifier: PacketIdentifier::new(1).unwrap(),
        ..Default::default()
    };
    let (client, _receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect_with_options(options).await;
    let mut connection = pin!(connection.run_until_disconnect());
    let publish_ct = client
        .publish_qos2(
            TopicName::new("qos2/expiry").unwrap(),
            Bytes::new(),
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
        Some(Some(Packet::Publish(_)))
    );
    incoming_packets_tx
        .send(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubRecReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let (_, Some(pubrel_token)) = publish_ct.await.unwrap() else {
        panic!("PUBLISH should be accepted");
    };

    drop(incoming_packets_tx);
    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::IoError(_));
    let (reconnected, _incoming_packets_tx, mut outgoing_packets_rx) =
        reconnect(connect_handle, false).await;
    let mut connection = pin!(reconnected.run_until_disconnect());

    let stale_ct = pubrel_token.confirm(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);
    assert_matches!(stale_ct.await, Err(CompletionError::Canceled(_)));

    client
        .publish_qos2(
            TopicName::new("qos2/expiry/reused").unwrap(),
            Bytes::new(),
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
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, false),
            ..
        }))) if packet_identifier.get() == 1
    );
}

#[tokio::test(start_paused = true)]
async fn session_expiry_invalidates_inbound_qos2_tokens() {
    let (_client, mut receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());

    incoming_packets_tx
        .send(Packet::Publish(mqtt_proto::Publish {
            topic_name: topic("qos2/expiry/pubcomp"),
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(
                PacketIdentifier::new(41).unwrap(),
                false,
            ),
            retain: false,
            payload: Bytes::new(),
            other_properties: Default::default(),
        }))
        .unwrap();
    let (_, ManualAcknowledgement::QoS2(pubrec_token)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("expected incoming QoS 2 PUBLISH");
    };
    let ct = pubrec_token.accept(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRec(_)))
    );
    incoming_packets_tx
        .send(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier: PacketIdentifier::new(41).unwrap(),
            reason_code: PubRelReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let (_, pubcomp_token) = ct.await.unwrap();

    incoming_packets_tx
        .send(Packet::Publish(mqtt_proto::Publish {
            topic_name: topic("qos2/expiry/pubrec"),
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(
                PacketIdentifier::new(42).unwrap(),
                false,
            ),
            retain: false,
            payload: Bytes::new(),
            other_properties: Default::default(),
        }))
        .unwrap();
    let (_, ManualAcknowledgement::QoS2(pubrec_token)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("expected incoming QoS 2 PUBLISH");
    };

    drop(incoming_packets_tx);
    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::IoError(_));
    let (reconnected, _incoming_packets_tx, mut outgoing_packets_rx) =
        reconnect(connect_handle, false).await;
    let mut connection = pin!(reconnected.run_until_disconnect());

    let ct = pubrec_token.accept(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);
    assert_matches!(ct.await, Err(CompletionError::Canceled(_)));

    let ct = pubcomp_token.confirm(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);
    assert_matches!(ct.await, Err(CompletionError::Canceled(_)));
}

#[tokio::test(start_paused = true)]
async fn inbound_qos2_rejection_finishes_receiver_state() {
    let (_client, mut receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());
    let packet_identifier = PacketIdentifier::new(11).unwrap();
    incoming_packets_tx
        .send(Packet::Publish(mqtt_proto::Publish {
            topic_name: topic("qos2/reject"),
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(
                packet_identifier,
                false,
            ),
            retain: false,
            payload: Bytes::new(),
            other_properties: Default::default(),
        }))
        .unwrap();
    let (_, ManualAcknowledgement::QoS2(pubrec_token)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("expected incoming QoS 2 PUBLISH");
    };
    let reject_ct = pubrec_token
        .reject(
            PubRejectReason::NotAuthorized,
            PubRecProperties {
                reason_string: Some("rejected".to_string()),
                user_properties: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier: outgoing_identifier,
            reason_code: PubRecReasonCode::NotAuthorized,
            ..
        }))) if outgoing_identifier == packet_identifier
    );
    reject_ct.await.unwrap();

    incoming_packets_tx
        .send(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier,
            reason_code: PubRelReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubComp(mqtt_proto::PubComp {
            packet_identifier: outgoing_identifier,
            reason_code: PubCompReasonCode::PacketIdentifierNotFound,
            ..
        }))) if outgoing_identifier == packet_identifier
    );
}

#[tokio::test(start_paused = true)]
async fn inbound_pubrec_follows_publish_order() {
    let (_client, mut receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());

    for packet_identifier in [21, 22] {
        incoming_packets_tx
            .send(Packet::Publish(mqtt_proto::Publish {
                topic_name: topic("qos2/ordered-inbound"),
                packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(
                    PacketIdentifier::new(packet_identifier).unwrap(),
                    false,
                ),
                retain: false,
                payload: Bytes::new(),
                other_properties: Default::default(),
            }))
            .unwrap();
    }
    let (_, ManualAcknowledgement::QoS2(first)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("expected first QoS 2 PUBLISH");
    };
    let (_, ManualAcknowledgement::QoS2(second)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("expected second QoS 2 PUBLISH");
    };

    let second_ct = second.accept(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    let first_ct = first.accept(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    for expected in [21, 22] {
        assert_matches!(
            outgoing_packets_rx.recv().now_or_never(),
            Some(Some(Packet::PubRec(mqtt_proto::PubRec { packet_identifier, .. })))
                if packet_identifier.get() == expected
        );
    }
    drop(first_ct);
    drop(second_ct);
}

#[tokio::test(start_paused = true)]
async fn inbound_qos2_recovers_each_session_phase() {
    let (_client, mut receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());
    let packet_identifier = PacketIdentifier::new(31).unwrap();
    let publish = mqtt_proto::Publish {
        topic_name: topic("qos2/inbound-recovery"),
        packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, false),
        retain: false,
        payload: Bytes::new(),
        other_properties: Default::default(),
    };
    incoming_packets_tx
        .send(Packet::Publish(publish.clone()))
        .unwrap();
    let (_, ManualAcknowledgement::QoS2(pubrec_token)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("expected incoming QoS 2 PUBLISH");
    };
    let pubrec_ct = pubrec_token.accept(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRec(_)))
    );

    drop(incoming_packets_tx);
    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::IoError(_));
    let (reconnected, incoming_packets_tx, mut outgoing_packets_rx) =
        reconnect(connect_handle, true).await;
    let mut connection = pin!(reconnected.run_until_disconnect());

    let mut duplicate = publish;
    duplicate.packet_identifier_dup_qos =
        PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, true);
    incoming_packets_tx
        .send(Packet::Publish(duplicate))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRec(mqtt_proto::PubRec { packet_identifier: outgoing_identifier, .. })))
            if outgoing_identifier == packet_identifier
    );
    assert!(receiver.recv().now_or_never().is_none());

    incoming_packets_tx
        .send(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier,
            reason_code: PubRelReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let (_, pubcomp_token) = pubrec_ct.await.unwrap();

    drop(incoming_packets_tx);
    let (connect_handle, event) = connection.await;
    assert_matches!(event, DisconnectedEvent::IoError(_));
    let (reconnected, incoming_packets_tx, mut outgoing_packets_rx) =
        reconnect(connect_handle, true).await;
    let mut connection = pin!(reconnected.run_until_disconnect());
    incoming_packets_tx
        .send(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier,
            reason_code: PubRelReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(outgoing_packets_rx.recv().now_or_never(), None);

    let pubcomp_ct = pubcomp_token.confirm(Default::default()).await.unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubComp(mqtt_proto::PubComp { packet_identifier: outgoing_identifier, .. })))
            if outgoing_identifier == packet_identifier
    );
    pubcomp_ct.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn packet_identifiers_are_independent_by_direction() {
    let (client, mut receiver, connection, incoming_packets_tx, mut outgoing_packets_rx) =
        connect().await;
    let mut connection = pin!(connection.run_until_disconnect());
    let packet_identifier = PacketIdentifier::new(1).unwrap();

    let outbound_ct = client
        .publish_qos2(
            TopicName::new("qos2/bidirectional/outbound").unwrap(),
            Bytes::new(),
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
        Some(Some(Packet::Publish(_)))
    );

    incoming_packets_tx
        .send(Packet::Publish(mqtt_proto::Publish {
            topic_name: topic("qos2/bidirectional/inbound"),
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(
                packet_identifier,
                false,
            ),
            retain: false,
            payload: Bytes::new(),
            other_properties: Default::default(),
        }))
        .unwrap();
    let (_, ManualAcknowledgement::QoS2(inbound_token)) =
        receive_publish(&mut connection, &mut receiver).await
    else {
        panic!("expected inbound QoS 2 PUBLISH");
    };
    let inbound_ct = inbound_token.accept(Default::default()).await.unwrap();
    incoming_packets_tx
        .send(Packet::PubRec(mqtt_proto::PubRec {
            packet_identifier,
            reason_code: PubRecReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRec(_)))
    );
    let (_, Some(outbound_token)) = outbound_ct.await.unwrap() else {
        panic!("outbound PUBLISH should be accepted");
    };

    incoming_packets_tx
        .send(Packet::PubRel(mqtt_proto::PubRel {
            packet_identifier,
            reason_code: PubRelReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let (_, inbound_pubcomp_token) = inbound_ct.await.unwrap();
    let outbound_pubcomp_ct = outbound_token.confirm(Default::default()).await.unwrap();
    let inbound_pubcomp_ct = inbound_pubcomp_token
        .confirm(Default::default())
        .await
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubRel(_)))
    );
    assert_matches!(
        outgoing_packets_rx.recv().now_or_never(),
        Some(Some(Packet::PubComp(_)))
    );
    inbound_pubcomp_ct.await.unwrap();

    incoming_packets_tx
        .send(Packet::PubComp(mqtt_proto::PubComp {
            packet_identifier,
            reason_code: PubCompReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    outbound_pubcomp_ct.await.unwrap();
}
