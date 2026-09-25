// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::num::NonZeroU16;
use std::panic::AssertUnwindSafe;
use std::pin::pin;
use std::time::Duration;

use futures_util::FutureExt as _;
use matches::assert_matches;
use ms_mqtt_client::client::{
    ClientOptions, ConnectResult, DisconnectedEvent, KeepAliveConfig, new_client,
};
use ms_mqtt_client::error::ConnectError;
use ms_mqtt_client::mqtt_proto::{self, ConnectReasonCode, Packet};
use ms_mqtt_client::packet::{
    ConnAck, ConnectProperties, QoS, RetainOptions, SessionExpiryInterval, SubscribeProperties,
};
use ms_mqtt_client::topic::TopicFilter;
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};
use tokio::sync::mpsc::unbounded_channel;

async fn connect_new_client_with_session_present(clean_start: bool) -> ConnectResult {
    let (_client, connect_handle, _receiver) = new_client(ClientOptions {
        client_id: Some("foo".to_string()),
        ..Default::default()
    });
    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, _outgoing_packets_rx) = unbounded_channel();
    incoming_packets_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success {
                session_present: true,
            },
            other_properties: Default::default(),
        }))
        .unwrap();

    connect_handle
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
            clean_start,
            KeepAliveConfig::Infinite,
            None,
            None,
            None,
            ConnectProperties::default(),
            None,
        )
        .await
}

#[tokio::test]
async fn clean_start_rejects_session_present() {
    assert!(matches!(
        connect_new_client_with_session_present(true).await,
        ConnectResult::Failure(_, ConnectError::Protocol(_))
    ));
}

#[tokio::test]
async fn missing_local_session_state_rejects_session_present() {
    assert!(matches!(
        connect_new_client_with_session_present(false).await,
        ConnectResult::Failure(_, ConnectError::Protocol(_))
    ));
}

#[tokio::test]
async fn reconnect_accepts_session_present_with_local_state() {
    let (_client, connect_handle, _receiver) = new_client(ClientOptions {
        client_id: Some("foo".to_string()),
        ..Default::default()
    });
    let (first_incoming_tx, first_incoming_rx) = unbounded_channel();
    let (first_outgoing_tx, mut first_outgoing_rx) = unbounded_channel();
    first_incoming_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success {
                session_present: false,
            },
            other_properties: Default::default(),
        }))
        .unwrap();
    let properties = ConnectProperties {
        session_expiry_interval: SessionExpiryInterval::Duration(60),
        ..Default::default()
    };

    let ConnectResult::Success(connection, _connack, _disconnect_handle) = connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Test {
                    incoming_packets: first_incoming_rx,
                    outgoing_packets: first_outgoing_tx,
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
            properties.clone(),
            None,
        )
        .await
    else {
        panic!("expected initial connection to succeed");
    };
    assert_matches!(first_outgoing_rx.recv().await, Some(Packet::Connect(_)));
    drop(first_incoming_tx);
    let (connect_handle, _event) = connection.run_until_disconnect().await;

    let (second_incoming_tx, second_incoming_rx) = unbounded_channel();
    let (second_outgoing_tx, _second_outgoing_rx) = unbounded_channel();
    second_incoming_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success {
                session_present: true,
            },
            other_properties: Default::default(),
        }))
        .unwrap();

    assert!(matches!(
        connect_handle
            .connect(
                ConnectionTransportConfig {
                    transport_type: ConnectionTransportType::Test {
                        incoming_packets: second_incoming_rx,
                        outgoing_packets: second_outgoing_tx,
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
                properties,
                None,
            )
            .await,
        ConnectResult::Success(_, _, _)
    ));
}

#[tokio::test(start_paused = true)]
async fn subscribe_qos2_panics_without_submission() {
    let (client, _connect_handle, _receiver) = new_client(ClientOptions::default());

    let qos2_result = AssertUnwindSafe(client.subscribe(
        TopicFilter::new("test/topic").unwrap(),
        QoS::ExactlyOnce,
        false,
        RetainOptions::default(),
        SubscribeProperties::default(),
    ))
    .catch_unwind()
    .await;
    assert!(qos2_result.is_err());

    // The subscription queue has capacity one, so this completes only if QoS 2 submitted nothing.
    let _ct = tokio::time::timeout(
        Duration::from_secs(1),
        client.subscribe(
            TopicFilter::new("test/topic").unwrap(),
            QoS::AtLeastOnce,
            false,
            RetainOptions::default(),
            SubscribeProperties::default(),
        ),
    )
    .await
    .expect("QoS 2 should not consume subscription queue capacity")
    .expect("client should remain attached");
}

#[tokio::test(start_paused = true)]
async fn connect_connack_success() {
    let options = ClientOptions {
        client_id: Some("foo".to_string()),
        ..Default::default()
    };
    let (_client, connect_handle, _receiver) = new_client(options);

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

    let keep_alive_time = NonZeroU16::new(5).unwrap();

    let ConnectResult::Success(connection, connack, _disconnect_handle) = connect_handle
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
            KeepAliveConfig::Duration {
                ping_after: keep_alive_time,
                response_timeout: Duration::from_secs(5),
            },
            None,
            None,
            None,
            ConnectProperties::default(),
            Some(Duration::from_secs(5)),
        )
        .await
    else {
        panic!("expected successful connect")
    };
    let outgoing_packet = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(outgoing_packet, Packet::Connect(mqtt_proto::Connect { .. }));
    assert_matches!(connack, ConnAck { .. });

    let mut connection = pin!(connection.run_until_disconnect());

    // Run the connection for long enough that it has time to generate one PINGREQ.
    // Wait one second longer than the keep alive time.
    _ = tokio::time::timeout(
        Duration::from_secs(u64::from(keep_alive_time.get() + 1)),
        &mut connection,
    )
    .await;

    let outgoing_packet = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(outgoing_packet, Packet::PingReq(mqtt_proto::PingReq));
    incoming_packets_tx
        .send(Packet::PingResp(mqtt_proto::PingResp))
        .unwrap();

    // Server EOF
    drop(incoming_packets_tx);

    let (_connect_handle, disconnected_event) = connection.await;
    assert_matches!(disconnected_event, DisconnectedEvent::IoError(_));
}
