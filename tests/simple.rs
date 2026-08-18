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
use ms_mqtt_client::mqtt_proto::{self, ConnectReasonCode, Packet};
use ms_mqtt_client::packet::{ConnAck, ConnectProperties, QoS, RetainOptions, SubscribeProperties};
use ms_mqtt_client::topic::TopicFilter;
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};
use tokio::sync::mpsc::unbounded_channel;

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
