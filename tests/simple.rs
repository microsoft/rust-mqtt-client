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
use ms_mqtt_client::mqtt_proto::{self, ConnectReasonCode, KeepAlive, Packet};
use ms_mqtt_client::packet::{ConnAck, ConnectProperties, QoS, RetainOptions, SubscribeProperties};
use ms_mqtt_client::topic::TopicFilter;
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};
use test_case::test_case;
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

#[tokio::test(start_paused = true)]
async fn ping_completes_on_pingresp() {
    let (client, connect_handle, _receiver) = new_client(ClientOptions::default());

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

    let ConnectResult::Success(connection, _connack, _disconnect_handle) = connect_handle
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
        panic!("expected successful connect")
    };
    let outgoing_packet = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(outgoing_packet, Packet::Connect(mqtt_proto::Connect { .. }));

    let mut connection = pin!(connection.run_until_disconnect());
    let mut ct = client.ping().await.unwrap();

    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    let outgoing_packet = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(outgoing_packet, Packet::PingReq(mqtt_proto::PingReq));
    assert!((&mut ct).now_or_never().is_none());

    incoming_packets_tx
        .send(Packet::PingResp(mqtt_proto::PingResp))
        .unwrap();
    assert_matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut connection).await,
        Err(_)
    );
    // Paused time: the PINGREQ went out at 0s and its PINGRESP was read at 1s.
    assert_eq!(ct.now_or_never(), Some(Ok(Duration::from_secs(1))));
}

#[test_case(60, 60, None, false; "equal_intervals")]
#[test_case(30, 60, None, false; "longer_response_timeout")]
#[test_case(60, 59, None, false; "shorter_response_timeout_control")]
#[test_case(120, 60, Some(30), false; "shorter_server_keepalive")]
#[test_case(60, 60, None, true; "pingresp_allows_a_new_deadline")]
#[tokio::test(start_paused = true)]
async fn unanswered_ping_keeps_original_deadline(
    ping_after: u16,
    response_timeout: u64,
    server_keepalive: Option<u16>,
    respond_to_first_ping: bool,
) {
    let (_client, connect_handle, _receiver) = new_client(ClientOptions::default());
    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();

    incoming_packets_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success {
                session_present: false,
            },
            other_properties: mqtt_proto::ConnAckOtherProperties {
                server_keep_alive: server_keepalive
                    .map(|seconds| KeepAlive::Duration(NonZeroU16::new(seconds).unwrap())),
                ..Default::default()
            },
        }))
        .unwrap();

    let ConnectResult::Success(connection, _connack, _disconnect_handle) = connect_handle
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
                ping_after: NonZeroU16::new(ping_after).unwrap(),
                response_timeout: Duration::from_secs(response_timeout),
            },
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
    assert_matches!(
        outgoing_packets_rx.recv().await.unwrap(),
        Packet::Connect(_)
    );

    let effective_keepalive = u64::from(server_keepalive.unwrap_or(ping_after));
    let unanswered_ping_at = effective_keepalive * if respond_to_first_ping { 2 } else { 1 };
    let expected_timeout = unanswered_ping_at + response_timeout;
    let mut connection = pin!(connection.run_until_disconnect());
    let mut ping_count = 0;

    for elapsed in 0..=expected_timeout + 5 {
        if let Some((_connect_handle, event)) = connection.as_mut().now_or_never() {
            assert_matches!(event, DisconnectedEvent::PingTimeout);
            assert!(
                (expected_timeout..=expected_timeout + 1).contains(&elapsed),
                "expected timeout at {expected_timeout}s, disconnected at {elapsed}s"
            );
            assert!(ping_count > 0);
            return;
        }

        while let Ok(packet) = outgoing_packets_rx.try_recv() {
            assert_matches!(packet, Packet::PingReq(_));
            ping_count += 1;
            if ping_count == 1 {
                assert_eq!(elapsed, effective_keepalive);
                if respond_to_first_ping {
                    incoming_packets_tx
                        .send(Packet::PingResp(mqtt_proto::PingResp))
                        .unwrap();
                }
            }
        }
        tokio::time::advance(Duration::from_secs(1)).await;
    }

    panic!("still connected beyond the first unanswered PINGREQ deadline");
}
