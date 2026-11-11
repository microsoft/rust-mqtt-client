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

#[test_case(KeepAliveConfig::Infinite; "without_response_timeout")]
#[test_case(
    KeepAliveConfig::Duration {
        ping_after: NonZeroU16::new(60).unwrap(),
        response_timeout: Duration::from_secs(5),
    };
    "with_response_timeout"
)]
#[tokio::test(start_paused = true)]
async fn ping_completes_on_pingresp(keep_alive: KeepAliveConfig) {
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
            keep_alive,
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

    if let KeepAliveConfig::Duration {
        response_timeout, ..
    } = keep_alive
    {
        // Drive past the answered ping's deadline before sending another ping.
        tokio::time::advance(response_timeout).await;
        assert!(connection.as_mut().now_or_never().is_none());

        let _ct = client.ping().await.unwrap();
        assert!(connection.as_mut().now_or_never().is_none());
        assert_matches!(outgoing_packets_rx.try_recv(), Ok(Packet::PingReq(_)));

        tokio::time::advance(response_timeout - Duration::from_secs(1)).await;
        assert!(connection.as_mut().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        let (_connect_handle, event) = connection.as_mut().now_or_never().unwrap();
        assert_matches!(event, DisconnectedEvent::PingTimeout);
    }
}

#[test_case(60, 60, None, false; "equal_intervals")]
#[test_case(30, 60, None, false; "longer_response_timeout")]
#[test_case(60, 59, None, false; "shorter_response_timeout_control")]
#[test_case(120, 60, Some(30), false; "shorter_server_keepalive")]
#[test_case(120, 60, None, true; "manual_ping_while_awaiting_response")]
#[tokio::test(start_paused = true)]
async fn unanswered_ping_keeps_original_deadline(
    ping_after: u16,
    response_timeout: u64,
    server_keepalive: Option<u16>,
    send_manual_ping: bool,
) {
    let (client, connect_handle, _receiver) = new_client(ClientOptions::default());
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

    let ConnectResult::Success(connection, _, _disconnect_handle) = connect_handle
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
        panic!("expected successful connect");
    };
    assert_matches!(outgoing_packets_rx.try_recv(), Ok(Packet::Connect(_)));

    let mut connection = pin!(connection.run_until_disconnect());
    assert!(connection.as_mut().now_or_never().is_none());

    let effective_keepalive = u64::from(server_keepalive.unwrap_or(ping_after));
    let deadline = effective_keepalive + response_timeout;
    let mut first_ping = None;
    let mut ping_count = 0;

    for elapsed in 1..=deadline {
        tokio::time::advance(Duration::from_secs(1)).await;
        if send_manual_ping && elapsed == effective_keepalive + 1 {
            let _ct = client.ping().await.unwrap();
        }
        let disconnected = connection.as_mut().now_or_never();

        while let Ok(packet) = outgoing_packets_rx.try_recv() {
            assert_matches!(packet, Packet::PingReq(_));
            first_ping.get_or_insert(elapsed);
            ping_count += 1;
        }

        if elapsed < deadline {
            assert!(disconnected.is_none(), "disconnected early at {elapsed}s");
        } else {
            let (_connect_handle, event) =
                disconnected.expect("unanswered pings must not extend the original deadline");
            assert_matches!(event, DisconnectedEvent::PingTimeout);
        }
    }

    assert_eq!(first_ping, Some(effective_keepalive));
    if send_manual_ping {
        assert_eq!(
            ping_count, 2,
            "both automatic and manual pings must be sent"
        );
    }
}
