// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::pin::pin;
use std::num::NonZeroU16;
use std::time::Duration;

use azure_mqtt::client::{
    ClientOptions, ConnectResult, ConnectionTransportConfig, DisconnectedEvent, new_client,
};
use azure_mqtt::mqtt_proto::{self, ConnectReasonCode, Packet};
use azure_mqtt::packet::{ConnAck, ConnectProperties, KeepAlive};
use matches::assert_matches;
use tokio::sync::mpsc::unbounded_channel;

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
            ConnectionTransportConfig::Test {
                incoming_packets: incoming_packets_rx,
                outgoing_packets: outgoing_packets_tx,
            },
            false,
            KeepAlive::Duration(keep_alive_time),
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
    let server_connect = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(server_connect, Packet::Connect(mqtt_proto::Connect { .. }));
    assert_matches!(connack, ConnAck { .. });

    let mut connection = pin!(connection.run_until_disconnect());

    // Run the connection for long enough that it has time to generate one PINGREQ.
    // Wait one second longer than the keep alive time.
    _ = tokio::time::timeout(Duration::from_secs(u64::from(keep_alive_time.get() + 1)), &mut connection).await;

    let server_connect = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(server_connect, Packet::PingReq(mqtt_proto::PingReq));

    // Server EOF
    drop(incoming_packets_tx);

    let (_connect_handle, disconnected_event) = connection.await;
    assert_matches!(disconnected_event, DisconnectedEvent::IoError(_));
}
