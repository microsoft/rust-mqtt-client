// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// NOTE: Consider refactoring or restructuring this test module as appropriate after QoS2 expanded testing lands

use std::num::NonZeroU16;
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::FutureExt as _;
use matches::assert_matches;
use ms_mqtt_client::client::token::acknowledgement::PubAckToken;
use ms_mqtt_client::client::token::completion::PubAckCompletionToken;
use ms_mqtt_client::client::{
    ClientOptions, ConnectHandle, ConnectResult, DisconnectedEvent, KeepAliveConfig,
    ManualAcknowledgement, Receiver, new_client,
};
use ms_mqtt_client::mqtt_proto::{
    self, ConnectReasonCode, Packet, PacketIdentifier, PacketIdentifierDupQoS, PubAckReasonCode,
    topic,
};
use ms_mqtt_client::packet::{
    ConnectProperties, DeliveryQoS, PubRejectReason, SessionExpiryInterval,
};
use ms_mqtt_client::topic::TopicName;
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};
use test_case::test_matrix;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

mod common;

#[derive(Clone, Copy, Debug)]
enum Disconnect {
    ReadEof,
    PingTimeout,
    WriteFailure,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum StaleTiming {
    BeforeReconnect,
    BeforeRedelivery,
    AfterRedelivery,
    AfterFreshAck,
}

#[derive(Clone, Copy, Debug)]
enum StaleAction {
    Drop,
    Accept,
    Reject,
}

struct TestConnection {
    driver: Pin<Box<dyn Future<Output = (ConnectHandle, DisconnectedEvent)>>>,
    incoming_packets_tx: UnboundedSender<Packet<Bytes>>,
    outgoing_packets_rx: UnboundedReceiver<Packet<Bytes>>,
}

impl TestConnection {
    async fn connect(connect_handle: ConnectHandle, session_present: bool) -> Self {
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
                KeepAliveConfig::Duration {
                    ping_after: NonZeroU16::new(10).unwrap(),
                    response_timeout: Duration::from_secs(2),
                },
                None,
                None,
                None,
                ConnectProperties {
                    session_expiry_interval: SessionExpiryInterval::Duration(60),
                    ..Default::default()
                },
                None,
            )
            .await
        else {
            panic!("expected successful connect")
        };
        assert_matches!(outgoing_packets_rx.try_recv(), Ok(Packet::Connect(_)));
        Self {
            driver: Box::pin(connection.run_until_disconnect()),
            incoming_packets_tx,
            outgoing_packets_rx,
        }
    }

    async fn drive(&mut self) {
        assert_matches!(
            tokio::time::timeout(Duration::from_millis(1), &mut self.driver).await,
            Err(_)
        );
    }

    async fn receive_qos1(
        &mut self,
        receiver: &mut Receiver,
        packet_identifier: u16,
        dup: bool,
    ) -> PubAckToken {
        self.incoming_packets_tx
            .send(Packet::Publish(mqtt_proto::Publish {
                topic_name: topic("foo"),
                packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                    PacketIdentifier::new(packet_identifier).unwrap(),
                    dup,
                ),
                retain: false,
                payload: Bytes::from_static(b"payload"),
                other_properties: Default::default(),
            }))
            .unwrap();
        let (publish, ManualAcknowledgement::QoS1(token)) = tokio::time::timeout(
            Duration::from_secs(1),
            common::receive_publish(&mut self.driver, receiver),
        )
        .await
        .expect("PUBLISH delivery timed out") else {
            panic!("expected QoS 1 PUBLISH")
        };
        assert_eq!(publish.payload, Bytes::from_static(b"payload"));
        assert_matches!(
            publish.qos,
            DeliveryQoS::AtLeastOnce(info)
                if info.packet_identifier.get() == packet_identifier && info.dup == dup
        );
        token
    }

    async fn disconnect(mut self, reason: Disconnect) -> ConnectHandle {
        match reason {
            Disconnect::ReadEof => drop(self.incoming_packets_tx),
            Disconnect::PingTimeout => {}
            // The next PINGREQ fails to write while the read side remains open.
            Disconnect::WriteFailure => self.outgoing_packets_rx.close(),
        }
        let (connect_handle, event) =
            tokio::time::timeout(Duration::from_secs(20), &mut self.driver)
                .await
                .expect("disconnect timed out");
        match reason {
            Disconnect::ReadEof | Disconnect::WriteFailure => {
                assert_matches!(event, DisconnectedEvent::IoError(_));
            }
            Disconnect::PingTimeout => assert_matches!(event, DisconnectedEvent::PingTimeout),
        }
        connect_handle
    }

    fn expect_pubacks(&mut self, packet_identifiers: &[u16]) {
        for expected in packet_identifiers {
            assert_matches!(
                self.outgoing_packets_rx.try_recv(),
                Ok(Packet::PubAck(ack))
                    if ack.packet_identifier.get() == *expected
                        && ack.reason_code == PubAckReasonCode::Success
            );
        }
        assert_matches!(
            self.outgoing_packets_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );
    }
}

async fn submit_stale(token: PubAckToken, action: StaleAction) -> Option<PubAckCompletionToken> {
    match action {
        StaleAction::Drop => {
            drop(token);
            None
        }
        StaleAction::Accept => Some(token.accept(Default::default()).await.unwrap()),
        StaleAction::Reject => Some(
            token
                .reject(PubRejectReason::UnspecifiedError, Default::default())
                .await
                .unwrap(),
        ),
    }
}

#[test_matrix(
    [Disconnect::ReadEof, Disconnect::PingTimeout, Disconnect::WriteFailure],
    [false, true],
    [
        StaleTiming::BeforeReconnect,
        StaleTiming::BeforeRedelivery,
        StaleTiming::AfterRedelivery,
        StaleTiming::AfterFreshAck
    ],
    [StaleAction::Drop, StaleAction::Accept, StaleAction::Reject]
)]
#[tokio::test(start_paused = true)]
async fn reconnect_uses_fresh_puback_token(
    disconnect: Disconnect,
    session_present: bool,
    timing: StaleTiming,
    action: StaleAction,
) {
    let (_client, connect_handle, mut receiver) = new_client(ClientOptions {
        client_id: Some("persistent-client".to_string()),
        ..Default::default()
    });
    let mut connection = TestConnection::connect(connect_handle, false).await;
    let mut old_token = Some(connection.receive_qos1(&mut receiver, 5, false).await);
    let connect_handle = connection.disconnect(disconnect).await;
    let mut old_completion = None;

    if timing == StaleTiming::BeforeReconnect {
        old_completion = submit_stale(old_token.take().unwrap(), action).await;
    }
    let mut connection = TestConnection::connect(connect_handle, session_present).await;
    if timing == StaleTiming::BeforeRedelivery {
        old_completion = submit_stale(old_token.take().unwrap(), action).await;
    }
    connection.drive().await;
    connection.expect_pubacks(&[]);

    // With no resumed session this is a new delivery, not a retransmission.
    let current_token = connection
        .receive_qos1(&mut receiver, 5, session_present)
        .await;
    if timing == StaleTiming::AfterRedelivery {
        old_completion = submit_stale(old_token.take().unwrap(), action).await;
    }
    connection.drive().await;
    connection.expect_pubacks(&[]);

    let completion = current_token.accept(Default::default()).await.unwrap();
    connection.drive().await;
    connection.expect_pubacks(&[5]);
    assert_matches!(completion.now_or_never(), Some(Ok(())));

    if timing == StaleTiming::AfterFreshAck {
        old_completion = submit_stale(old_token.take().unwrap(), action).await;
    }
    connection.drive().await;
    connection.expect_pubacks(&[]);
    if let Some(completion) = old_completion {
        assert_matches!(completion.now_or_never(), Some(Err(_)));
    }
}

#[test_matrix(
    [Disconnect::ReadEof, Disconnect::PingTimeout, Disconnect::WriteFailure],
    [false, true]
)]
#[tokio::test(start_paused = true)]
async fn buffered_puback_does_not_cross_connections(disconnect: Disconnect, session_present: bool) {
    let (_client, connect_handle, mut receiver) = new_client(ClientOptions {
        client_id: Some("persistent-client".to_string()),
        ..Default::default()
    });
    let mut connection = TestConnection::connect(connect_handle, false).await;
    let old_first = connection.receive_qos1(&mut receiver, 5, false).await;
    let old_second = connection.receive_qos1(&mut receiver, 6, false).await;
    let mut old_completion = old_second.accept(Default::default()).await.unwrap();
    connection.drive().await;
    connection.expect_pubacks(&[]);
    assert_matches!((&mut old_completion).now_or_never(), None);

    let connect_handle = connection.disconnect(disconnect).await;
    let mut connection = TestConnection::connect(connect_handle, session_present).await;
    drop(old_first);
    connection.drive().await;
    connection.expect_pubacks(&[]);
    assert_matches!(old_completion.now_or_never(), Some(Err(_)));

    let first = connection
        .receive_qos1(&mut receiver, 5, session_present)
        .await;
    let second = connection
        .receive_qos1(&mut receiver, 6, session_present)
        .await;
    let mut second_completion = second.accept(Default::default()).await.unwrap();
    connection.drive().await;
    connection.expect_pubacks(&[]);
    assert_matches!((&mut second_completion).now_or_never(), None);

    let first_completion = first.accept(Default::default()).await.unwrap();
    connection.drive().await;
    connection.expect_pubacks(&[5, 6]);
    assert_matches!(first_completion.now_or_never(), Some(Ok(())));
    assert_matches!(second_completion.now_or_never(), Some(Ok(())));
}

#[test_matrix([Disconnect::ReadEof, Disconnect::PingTimeout, Disconnect::WriteFailure])]
#[tokio::test(start_paused = true)]
async fn repeated_reconnects_invalidate_all_older_tokens(disconnect: Disconnect) {
    let (_client, connect_handle, mut receiver) = new_client(ClientOptions {
        client_id: Some("persistent-client".to_string()),
        ..Default::default()
    });
    let mut connection = TestConnection::connect(connect_handle, false).await;
    let mut old_tokens = Vec::new();
    for redelivery in [false, true, true] {
        old_tokens.push(connection.receive_qos1(&mut receiver, 5, redelivery).await);
        let connect_handle = connection.disconnect(disconnect).await;
        connection = TestConnection::connect(connect_handle, true).await;
    }
    let current = connection.receive_qos1(&mut receiver, 5, true).await;
    drop(old_tokens);
    connection.drive().await;
    connection.expect_pubacks(&[]);

    let completion = current.accept(Default::default()).await.unwrap();
    connection.drive().await;
    connection.expect_pubacks(&[5]);
    assert_matches!(completion.now_or_never(), Some(Ok(())));
}

#[tokio::test(start_paused = true)]
async fn incoming_puback_reset_preserves_outbound_session_state() {
    let (client, connect_handle, mut receiver) = new_client(ClientOptions {
        client_id: Some("persistent-client".to_string()),
        ..Default::default()
    });
    let mut connection = TestConnection::connect(connect_handle, false).await;
    let mut outgoing_completion = client
        .publish_qos1(
            TopicName::new("foo").unwrap(),
            Bytes::from_static(b"outgoing"),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let old_incoming = connection.receive_qos1(&mut receiver, 1, false).await;
    assert_matches!(
        connection.outgoing_packets_rx.try_recv(),
        Ok(Packet::Publish(publish))
            if publish.packet_identifier_dup_qos
                == PacketIdentifierDupQoS::AtLeastOnce(PacketIdentifier::new(1).unwrap(), false)
                && publish.payload == Bytes::from_static(b"outgoing")
    );

    let connect_handle = connection.disconnect(Disconnect::ReadEof).await;
    let mut connection = TestConnection::connect(connect_handle, true).await;
    connection.drive().await;
    assert_matches!(
        connection.outgoing_packets_rx.try_recv(),
        Ok(Packet::Publish(publish))
            if publish.packet_identifier_dup_qos
                == PacketIdentifierDupQoS::AtLeastOnce(PacketIdentifier::new(1).unwrap(), true)
                && publish.payload == Bytes::from_static(b"outgoing")
    );
    assert_matches!((&mut outgoing_completion).now_or_never(), None);

    let current_incoming = connection.receive_qos1(&mut receiver, 1, true).await;
    drop(old_incoming);
    let incoming_completion = current_incoming.accept(Default::default()).await.unwrap();
    connection.drive().await;
    connection.expect_pubacks(&[1]);
    assert_matches!(incoming_completion.now_or_never(), Some(Ok(())));
    assert_matches!((&mut outgoing_completion).now_or_never(), None);

    connection
        .incoming_packets_tx
        .send(Packet::PubAck(mqtt_proto::PubAck {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubAckReasonCode::Success,
            other_properties: Default::default(),
        }))
        .unwrap();
    connection.drive().await;
    connection.expect_pubacks(&[]);
    assert_matches!(outgoing_completion.now_or_never(), Some(Ok(ack)) if ack.is_success());
}
