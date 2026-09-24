// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Every way an established connection can end must apply exactly one session transition.

use std::num::NonZeroU16;
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::FutureExt as _;
use matches::assert_matches;
use ms_mqtt_client::client::token::completion::{
    PublishQoS1CompletionToken, SubscribeCompletionToken,
};
use ms_mqtt_client::client::{
    Client, ClientOptions, ConnectHandle, ConnectResult, DisconnectHandle, DisconnectedEvent,
    KeepAliveConfig, new_client,
};
use ms_mqtt_client::mqtt_proto::{
    self, AuthenticateReasonCode, ConnectReasonCode, DisconnectReasonCode, Packet,
    PacketIdentifier, PacketIdentifierDupQoS, PingReq, PubAckReasonCode,
};
use ms_mqtt_client::packet::{
    ConnectProperties, DisconnectProperties, QoS, RetainOptions, SessionExpiryInterval,
};
use ms_mqtt_client::topic::{TopicFilter, TopicName};
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};
use test_case::test_matrix;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

// Never leased in these tests, so acknowledging it is always unexpected.
const UNKNOWN_PACKET_IDENTIFIER: u16 = 1000;

#[derive(Clone, Copy, Debug)]
enum Termination {
    // The server closes the transport.
    ReadEof,
    // Writing the next PINGREQ fails.
    WriteFailure,
    // The server does not respond to PINGREQ.
    PingTimeout,
    // The server acknowledges a PUBLISH that is not in flight.
    UnexpectedAck,
    // The server completes a reauthentication that was never started.
    UnexpectedAuth,
    // The server sends a packet that servers never send.
    UnexpectedPacket,
    // The server sends DISCONNECT.
    ServerDisconnect,
}

fn puback(packet_identifier: u16) -> Packet<Bytes> {
    Packet::PubAck(mqtt_proto::PubAck {
        packet_identifier: PacketIdentifier::new(packet_identifier).unwrap(),
        reason_code: PubAckReasonCode::Success,
        other_properties: Default::default(),
    })
}

fn termination_packet(termination: Termination) -> Option<Packet<Bytes>> {
    match termination {
        Termination::ReadEof | Termination::WriteFailure | Termination::PingTimeout => None,
        Termination::UnexpectedAck => Some(puback(UNKNOWN_PACKET_IDENTIFIER)),
        Termination::UnexpectedAuth => Some(Packet::Auth(mqtt_proto::Auth {
            reason_code: AuthenticateReasonCode::Success,
            authentication: None,
            reason_string: None,
            user_properties: Vec::new(),
        })),
        Termination::UnexpectedPacket => Some(Packet::PingReq(PingReq)),
        Termination::ServerDisconnect => Some(Packet::Disconnect(mqtt_proto::Disconnect {
            reason_code: DisconnectReasonCode::ServerShuttingDown,
            other_properties: Default::default(),
        })),
    }
}

struct TestConnection {
    driver: Pin<Box<dyn Future<Output = (ConnectHandle, DisconnectedEvent)>>>,
    incoming_packets_tx: UnboundedSender<Packet<Bytes>>,
    outgoing_packets_rx: UnboundedReceiver<Packet<Bytes>>,
    disconnect_handle: DisconnectHandle,
}

impl TestConnection {
    // Requests a Session Expiry Interval of 60 seconds, which the CONNACK can override.
    async fn connect(
        connect_handle: ConnectHandle,
        session_present: bool,
        connack_session_expiry_interval: Option<SessionExpiryInterval>,
    ) -> Self {
        let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
        let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();
        incoming_packets_tx
            .send(Packet::ConnAck(mqtt_proto::ConnAck {
                reason_code: ConnectReasonCode::Success { session_present },
                other_properties: mqtt_proto::ConnAckOtherProperties {
                    session_expiry_interval: connack_session_expiry_interval,
                    ..Default::default()
                },
            }))
            .unwrap();

        let ConnectResult::Success(connection, _, disconnect_handle) = connect_handle
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
            disconnect_handle,
        }
    }

    async fn drive(&mut self) {
        assert_matches!(
            tokio::time::timeout(Duration::from_millis(1), &mut self.driver).await,
            Err(_)
        );
    }

    fn expect_publish(&mut self, packet_identifier: u16, dup: bool) {
        assert_matches!(
            self.outgoing_packets_rx.try_recv(),
            Ok(Packet::Publish(publish))
                if publish.packet_identifier_dup_qos
                    == PacketIdentifierDupQoS::AtLeastOnce(
                        PacketIdentifier::new(packet_identifier).unwrap(),
                        dup,
                    )
        );
        assert_matches!(
            self.outgoing_packets_rx.try_recv(),
            Err(TryRecvError::Empty)
        );
    }

    fn expect_subscribe(&mut self, packet_identifier: u16) {
        assert_matches!(
            self.outgoing_packets_rx.try_recv(),
            Ok(Packet::Subscribe(subscribe))
                if subscribe.packet_identifier.get() == packet_identifier
        );
        assert_matches!(
            self.outgoing_packets_rx.try_recv(),
            Err(TryRecvError::Empty)
        );
    }

    async fn terminate(self, termination: Termination) -> ConnectHandle {
        let Self {
            driver,
            incoming_packets_tx,
            mut outgoing_packets_rx,
            disconnect_handle: _disconnect_handle,
        } = self;
        // Keep the server side of the transport open unless the termination closes it.
        let _incoming_packets_tx = if let Termination::ReadEof = termination {
            drop(incoming_packets_tx);
            None
        } else {
            if let Some(packet) = termination_packet(termination) {
                incoming_packets_tx.send(packet).unwrap();
            }
            Some(incoming_packets_tx)
        };
        if let Termination::WriteFailure = termination {
            outgoing_packets_rx.close();
        }

        let (connect_handle, event) = tokio::time::timeout(Duration::from_secs(30), driver)
            .await
            .expect("connection did not end");
        match termination {
            Termination::ReadEof | Termination::WriteFailure => {
                assert_matches!(event, DisconnectedEvent::IoError(_));
            }
            Termination::PingTimeout => assert_matches!(event, DisconnectedEvent::PingTimeout),
            Termination::UnexpectedAck
            | Termination::UnexpectedAuth
            | Termination::UnexpectedPacket => {
                assert_matches!(event, DisconnectedEvent::ProtocolError(_));
            }
            Termination::ServerDisconnect => {
                assert_matches!(event, DisconnectedEvent::ServerDisconnect(_));
            }
        }
        connect_handle
    }

    async fn disconnect(
        self,
        properties: &DisconnectProperties,
    ) -> (
        ConnectHandle,
        DisconnectedEvent,
        UnboundedReceiver<Packet<Bytes>>,
    ) {
        let Self {
            driver,
            incoming_packets_tx: _incoming_packets_tx,
            outgoing_packets_rx,
            disconnect_handle,
        } = self;
        disconnect_handle.disconnect(properties).unwrap();
        let (connect_handle, event) = tokio::time::timeout(Duration::from_secs(1), driver)
            .await
            .expect("connection did not end");
        (connect_handle, event, outgoing_packets_rx)
    }
}

fn persistent_client(max_packet_identifier: PacketIdentifier) -> (Client, ConnectHandle) {
    let (client, connect_handle, _receiver) = new_client(ClientOptions {
        client_id: Some("persistent-client".to_string()),
        max_packet_identifier,
        ..Default::default()
    });
    (client, connect_handle)
}

async fn publish_qos1(client: &Client) -> PublishQoS1CompletionToken {
    client
        .publish_qos1(
            TopicName::new("foo").unwrap(),
            Bytes::from_static(b"payload"),
            false,
            Default::default(),
        )
        .await
        .unwrap()
}

async fn subscribe(client: &Client) -> SubscribeCompletionToken {
    client
        .subscribe(
            TopicFilter::new("foo").unwrap(),
            QoS::AtLeastOnce,
            false,
            RetainOptions::default(),
            Default::default(),
        )
        .await
        .unwrap()
}

fn expire_session_on_disconnect() -> DisconnectProperties {
    DisconnectProperties {
        session_expiry_interval: Some(SessionExpiryInterval::Duration(0)),
        ..Default::default()
    }
}

#[test_matrix([
    Termination::ReadEof,
    Termination::WriteFailure,
    Termination::PingTimeout,
    Termination::UnexpectedAck,
    Termination::UnexpectedAuth,
    Termination::UnexpectedPacket,
    Termination::ServerDisconnect
])]
#[tokio::test(start_paused = true)]
async fn termination_cancels_pending_subscribe(termination: Termination) {
    // With a single packet identifier, the next SUBSCRIBE can only be sent once it is released.
    let (client, connect_handle) = persistent_client(PacketIdentifier::new(1).unwrap());
    let mut connection = TestConnection::connect(connect_handle, false, None).await;
    let completion = subscribe(&client).await;
    connection.drive().await;
    connection.expect_subscribe(1);

    let connect_handle = connection.terminate(termination).await;
    assert_matches!(completion.now_or_never(), Some(Err(_)));

    let mut connection = TestConnection::connect(connect_handle, true, None).await;
    let _completion = subscribe(&client).await;
    connection.drive().await;
    connection.expect_subscribe(1);
}

#[test_matrix([
    Termination::ReadEof,
    Termination::WriteFailure,
    Termination::PingTimeout,
    Termination::UnexpectedAck,
    Termination::UnexpectedAuth,
    Termination::UnexpectedPacket,
    Termination::ServerDisconnect
])]
#[tokio::test(start_paused = true)]
async fn termination_prepares_inflight_publish_for_replay(termination: Termination) {
    let (client, connect_handle) = persistent_client(PacketIdentifier::MAX);
    let mut connection = TestConnection::connect(connect_handle, false, None).await;
    let mut completion = publish_qos1(&client).await;
    connection.drive().await;
    connection.expect_publish(1, false);

    let connect_handle = connection.terminate(termination).await;
    assert_matches!((&mut completion).now_or_never(), None);

    let mut connection = TestConnection::connect(connect_handle, true, None).await;
    connection.drive().await;
    connection.expect_publish(1, true);

    connection.incoming_packets_tx.send(puback(1)).unwrap();
    connection.drive().await;
    assert_matches!(completion.now_or_never(), Some(Ok(puback)) if puback.is_success());
}

#[test_matrix([
    Termination::ReadEof,
    Termination::WriteFailure,
    Termination::PingTimeout,
    Termination::UnexpectedAck,
    Termination::UnexpectedAuth,
    Termination::UnexpectedPacket,
    Termination::ServerDisconnect
])]
#[tokio::test(start_paused = true)]
async fn termination_expires_zero_expiry_session(termination: Termination) {
    let (client, connect_handle) = persistent_client(PacketIdentifier::MAX);
    // The server overrides the requested Session Expiry Interval with zero.
    let mut connection = TestConnection::connect(
        connect_handle,
        false,
        Some(SessionExpiryInterval::Duration(0)),
    )
    .await;
    let completion = publish_qos1(&client).await;
    connection.drive().await;
    connection.expect_publish(1, false);

    let _connect_handle = connection.terminate(termination).await;
    assert_matches!(completion.now_or_never(), Some(Err(_)));
}

#[tokio::test(start_paused = true)]
async fn written_disconnect_applies_application_disconnect() {
    let (client, connect_handle) = persistent_client(PacketIdentifier::MAX);
    let mut connection = TestConnection::connect(connect_handle, false, None).await;
    let completion = publish_qos1(&client).await;
    connection.drive().await;
    connection.expect_publish(1, false);

    let (_connect_handle, event, mut outgoing_packets_rx) =
        connection.disconnect(&expire_session_on_disconnect()).await;
    assert_matches!(event, DisconnectedEvent::ApplicationDisconnect);
    assert_matches!(
        outgoing_packets_rx.try_recv(),
        Ok(Packet::Disconnect(disconnect))
            if disconnect.other_properties.session_expiry_interval
                == Some(SessionExpiryInterval::Duration(0))
    );
    // The Session Expiry Interval override of zero expired the session.
    assert_matches!(completion.now_or_never(), Some(Err(_)));
}

#[tokio::test(start_paused = true)]
async fn unwritten_disconnect_does_not_apply_application_disconnect() {
    let (client, connect_handle) = persistent_client(PacketIdentifier::MAX);
    let mut connection = TestConnection::connect(connect_handle, false, None).await;
    let mut completion = publish_qos1(&client).await;
    connection.drive().await;
    connection.expect_publish(1, false);

    // The DISCONNECT, and its Session Expiry Interval override, never reach the server.
    connection.outgoing_packets_rx.close();
    let (connect_handle, event, _outgoing_packets_rx) =
        connection.disconnect(&expire_session_on_disconnect()).await;
    assert_matches!(event, DisconnectedEvent::IoError(_));
    assert_matches!((&mut completion).now_or_never(), None);

    let mut connection = TestConnection::connect(connect_handle, true, None).await;
    connection.drive().await;
    connection.expect_publish(1, true);
}
