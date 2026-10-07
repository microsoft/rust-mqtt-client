// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// The CONNACK Session Expiry Interval, or the CONNECT value when CONNACK omits it, decides whether
// session state is kept after the connection ends. A client DISCONNECT value replaces it.

use futures_util::future::FutureExt as _;
use matches::assert_matches;
use ms_mqtt_client::client::token::completion::PublishQoS1CompletionToken;
use ms_mqtt_client::client::{ConnectHandle, DisconnectedEvent};
use ms_mqtt_client::packet::{
    ConnectProperties, DisconnectProperties, PacketIdentifier, SessionExpiryInterval,
};
use test_case::test_matrix;

use crate::connection_termination::{
    Termination, TestConnection, expire_session_on_disconnect, persistent_client, publish_qos1,
};

fn assert_expired(completion: PublishQoS1CompletionToken) {
    assert_matches!(completion.now_or_never(), Some(Err(_)));
}

async fn assert_kept(connect_handle: ConnectHandle, completion: &mut PublishQoS1CompletionToken) {
    assert_matches!(completion.now_or_never(), None);

    let mut connection = TestConnection::connect(connect_handle, true, None).await;
    connection.drive().await;
    connection.expect_publish(1, true);
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
async fn default_connect_interval_expires_session_on_termination(termination: Termination) {
    let (client, connect_handle) = persistent_client(PacketIdentifier::MAX);
    let mut connection = TestConnection::connect_with_session_expiry_interval(
        connect_handle,
        false,
        ConnectProperties::default().session_expiry_interval,
        None,
    )
    .await;
    let completion = publish_qos1(&client).await;
    connection.drive().await;
    connection.expect_publish(1, false);

    let _connect_handle = connection.terminate(termination).await;
    assert_expired(completion);
}

#[tokio::test(start_paused = true)]
async fn default_connect_interval_expires_session_on_disconnect() {
    let (client, connect_handle) = persistent_client(PacketIdentifier::MAX);
    let mut connection = TestConnection::connect_with_session_expiry_interval(
        connect_handle,
        false,
        ConnectProperties::default().session_expiry_interval,
        None,
    )
    .await;
    let completion = publish_qos1(&client).await;
    connection.drive().await;
    connection.expect_publish(1, false);

    let (_connect_handle, event, _outgoing_packets_rx) = connection
        .disconnect(&DisconnectProperties::default())
        .await;
    assert_matches!(event, DisconnectedEvent::ApplicationDisconnect);
    assert_expired(completion);
}

#[tokio::test(start_paused = true)]
async fn connack_interval_replaces_connect_interval() {
    let (client, connect_handle) = persistent_client(PacketIdentifier::MAX);
    let mut connection = TestConnection::connect_with_session_expiry_interval(
        connect_handle,
        false,
        SessionExpiryInterval::Duration(0),
        Some(SessionExpiryInterval::Duration(60)),
    )
    .await;
    let mut completion = publish_qos1(&client).await;
    connection.drive().await;
    connection.expect_publish(1, false);

    let connect_handle = connection.terminate(Termination::ReadEof).await;
    assert_kept(connect_handle, &mut completion).await;
}

#[tokio::test(start_paused = true)]
async fn zero_connack_interval_does_not_carry_over() {
    let (client, connect_handle) = persistent_client(PacketIdentifier::MAX);
    let connection = TestConnection::connect(
        connect_handle,
        false,
        Some(SessionExpiryInterval::Duration(0)),
    )
    .await;
    let connect_handle = connection.terminate(Termination::ReadEof).await;

    let mut connection = TestConnection::connect(connect_handle, false, None).await;
    let mut completion = publish_qos1(&client).await;
    connection.drive().await;
    connection.expect_publish(1, false);

    let connect_handle = connection.terminate(Termination::ReadEof).await;
    assert_kept(connect_handle, &mut completion).await;
}

#[tokio::test(start_paused = true)]
async fn zero_disconnect_interval_does_not_carry_over() {
    let (client, connect_handle) = persistent_client(PacketIdentifier::MAX);
    let connection = TestConnection::connect(connect_handle, false, None).await;
    let (connect_handle, event, _outgoing_packets_rx) =
        connection.disconnect(&expire_session_on_disconnect()).await;
    assert_matches!(event, DisconnectedEvent::ApplicationDisconnect);

    let mut connection = TestConnection::connect(connect_handle, false, None).await;
    let mut completion = publish_qos1(&client).await;
    connection.drive().await;
    connection.expect_publish(1, false);

    let connect_handle = connection.terminate(Termination::ReadEof).await;
    assert_kept(connect_handle, &mut completion).await;
}

#[tokio::test(start_paused = true)]
async fn disconnect_interval_replaces_connack_interval() {
    let (client, connect_handle) = persistent_client(PacketIdentifier::MAX);
    let mut connection = TestConnection::connect(
        connect_handle,
        false,
        Some(SessionExpiryInterval::Duration(0)),
    )
    .await;
    let mut completion = publish_qos1(&client).await;
    connection.drive().await;
    connection.expect_publish(1, false);

    let (connect_handle, event, _outgoing_packets_rx) = connection
        .disconnect(&DisconnectProperties {
            session_expiry_interval: Some(SessionExpiryInterval::Duration(60)),
            ..Default::default()
        })
        .await;
    assert_matches!(event, DisconnectedEvent::ApplicationDisconnect);
    assert_kept(connect_handle, &mut completion).await;
}

// Enhanced authentication carries the CONNECT interval through its own handle.
#[tokio::test(start_paused = true)]
async fn enhanced_auth_connect_interval_keeps_session() {
    let (client, connect_handle) = persistent_client(PacketIdentifier::MAX);
    let mut connection =
        TestConnection::connect_enhanced_auth(connect_handle, SessionExpiryInterval::Duration(60))
            .await;
    let mut completion = publish_qos1(&client).await;
    connection.drive().await;
    connection.expect_publish(1, false);

    let connect_handle = connection.terminate(Termination::ReadEof).await;
    assert_kept(connect_handle, &mut completion).await;
}
