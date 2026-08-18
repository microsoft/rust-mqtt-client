// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! MQTT session lifecycle behavior through a live server.

use std::time::Duration;

use bytes::Bytes;
use ms_mqtt_client::client::{
    Client, ConnectHandle, DisconnectedEvent, ManualAcknowledgement, Receiver,
};
use ms_mqtt_client::packet::{
    ConnectProperties, DisconnectProperties, PubAckProperties, Publish, PublishProperties, QoS,
    RetainOptions, SessionExpiryInterval, SubscribeProperties, UnsubscribeProperties,
};
use ms_mqtt_client::topic::{TopicFilter, TopicName};

use crate::common::{
    Endpoint, SessionOptions, TestConnection, connect_tcp, connect_tcp_with_session,
    reconnect_tcp_with_session,
};

const PERSISTENT_SESSION_EXPIRY: u32 = 60;

struct DisconnectedClient {
    client: Client,
    receiver: Receiver,
    connect_handle: ConnectHandle,
}

fn session_options(clean_start: bool, expiry_seconds: u32) -> SessionOptions {
    SessionOptions {
        clean_start,
        properties: ConnectProperties {
            session_expiry_interval: SessionExpiryInterval::Duration(expiry_seconds),
            ..Default::default()
        },
    }
}

async fn disconnect_for_reconnect(connection: TestConnection) -> DisconnectedClient {
    disconnect_for_reconnect_with_properties(connection, DisconnectProperties::default()).await
}

async fn disconnect_for_reconnect_with_properties(
    connection: TestConnection,
    properties: DisconnectProperties,
) -> DisconnectedClient {
    let (client, receiver, connect_handle, event) = connection
        .disconnect_for_reconnect_with_properties(properties)
        .await;
    assert!(matches!(event, DisconnectedEvent::ApplicationDisconnect));
    DisconnectedClient {
        client,
        receiver,
        connect_handle,
    }
}

async fn reconnect(
    endpoint: &Endpoint,
    disconnected: DisconnectedClient,
    clean_start: bool,
    expiry_seconds: u32,
) -> TestConnection {
    reconnect_tcp_with_session(
        disconnected.client,
        disconnected.connect_handle,
        disconnected.receiver,
        endpoint,
        session_options(clean_start, expiry_seconds),
    )
    .await
}

/// Cleanly disconnects and requests immediate expiry of the server-held session.
async fn disconnect_and_end_session(connection: TestConnection) {
    let event = connection
        .disconnect_with_properties(DisconnectProperties {
            session_expiry_interval: Some(SessionExpiryInterval::Duration(0)),
            ..Default::default()
        })
        .await;
    assert!(matches!(event, DisconnectedEvent::ApplicationDisconnect));
}

async fn subscribe_and_expect_success(connection: &TestConnection, topic: &str) {
    let suback = connection
        .client
        .subscribe(
            TopicFilter::new(topic).unwrap(),
            QoS::AtLeastOnce,
            false,
            RetainOptions::default(),
            SubscribeProperties::default(),
        )
        .await
        .expect("subscriber should still be attached")
        .await
        .expect("SUBSCRIBE should complete");
    assert!(suback.is_success(), "server rejected SUBSCRIBE: {suback:?}");
}

async fn publish_qos1_and_expect_success(
    connection: &TestConnection,
    topic: &str,
    payload: Bytes,
    properties: PublishProperties,
) {
    let puback = connection
        .client
        .publish_qos1(TopicName::new(topic).unwrap(), payload, false, properties)
        .await
        .expect("publisher should still be attached")
        .await
        .expect("QoS 1 PUBLISH should complete");
    assert!(puback.is_success(), "server rejected PUBLISH: {puback:?}");
}

async fn receive_qos1_and_ack(connection: &mut TestConnection) -> Publish {
    let (publish, manual_ack) = connection
        .receiver
        .recv()
        .await
        .expect("subscriber should receive the PUBLISH");
    let ManualAcknowledgement::QoS1(acknowledgement) = manual_ack else {
        panic!("QoS 1 PUBLISH should require a PUBACK");
    };
    acknowledgement
        .accept(PubAckProperties::default())
        .await
        .expect("subscriber should still be attached")
        .await
        .expect("PUBACK should be sent");
    publish
}

/// Verifies that reconnecting without a clean start resumes a persistent MQTT session.
#[tokio::test]
async fn persistent_session_is_resumed() {
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let connection = connect_tcp_with_session(
            &endpoint,
            "persistent_session_is_resumed",
            session_options(true, PERSISTENT_SESSION_EXPIRY),
        )
        .await;
        assert!(!connection.connack.session_present);

        let disconnected = disconnect_for_reconnect(connection).await;
        let resumed = reconnect(
            &endpoint,
            disconnected,
            false,
            PERSISTENT_SESSION_EXPIRY,
        )
        .await;
        assert!(resumed.connack.session_present);

        disconnect_and_end_session(resumed).await;
    }
}

/// Verifies that a persistent QoS 1 subscription receives a publication queued while the client
/// is offline.
#[tokio::test]
async fn persistent_subscription_receives_queued_qos1() {
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let subscriber = connect_tcp_with_session(
            &endpoint,
            "persistent_subscription_receives_queued_qos1_subscriber",
            session_options(true, PERSISTENT_SESSION_EXPIRY),
        )
        .await;
        let topic = "ms-mqtt-client/network/session/offline-qos1";
        subscribe_and_expect_success(&subscriber, topic).await;
        let disconnected = disconnect_for_reconnect(subscriber).await;

        let publisher = connect_tcp(
            &endpoint,
            "persistent_subscription_receives_queued_qos1_publisher",
        )
        .await;
        publish_qos1_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(b"queued while subscriber was offline"),
            PublishProperties::default(),
        )
        .await;

        let mut resumed = reconnect(
            &endpoint,
            disconnected,
            false,
            PERSISTENT_SESSION_EXPIRY,
        )
        .await;
        assert!(resumed.connack.session_present);
        let publish = receive_qos1_and_ack(&mut resumed).await;
        assert_eq!(publish.topic_name, TopicName::new(topic).unwrap());
        assert_eq!(
            publish.payload,
            Bytes::from_static(b"queued while subscriber was offline")
        );

        disconnect_and_end_session(resumed).await;
        assert!(matches!(
            publisher.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
    }
}

/// Verifies that a clean start discards an existing session and its subscriptions.
#[tokio::test]
async fn clean_start_discards_existing_session() {
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let subscriber = connect_tcp_with_session(
            &endpoint,
            "clean_start_discards_existing_session_subscriber",
            session_options(true, PERSISTENT_SESSION_EXPIRY),
        )
        .await;
        let topic = "ms-mqtt-client/network/session/clean-start";
        subscribe_and_expect_success(&subscriber, topic).await;
        let disconnected = disconnect_for_reconnect(subscriber).await;

        let mut restarted = reconnect(
            &endpoint,
            disconnected,
            true,
            PERSISTENT_SESSION_EXPIRY,
        )
        .await;
        assert!(!restarted.connack.session_present);

        let publisher = connect_tcp(
            &endpoint,
            "clean_start_discards_existing_session_publisher",
        )
        .await;
        publish_qos1_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(b"discarded subscription"),
            PublishProperties::default(),
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(500), restarted.receiver.recv())
                .await
                .is_err(),
            "discarded subscription received a PUBLISH"
        );

        disconnect_and_end_session(restarted).await;
        assert!(matches!(
            publisher.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
    }
}

/// Verifies that a zero session expiry prevents session state from surviving disconnect.
#[tokio::test]
async fn zero_expiry_does_not_preserve_session() {
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let connection = connect_tcp_with_session(
            &endpoint,
            "zero_expiry_does_not_preserve_session",
            session_options(true, 0),
        )
        .await;
        let disconnected = disconnect_for_reconnect(connection).await;

        let reconnected = reconnect(&endpoint, disconnected, false, 0).await;
        assert!(!reconnected.connack.session_present);

        disconnect_and_end_session(reconnected).await;
    }
}

/// Verifies that DISCONNECT can reduce a nonzero session expiry to zero and delete the session.
#[tokio::test]
async fn disconnect_can_delete_persistent_session() {
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let connection = connect_tcp_with_session(
            &endpoint,
            "disconnect_can_delete_persistent_session",
            session_options(true, PERSISTENT_SESSION_EXPIRY),
        )
        .await;
        let disconnected = disconnect_for_reconnect_with_properties(
            connection,
            DisconnectProperties {
                session_expiry_interval: Some(SessionExpiryInterval::Duration(0)),
                ..Default::default()
            },
        )
        .await;

        // Some servers (e.g. HiveMQ CE) mark a session for deletion asynchronously after
        // processing the DISCONNECT that overrides its expiry to zero, so a reconnect that
        // races the deletion can still observe the still-present session. Give the server a
        // brief moment to complete the deletion before reconnecting.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let reconnected = reconnect(
            &endpoint,
            disconnected,
            false,
            PERSISTENT_SESSION_EXPIRY,
        )
        .await;
        assert!(!reconnected.connack.session_present);

        disconnect_and_end_session(reconnected).await;
    }
}

/// Verifies that the server deletes a disconnected session after its expiry interval elapses.
#[tokio::test]
async fn session_expires_after_interval() {
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let connection = connect_tcp_with_session(
            &endpoint,
            "session_expires_after_interval",
            session_options(true, 1),
        )
        .await;
        let disconnected = disconnect_for_reconnect(connection).await;

        tokio::time::sleep(Duration::from_secs(2)).await;
        let reconnected = reconnect(
            &endpoint,
            disconnected,
            false,
            PERSISTENT_SESSION_EXPIRY,
        )
        .await;
        assert!(!reconnected.connack.session_present);

        disconnect_and_end_session(reconnected).await;
    }
}

/// Verifies that QoS 1 publications queued for an offline persistent session retain their
/// original order when the subscriber resumes.
#[tokio::test]
async fn queued_qos1_messages_preserve_order() {
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let subscriber = connect_tcp_with_session(
            &endpoint,
            "queued_qos1_messages_preserve_order_subscriber",
            session_options(true, PERSISTENT_SESSION_EXPIRY),
        )
        .await;
        let topic = "ms-mqtt-client/network/session/queued-order";
        subscribe_and_expect_success(&subscriber, topic).await;
        let disconnected = disconnect_for_reconnect(subscriber).await;

        let publisher = connect_tcp(
            &endpoint,
            "queued_qos1_messages_preserve_order_publisher",
        )
        .await;
        for sequence in 0..5 {
            publish_qos1_and_expect_success(
                &publisher,
                topic,
                Bytes::from(format!("message-{sequence}")),
                PublishProperties::default(),
            )
            .await;
        }

        let mut resumed = reconnect(
            &endpoint,
            disconnected,
            false,
            PERSISTENT_SESSION_EXPIRY,
        )
        .await;
        assert!(resumed.connack.session_present);
        for sequence in 0..5 {
            let publish = receive_qos1_and_ack(&mut resumed).await;
            assert_eq!(publish.payload, Bytes::from(format!("message-{sequence}")));
        }

        disconnect_and_end_session(resumed).await;
        assert!(matches!(
            publisher.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
    }
}

/// Verifies that a queued publication is discarded when its message expiry interval elapses
/// before the persistent subscriber resumes.
#[tokio::test]
async fn expired_queued_message_is_not_delivered() {
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let subscriber = connect_tcp_with_session(
            &endpoint,
            "expired_queued_message_is_not_delivered_subscriber",
            session_options(true, PERSISTENT_SESSION_EXPIRY),
        )
        .await;
        let topic = "ms-mqtt-client/network/session/expired-message";
        subscribe_and_expect_success(&subscriber, topic).await;
        let disconnected = disconnect_for_reconnect(subscriber).await;

        let publisher = connect_tcp(
            &endpoint,
            "expired_queued_message_is_not_delivered_publisher",
        )
        .await;
        publish_qos1_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(b"expires while offline"),
            PublishProperties {
                message_expiry_interval: Some(1),
                ..Default::default()
            },
        )
        .await;

        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut resumed = reconnect(
            &endpoint,
            disconnected,
            false,
            PERSISTENT_SESSION_EXPIRY,
        )
        .await;
        assert!(resumed.connack.session_present);
        assert!(
            tokio::time::timeout(Duration::from_millis(500), resumed.receiver.recv())
                .await
                .is_err(),
            "subscriber received a queued PUBLISH after its message expiry interval"
        );

        disconnect_and_end_session(resumed).await;
        assert!(matches!(
            publisher.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
    }
}

/// Verifies that an unsubscribe remains effective after a persistent session is resumed.
#[tokio::test]
async fn unsubscribe_is_preserved_across_resume() {
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let subscriber = connect_tcp_with_session(
            &endpoint,
            "unsubscribe_is_preserved_across_resume_subscriber",
            session_options(true, PERSISTENT_SESSION_EXPIRY),
        )
        .await;
        let topic = "ms-mqtt-client/network/session/unsubscribe";
        subscribe_and_expect_success(&subscriber, topic).await;
        let unsuback = subscriber
            .client
            .unsubscribe(
                TopicFilter::new(topic).unwrap(),
                UnsubscribeProperties::default(),
            )
            .await
            .expect("subscriber should still be attached")
            .await
            .expect("UNSUBSCRIBE should complete");
        assert!(
            unsuback.is_success(),
            "server rejected UNSUBSCRIBE: {unsuback:?}"
        );
        let disconnected = disconnect_for_reconnect(subscriber).await;

        let mut resumed = reconnect(
            &endpoint,
            disconnected,
            false,
            PERSISTENT_SESSION_EXPIRY,
        )
        .await;
        assert!(resumed.connack.session_present);
        let publisher = connect_tcp(
            &endpoint,
            "unsubscribe_is_preserved_across_resume_publisher",
        )
        .await;
        publish_qos1_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(b"must not be delivered"),
            PublishProperties::default(),
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(500), resumed.receiver.recv())
                .await
                .is_err(),
            "resumed session restored an unsubscribed topic"
        );

        disconnect_and_end_session(resumed).await;
        assert!(matches!(
            publisher.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
    }
}

// TODO: Verify persistent-session resume after an ungraceful transport close.
// TODO: Verify that resuming before the Will Delay expires suppresses the Will.
// TODO: Verify Will publication when session expiry occurs before the Will Delay.
// TODO: Verify duplicate-client-ID takeover and the displaced client's disconnect reason.
