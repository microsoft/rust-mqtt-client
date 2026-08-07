// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! End-to-end publication routing through a live MQTT server.

use std::time::Duration;

use bytes::Bytes;
use ms_mqtt_client::client::{DisconnectedEvent, ManualAcknowledgement};
use ms_mqtt_client::packet::{
    DeliveryQoS, PayloadFormatIndicator, PubAckProperties, Publish, PublishProperties, QoS,
    RetainOptions, SubscribeProperties, UnsubscribeProperties,
};
use ms_mqtt_client::topic::{TopicFilter, TopicName};

use crate::common::capabilities::Feature;
use crate::common::{Endpoint, RunningConnection, connect_tcp};

const DEFAULT_PORT: u16 = 1883;

async fn connect_pair_and_start(test_name: &str) -> (RunningConnection, RunningConnection) {
    let endpoint = Endpoint::from_env(DEFAULT_PORT);
    let subscriber = connect_tcp(&endpoint, &format!("{test_name}_subscriber"))
        .await
        .start();
    let publisher = connect_tcp(&endpoint, &format!("{test_name}_publisher"))
        .await
        .start();
    (subscriber, publisher)
}

async fn subscribe_and_expect_success(subscriber: &RunningConnection, filter: &str, qos: QoS) {
    subscribe_with_options_and_expect_success(
        subscriber,
        filter,
        qos,
        false,
        SubscribeProperties::default(),
    )
    .await;
}

async fn subscribe_with_options_and_expect_success(
    subscriber: &RunningConnection,
    filter: &str,
    qos: QoS,
    no_local: bool,
    properties: SubscribeProperties,
) {
    let suback = subscriber
        .client
        .subscribe(
            TopicFilter::new(filter).unwrap(),
            qos,
            no_local,
            RetainOptions::default(),
            properties,
        )
        .await
        .expect("subscriber should still be attached")
        .await
        .expect("SUBSCRIBE should complete");
    assert!(
        suback.is_success(),
        "server rejected the subscription: {suback:?}"
    );
}

async fn publish_qos0_and_expect_success(
    publisher: &RunningConnection,
    topic: &str,
    payload: Bytes,
    properties: PublishProperties,
) {
    publisher
        .client
        .publish_qos0(TopicName::new(topic).unwrap(), payload, false, properties)
        .await
        .expect("publisher should still be attached")
        .await
        .expect("QoS 0 PUBLISH should be sent");
}

async fn publish_qos1_and_expect_success(
    publisher: &RunningConnection,
    topic: &str,
    payload: Bytes,
    properties: PublishProperties,
) {
    let puback = publisher
        .client
        .publish_qos1(TopicName::new(topic).unwrap(), payload, false, properties)
        .await
        .expect("publisher should still be attached")
        .await
        .expect("QoS 1 PUBLISH should complete");
    assert!(
        puback.is_success(),
        "server rejected the PUBLISH: {puback:?}"
    );
}

async fn receive_qos1_and_ack(subscriber: &mut RunningConnection) -> Publish {
    let (publish, acknowledgement) = subscriber
        .receiver
        .recv()
        .await
        .expect("subscriber should receive the PUBLISH");
    let ManualAcknowledgement::QoS1(acknowledgement) = acknowledgement else {
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

async fn disconnect_pair_and_expect_application_disconnect(
    subscriber: RunningConnection,
    publisher: RunningConnection,
) {
    assert!(matches!(
        subscriber.disconnect().await,
        DisconnectedEvent::ApplicationDisconnect
    ));
    assert!(matches!(
        publisher.disconnect().await,
        DisconnectedEvent::ApplicationDisconnect
    ));
}

/// Verifies that a QoS 0 publication is routed to a subscriber with the expected topic,
/// payload, delivery QoS, and no acknowledgement token.
#[tokio::test]
async fn publish_receive_qos0() {
    crate::test_timeout! {
        let (mut subscriber, publisher) = connect_pair_and_start("publish_receive_qos0").await;
        let topic = "ms-mqtt-client/network/qos0";
        subscribe_and_expect_success(&subscriber, topic, QoS::AtMostOnce).await;

        publish_qos0_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(b"qos0 payload"),
            PublishProperties::default(),
        )
        .await;

        let (publish, acknowledgement) = subscriber
            .receiver
            .recv()
            .await
            .expect("subscriber should receive the PUBLISH");
        assert_eq!(publish.topic_name, TopicName::new(topic).unwrap());
        assert_eq!(publish.payload, Bytes::from_static(b"qos0 payload"));
        assert_eq!(publish.qos, DeliveryQoS::AtMostOnce);
        assert!(matches!(acknowledgement, ManualAcknowledgement::QoS0));

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}

/// Verifies end-to-end QoS 1 delivery, including the publisher's PUBACK and the subscriber's
/// explicit acknowledgement of the received publication.
#[tokio::test]
async fn publish_receive_qos1() {
    crate::test_timeout! {
        let (mut subscriber, publisher) = connect_pair_and_start("publish_receive_qos1").await;
        let topic = "ms-mqtt-client/network/qos1";
        subscribe_and_expect_success(&subscriber, topic, QoS::AtLeastOnce).await;

        publish_qos1_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(b"qos1 payload"),
            PublishProperties::default(),
        )
        .await;

        let publish = receive_qos1_and_ack(&mut subscriber).await;
        assert_eq!(publish.topic_name, TopicName::new(topic).unwrap());
        assert_eq!(publish.payload, Bytes::from_static(b"qos1 payload"));
        assert!(matches!(publish.qos, DeliveryQoS::AtLeastOnce(_)));

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}

/// Verifies that a successful UNSUBSCRIBE stops subsequent publications from reaching the
/// former subscriber.
#[tokio::test]
async fn unsubscribe_stops_delivery() {
    crate::test_timeout! {
        let (mut subscriber, publisher) =
            connect_pair_and_start("unsubscribe_stops_delivery").await;
        let topic = "ms-mqtt-client/network/unsubscribe";
        subscribe_and_expect_success(&subscriber, topic, QoS::AtLeastOnce).await;

        publish_qos1_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(b"before unsubscribe"),
            PublishProperties::default(),
        )
        .await;
        assert_eq!(
            receive_qos1_and_ack(&mut subscriber).await.payload,
            Bytes::from_static(b"before unsubscribe")
        );

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
            "server rejected the unsubscription: {unsuback:?}"
        );

        publish_qos1_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(b"after unsubscribe"),
            PublishProperties::default(),
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(500), subscriber.receiver.recv())
                .await
                .is_err(),
            "subscriber received a PUBLISH after unsubscribing"
        );

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}

/// Verifies that MQTT 5 publication metadata survives an end-to-end server round trip.
#[tokio::test]
async fn publish_properties_round_trip() {
    crate::test_timeout! {
        let (mut subscriber, publisher) =
            connect_pair_and_start("publish_properties_round_trip").await;
        let topic = "ms-mqtt-client/network/properties";
        subscribe_and_expect_success(&subscriber, topic, QoS::AtLeastOnce).await;
        let properties = PublishProperties {
            payload_format_indicator: PayloadFormatIndicator::UTF8,
            response_topic: Some(
                TopicName::new("ms-mqtt-client/network/properties/response").unwrap(),
            ),
            correlation_data: Some(Bytes::from_static(b"correlation-42")),
            user_properties: vec![
                ("source".to_string(), "network-suite".to_string()),
                ("sequence".to_string(), "42".to_string()),
            ],
            content_type: Some("application/json".to_string()),
            ..Default::default()
        };

        publish_qos1_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(br#"{"value":42}"#),
            properties.clone(),
        )
        .await;
        let publish = receive_qos1_and_ack(&mut subscriber).await;
        assert_eq!(publish.properties, properties);

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}

/// Verifies that single-level (`+`) and multi-level (`#`) wildcard subscriptions route
/// publications from matching topics.
#[tokio::test]
async fn wildcard_subscriptions_route_matching_topics() {
    crate::require_server_feature!(Feature::WildcardSubscriptions);
    crate::test_timeout! {
        let (mut subscriber, publisher) =
            connect_pair_and_start("wildcard_subscriptions_route_matching_topics").await;
        subscribe_and_expect_success(
            &subscriber,
            "ms-mqtt-client/network/wildcard/+/reading",
            QoS::AtMostOnce,
        )
        .await;
        subscribe_and_expect_success(
            &subscriber,
            "ms-mqtt-client/network/wildcard/events/#",
            QoS::AtMostOnce,
        )
        .await;

        let plus_topic = "ms-mqtt-client/network/wildcard/device-1/reading";
        publish_qos0_and_expect_success(
            &publisher,
            plus_topic,
            Bytes::from_static(b"plus"),
            PublishProperties::default(),
        )
        .await;
        let (plus_publish, _) = subscriber
            .receiver
            .recv()
            .await
            .expect("+ subscription should receive a matching PUBLISH");
        assert_eq!(plus_publish.topic_name, TopicName::new(plus_topic).unwrap());

        let hash_topic = "ms-mqtt-client/network/wildcard/events/site/online";
        publish_qos0_and_expect_success(
            &publisher,
            hash_topic,
            Bytes::from_static(b"hash"),
            PublishProperties::default(),
        )
        .await;
        let (hash_publish, _) = subscriber
            .receiver
            .recv()
            .await
            .expect("# subscription should receive a matching PUBLISH");
        assert_eq!(hash_publish.topic_name, TopicName::new(hash_topic).unwrap());

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}

/// Verifies that a no-local subscription receives remote publications but suppresses
/// publications sent by the same client.
#[tokio::test]
async fn no_local_suppresses_self_publish() {
    crate::test_timeout! {
        let (mut subscriber, publisher) =
            connect_pair_and_start("no_local_suppresses_self_publish").await;
        let topic = "ms-mqtt-client/network/no-local";
        subscribe_with_options_and_expect_success(
            &subscriber,
            topic,
            QoS::AtLeastOnce,
            true,
            SubscribeProperties::default(),
        )
        .await;

        publish_qos1_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(b"remote"),
            PublishProperties::default(),
        )
        .await;
        assert_eq!(
            receive_qos1_and_ack(&mut subscriber).await.payload,
            Bytes::from_static(b"remote")
        );

        publish_qos1_and_expect_success(
            &subscriber,
            topic,
            Bytes::from_static(b"local"),
            PublishProperties::default(),
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(500), subscriber.receiver.recv())
                .await
                .is_err(),
            "no-local subscription received its own PUBLISH"
        );

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}
