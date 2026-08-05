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
use crate::common::{Endpoint, RunningConnection, connect_tcp, with_timeout};

const DEFAULT_PORT: u16 = 1883;

async fn connect_pair(test_name: &str) -> (RunningConnection, RunningConnection) {
    let endpoint = Endpoint::from_env(DEFAULT_PORT);
    let subscriber = connect_tcp(&endpoint, &format!("{test_name}_subscriber"))
        .await
        .start();
    let publisher = connect_tcp(&endpoint, &format!("{test_name}_publisher"))
        .await
        .start();
    (subscriber, publisher)
}

async fn subscribe(subscriber: &RunningConnection, filter: &str, qos: QoS) {
    subscribe_with_options(
        subscriber,
        filter,
        qos,
        false,
        SubscribeProperties::default(),
    )
    .await;
}

async fn subscribe_with_options(
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
        "broker rejected the subscription: {suback:?}"
    );
}

async fn publish_qos0(
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

async fn publish_qos1(
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
        "broker rejected the PUBLISH: {puback:?}"
    );
}

async fn receive_qos1(subscriber: &mut RunningConnection) -> Publish {
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

async fn disconnect_pair(subscriber: RunningConnection, publisher: RunningConnection) {
    assert!(matches!(
        subscriber.disconnect().await,
        DisconnectedEvent::ApplicationDisconnect
    ));
    assert!(matches!(
        publisher.disconnect().await,
        DisconnectedEvent::ApplicationDisconnect
    ));
}

#[tokio::test]
async fn publish_receive_qos0() {
    with_timeout(Box::pin(async {
        let (mut subscriber, publisher) = connect_pair("publish_receive_qos0").await;
        let topic = "ms-mqtt-client/network/qos0";
        subscribe(&subscriber, topic, QoS::AtMostOnce).await;

        publish_qos0(
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

        disconnect_pair(subscriber, publisher).await;
    }))
    .await;
}

#[tokio::test]
async fn publish_receive_qos1() {
    with_timeout(Box::pin(async {
        let (mut subscriber, publisher) = connect_pair("publish_receive_qos1").await;
        let topic = "ms-mqtt-client/network/qos1";
        subscribe(&subscriber, topic, QoS::AtLeastOnce).await;

        publish_qos1(
            &publisher,
            topic,
            Bytes::from_static(b"qos1 payload"),
            PublishProperties::default(),
        )
        .await;

        let publish = receive_qos1(&mut subscriber).await;
        assert_eq!(publish.topic_name, TopicName::new(topic).unwrap());
        assert_eq!(publish.payload, Bytes::from_static(b"qos1 payload"));
        assert!(matches!(publish.qos, DeliveryQoS::AtLeastOnce(_)));

        disconnect_pair(subscriber, publisher).await;
    }))
    .await;
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    with_timeout(Box::pin(async {
        let (mut subscriber, publisher) = connect_pair("unsubscribe_stops_delivery").await;
        let topic = "ms-mqtt-client/network/unsubscribe";
        subscribe(&subscriber, topic, QoS::AtLeastOnce).await;

        publish_qos1(
            &publisher,
            topic,
            Bytes::from_static(b"before unsubscribe"),
            PublishProperties::default(),
        )
        .await;
        assert_eq!(
            receive_qos1(&mut subscriber).await.payload,
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
            "broker rejected the unsubscription: {unsuback:?}"
        );

        publish_qos1(
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

        disconnect_pair(subscriber, publisher).await;
    }))
    .await;
}

#[tokio::test]
async fn publish_properties_round_trip() {
    with_timeout(Box::pin(async {
        let (mut subscriber, publisher) = connect_pair("publish_properties_round_trip").await;
        let topic = "ms-mqtt-client/network/properties";
        subscribe(&subscriber, topic, QoS::AtLeastOnce).await;
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

        publish_qos1(
            &publisher,
            topic,
            Bytes::from_static(br#"{"value":42}"#),
            properties.clone(),
        )
        .await;
        let publish = receive_qos1(&mut subscriber).await;
        assert_eq!(publish.properties, properties);

        disconnect_pair(subscriber, publisher).await;
    }))
    .await;
}

#[tokio::test]
async fn wildcard_subscriptions_route_matching_topics() {
    crate::require_feature!(Feature::WildcardSubscriptions);
    with_timeout(Box::pin(async {
        let (mut subscriber, publisher) =
            connect_pair("wildcard_subscriptions_route_matching_topics").await;
        subscribe(
            &subscriber,
            "ms-mqtt-client/network/wildcard/+/reading",
            QoS::AtMostOnce,
        )
        .await;
        subscribe(
            &subscriber,
            "ms-mqtt-client/network/wildcard/events/#",
            QoS::AtMostOnce,
        )
        .await;

        let plus_topic = "ms-mqtt-client/network/wildcard/device-1/reading";
        publish_qos0(
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
        publish_qos0(
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

        disconnect_pair(subscriber, publisher).await;
    }))
    .await;
}

#[tokio::test]
async fn no_local_suppresses_self_publish() {
    with_timeout(Box::pin(async {
        let (mut subscriber, publisher) = connect_pair("no_local_suppresses_self_publish").await;
        let topic = "ms-mqtt-client/network/no-local";
        subscribe_with_options(
            &subscriber,
            topic,
            QoS::AtLeastOnce,
            true,
            SubscribeProperties::default(),
        )
        .await;

        publish_qos1(
            &publisher,
            topic,
            Bytes::from_static(b"remote"),
            PublishProperties::default(),
        )
        .await;
        assert_eq!(
            receive_qos1(&mut subscriber).await.payload,
            Bytes::from_static(b"remote")
        );

        publish_qos1(
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

        disconnect_pair(subscriber, publisher).await;
    }))
    .await;
}
