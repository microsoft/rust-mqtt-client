// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! End-to-end MQTT messaging behavior through a live server.

use std::num::NonZeroU32;
use std::time::Duration;

use bytes::Bytes;
use ms_mqtt_client::client::{DisconnectedEvent, ManualAcknowledgement};
use ms_mqtt_client::packet::{
    DeliveryQoS, PayloadFormatIndicator, PubAckProperties, Publish, PublishProperties, QoS,
    RetainHandling, RetainOptions, SubscribeProperties, UnsubscribeProperties, Will,
    WillProperties,
};
use ms_mqtt_client::topic::{TopicFilter, TopicName};

use crate::common::server::ServerFeature;
use crate::common::{Endpoint, TestConnection, connect_tcp, connect_tcp_with_will};

async fn connect_pair(test_name: &str) -> (TestConnection, TestConnection) {
    let endpoint = Endpoint::from_env();
    let subscriber = connect_tcp(&endpoint, &format!("{test_name}_subscriber")).await;
    let publisher = connect_tcp(&endpoint, &format!("{test_name}_publisher")).await;
    (subscriber, publisher)
}

async fn subscribe_and_expect_success(subscriber: &TestConnection, filter: &str, qos: QoS) {
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
    subscriber: &TestConnection,
    filter: &str,
    qos: QoS,
    no_local: bool,
    properties: SubscribeProperties,
) {
    subscribe_with_retain_options_and_expect_success(
        subscriber,
        filter,
        qos,
        no_local,
        RetainOptions::default(),
        properties,
    )
    .await;
}

async fn subscribe_with_retain_options_and_expect_success(
    subscriber: &TestConnection,
    filter: &str,
    qos: QoS,
    no_local: bool,
    retain_options: RetainOptions,
    properties: SubscribeProperties,
) {
    let suback = subscriber
        .client
        .subscribe(
            TopicFilter::new(filter).unwrap(),
            qos,
            no_local,
            retain_options,
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
    publisher: &TestConnection,
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
    publisher: &TestConnection,
    topic: &str,
    payload: Bytes,
    properties: PublishProperties,
) {
    publish_qos1_with_retain_and_expect_success(publisher, topic, payload, false, properties).await;
}

async fn publish_qos1_with_retain_and_expect_success(
    publisher: &TestConnection,
    topic: &str,
    payload: Bytes,
    retain: bool,
    properties: PublishProperties,
) {
    let puback = publisher
        .client
        .publish_qos1(TopicName::new(topic).unwrap(), payload, retain, properties)
        .await
        .expect("publisher should still be attached")
        .await
        .expect("QoS 1 PUBLISH should complete");
    assert!(
        puback.is_success(),
        "server rejected the PUBLISH: {puback:?}"
    );
}

async fn receive_qos1_and_ack(subscriber: &mut TestConnection) -> Publish {
    let (publish, manual_ack) = subscriber
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

async fn disconnect_pair_and_expect_application_disconnect(
    subscriber: TestConnection,
    publisher: TestConnection,
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
        let (mut subscriber, publisher) = connect_pair("publish_receive_qos0").await;
        let topic = "ms-mqtt-client/network/qos0";
        subscribe_and_expect_success(&subscriber, topic, QoS::AtMostOnce).await;

        publish_qos0_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(b"qos0 payload"),
            PublishProperties::default(),
        )
        .await;

        let (publish, manual_ack) = subscriber
            .receiver
            .recv()
            .await
            .expect("subscriber should receive the PUBLISH");
        assert_eq!(publish.topic_name, TopicName::new(topic).unwrap());
        assert_eq!(publish.payload, Bytes::from_static(b"qos0 payload"));
        assert_eq!(publish.qos, DeliveryQoS::AtMostOnce);
        assert!(matches!(manual_ack, ManualAcknowledgement::QoS0));

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}

/// Verifies end-to-end QoS 1 delivery, including the publisher's PUBACK and the subscriber's
/// explicit acknowledgement of the received publication.
#[tokio::test]
async fn publish_receive_qos1() {
    crate::test_timeout! {
        let (mut subscriber, publisher) = connect_pair("publish_receive_qos1").await;
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
            connect_pair("unsubscribe_stops_delivery").await;
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
            connect_pair("publish_properties_round_trip").await;
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
    crate::require_server_feature!(ServerFeature::WildcardSubscriptions);
    crate::test_timeout! {
        let (mut subscriber, publisher) =
            connect_pair("wildcard_subscriptions_route_matching_topics").await;
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
            connect_pair("no_local_suppresses_self_publish").await;
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

/// Verifies that a subscription identifier supplied in SUBSCRIBE is returned on a matching
/// publication delivered by a supporting server.
#[tokio::test]
async fn subscribe_with_subscription_identifier() {
    crate::require_server_feature!(ServerFeature::SubscriptionIdentifiers);
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let mut connection =
            connect_tcp(&endpoint, "subscribe_with_subscription_identifier").await;
        let topic = "ms-mqtt-client/network/subid";

        subscribe_with_options_and_expect_success(
            &connection,
            topic,
            QoS::AtLeastOnce,
            false,
            SubscribeProperties {
                subscription_identifier: Some(NonZeroU32::new(1).unwrap()),
                ..Default::default()
            },
        )
        .await;

        publish_qos0_and_expect_success(
            &connection,
            topic,
            Bytes::from_static(b"subscription identifier"),
            PublishProperties::default(),
        )
        .await;
        let (publish, manual_ack) = connection
            .receiver
            .recv()
            .await
            .expect("subscription should receive the PUBLISH");
        assert_eq!(
            publish.properties.subscription_identifiers,
            vec![NonZeroU32::new(1).unwrap()]
        );
        assert!(matches!(manual_ack, ManualAcknowledgement::QoS0));

        assert!(matches!(
            connection.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
    }
}

/// Verifies that delivery QoS is the lower of the publication QoS and the subscription's
/// maximum QoS.
#[tokio::test]
async fn delivery_qos_is_negotiated() {
    crate::test_timeout! {
        let (mut subscriber, publisher) = connect_pair("delivery_qos_is_negotiated").await;
        let qos0_subscription_topic = "ms-mqtt-client/network/qos-negotiation/qos0-subscription";
        subscribe_and_expect_success(&subscriber, qos0_subscription_topic, QoS::AtMostOnce).await;

        publish_qos1_and_expect_success(
            &publisher,
            qos0_subscription_topic,
            Bytes::from_static(b"qos 1 publication"),
            PublishProperties::default(),
        )
        .await;
        let (publish, manual_ack) = subscriber
            .receiver
            .recv()
            .await
            .expect("subscriber should receive the QoS 1 publication at QoS 0");
        assert_eq!(publish.qos, DeliveryQoS::AtMostOnce);
        assert!(matches!(manual_ack, ManualAcknowledgement::QoS0));

        let qos1_subscription_topic = "ms-mqtt-client/network/qos-negotiation/qos1-subscription";
        subscribe_and_expect_success(&subscriber, qos1_subscription_topic, QoS::AtLeastOnce).await;
        publish_qos0_and_expect_success(
            &publisher,
            qos1_subscription_topic,
            Bytes::from_static(b"qos 0 publication"),
            PublishProperties::default(),
        )
        .await;
        let (publish, manual_ack) = subscriber
            .receiver
            .recv()
            .await
            .expect("subscriber should receive the QoS 0 publication at QoS 0");
        assert_eq!(publish.qos, DeliveryQoS::AtMostOnce);
        assert!(matches!(manual_ack, ManualAcknowledgement::QoS0));

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}

/// Verifies that a retained publication is delivered to a later subscriber with its retained
/// payload and flag.
#[tokio::test]
async fn retained_publication_is_delivered_to_late_subscriber() {
    crate::test_timeout! {
        let (mut subscriber, publisher) =
            connect_pair("retained_publication_is_delivered_to_late_subscriber").await;
        let topic = "ms-mqtt-client/network/retained/late-subscriber";
        publish_qos1_with_retain_and_expect_success(
            &publisher,
            topic,
            Bytes::from_static(b"retained payload"),
            true,
            PublishProperties::default(),
        )
        .await;

        subscribe_and_expect_success(&subscriber, topic, QoS::AtLeastOnce).await;
        let publish = receive_qos1_and_ack(&mut subscriber).await;
        assert_eq!(publish.payload, Bytes::from_static(b"retained payload"));
        assert!(publish.retain);

        assert!(matches!(
            subscriber.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
        publish_qos1_with_retain_and_expect_success(
            &publisher,
            topic,
            Bytes::new(),
            true,
            PublishProperties::default(),
        )
        .await;
        assert!(matches!(
            publisher.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
    }
}

/// Verifies that retained-message subscription options suppress delivery when requested and
/// send a retained message only for a newly created subscription.
#[tokio::test]
async fn retain_handling_options_control_delivery() {
    crate::test_timeout! {
        let (mut subscriber, publisher) =
            connect_pair("retain_handling_options_control_delivery").await;
        let do_not_send_topic = "ms-mqtt-client/network/retained/do-not-send";
        let new_subscription_topic = "ms-mqtt-client/network/retained/new-subscription";

        for topic in [do_not_send_topic, new_subscription_topic] {
            publish_qos1_with_retain_and_expect_success(
                &publisher,
                topic,
                Bytes::from_static(b"retained payload"),
                true,
                PublishProperties::default(),
            )
            .await;
        }

        subscribe_with_retain_options_and_expect_success(
            &subscriber,
            do_not_send_topic,
            QoS::AtLeastOnce,
            false,
            RetainOptions {
                retain_as_published: true,
                retain_handling: RetainHandling::DoNotSend,
            },
            SubscribeProperties::default(),
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(500), subscriber.receiver.recv())
                .await
                .is_err(),
            "DoNotSend subscription received a retained publication"
        );

        let send_only_if_new = RetainOptions {
            retain_as_published: true,
            retain_handling: RetainHandling::SendOnlyIfSubscriptionDoesNotCurrentlyExist,
        };
        subscribe_with_retain_options_and_expect_success(
            &subscriber,
            new_subscription_topic,
            QoS::AtLeastOnce,
            false,
            send_only_if_new.clone(),
            SubscribeProperties::default(),
        )
        .await;
        assert_eq!(
            receive_qos1_and_ack(&mut subscriber).await.payload,
            Bytes::from_static(b"retained payload")
        );

        subscribe_with_retain_options_and_expect_success(
            &subscriber,
            new_subscription_topic,
            QoS::AtLeastOnce,
            false,
            send_only_if_new,
            SubscribeProperties::default(),
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(500), subscriber.receiver.recv())
                .await
                .is_err(),
            "existing subscription received the retained publication again"
        );

        assert!(matches!(
            subscriber.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
        for topic in [do_not_send_topic, new_subscription_topic] {
            publish_qos1_with_retain_and_expect_success(
                &publisher,
                topic,
                Bytes::new(),
                true,
                PublishProperties::default(),
            )
            .await;
        }
        assert!(matches!(
            publisher.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
    }
}

/// Verifies that wildcard subscriptions do not route topics with missing or extra levels, or
/// topics from an unrelated branch.
#[tokio::test]
async fn wildcard_subscriptions_do_not_route_non_matching_topics() {
    crate::require_server_feature!(ServerFeature::WildcardSubscriptions);
    crate::test_timeout! {
        let (mut subscriber, publisher) =
            connect_pair("wildcard_subscriptions_do_not_route_non_matching_topics").await;
        subscribe_and_expect_success(
            &subscriber,
            "ms-mqtt-client/network/wildcard-nonmatch/+/reading",
            QoS::AtMostOnce,
        )
        .await;
        subscribe_and_expect_success(
            &subscriber,
            "ms-mqtt-client/network/wildcard-nonmatch/events/#",
            QoS::AtMostOnce,
        )
        .await;

        for topic in [
            "ms-mqtt-client/network/wildcard-nonmatch/reading",
            "ms-mqtt-client/network/wildcard-nonmatch/device/site/reading",
            "ms-mqtt-client/network/wildcard-nonmatch/event/site/online",
        ] {
            publish_qos0_and_expect_success(
                &publisher,
                topic,
                Bytes::from_static(b"must not be delivered"),
                PublishProperties::default(),
            )
            .await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(500), subscriber.receiver.recv())
                .await
                .is_err(),
            "wildcard subscription received a non-matching publication"
        );

        disconnect_pair_and_expect_application_disconnect(subscriber, publisher).await;
    }
}

/// Verifies that an ungraceful connection close publishes the configured Will and a clean MQTT
/// disconnect suppresses it.
#[tokio::test]
async fn will_is_published_only_after_ungraceful_disconnect() {
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let mut subscriber = connect_tcp(&endpoint, "will_messages_subscriber").await;
        let topic = "ms-mqtt-client/network/will";
        subscribe_and_expect_success(&subscriber, topic, QoS::AtLeastOnce).await;

        let will = Will {
            topic_name: TopicName::new(topic).unwrap(),
            qos: QoS::AtLeastOnce,
            retain: false,
            payload: Bytes::from_static(b"unexpected disconnect"),
            properties: WillProperties {
                delay_interval: 0,
                payload_format_indicator: PayloadFormatIndicator::Unspecified,
                message_expiry_interval: None,
                content_type: None,
                response_topic: None,
                correlation_data: None,
                user_properties: Vec::new(),
            },
        };
        let ungraceful =
            connect_tcp_with_will(&endpoint, "will_messages_ungraceful", will.clone()).await;
        drop(ungraceful);

        let publish = tokio::time::timeout(
            Duration::from_secs(5),
            receive_qos1_and_ack(&mut subscriber),
        )
        .await
        .expect("subscriber should receive the Will after an ungraceful close");
        assert_eq!(publish.payload, Bytes::from_static(b"unexpected disconnect"));

        let clean = connect_tcp_with_will(&endpoint, "will_messages_clean", will).await;
        assert!(matches!(
            clean.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(500), subscriber.receiver.recv())
                .await
                .is_err(),
            "clean disconnect published the Will"
        );
        assert!(matches!(
            subscriber.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
    }
}
