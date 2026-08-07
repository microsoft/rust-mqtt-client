// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Tests awaiting assignment to a focused network-test suite.

use std::num::NonZeroU32;

use bytes::Bytes;
use ms_mqtt_client::client::{DisconnectedEvent, ManualAcknowledgement};
use ms_mqtt_client::packet::{PublishProperties, QoS, RetainOptions, SubscribeProperties};
use ms_mqtt_client::topic::{TopicFilter, TopicName};

use crate::common::capabilities::Feature;
use crate::common::{Endpoint, connect_tcp};

const DEFAULT_PORT: u16 = 1883;

/// Verifies that a subscription identifier supplied in SUBSCRIBE is returned on a matching
/// publication delivered by a supporting server.
#[tokio::test]
async fn subscribe_with_subscription_identifier() {
    crate::require_server_feature!(Feature::SubscriptionIdentifiers);
    crate::test_timeout! {
        let endpoint = Endpoint::from_env(DEFAULT_PORT);
        let mut live = connect_tcp(&endpoint, "subscribe_with_subscription_identifier")
            .await
            .start();
        let topic = "ms-mqtt-client/network/subid";

        let token = live
            .client
            .subscribe(
                TopicFilter::new(topic).unwrap(),
                QoS::AtLeastOnce,
                false,
                RetainOptions::default(),
                SubscribeProperties {
                    subscription_identifier: Some(NonZeroU32::new(1).unwrap()),
                    ..Default::default()
                },
            )
            .await
            .expect("client should still be attached");

        let suback = token.await.expect("SUBSCRIBE should complete");
        assert!(
            suback.is_success(),
            "server rejected the subscription: {suback:?}"
        );

        live.client
            .publish_qos0(
                TopicName::new(topic).unwrap(),
                Bytes::from_static(b"subscription identifier"),
                false,
                PublishProperties::default(),
            )
            .await
            .expect("client should still be attached")
            .await
            .expect("PUBLISH should be sent");
        let (publish, acknowledgement) = live
            .receiver
            .recv()
            .await
            .expect("subscription should receive the PUBLISH");
        assert_eq!(
            publish.properties.subscription_identifiers,
            vec![NonZeroU32::new(1).unwrap()]
        );
        assert!(matches!(acknowledgement, ManualAcknowledgement::QoS0));

        assert!(matches!(
            live.disconnect().await,
            DisconnectedEvent::ApplicationDisconnect
        ));
    }
}
