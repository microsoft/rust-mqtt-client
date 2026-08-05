// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Live network tests, written to be broker-agnostic.
//!
//! The broker is chosen at run time, not at compile time: these tests connect to whatever
//! `MQTT_HOST`/`MQTT_PORT` point at, so the same suite validates every broker. Pick one with
//! `make network-test BROKER=<name>` (see `tests/network/brokers/`).
//!
//! Enabled by the `__network` feature, which leaves `#[ignore]` free to quarantine a flaky
//! test. Add a new area of coverage as another module here.

mod common;
mod pubsub;

use std::num::NonZeroU32;

use bytes::Bytes;
use common::capabilities::{Feature, broker_name, supports};
use common::{Endpoint, connect_tcp, with_timeout};
use ms_mqtt_client::client::{DisconnectedEvent, ManualAcknowledgement};
use ms_mqtt_client::packet::{
    DisconnectProperties, PublishProperties, QoS, RetainOptions, SubscribeProperties,
};
use ms_mqtt_client::topic::{TopicFilter, TopicName};

const DEFAULT_PORT: u16 = 1883;

// TODO: Consider moving elsewhere, as this is not really a client test
// Guards the inventory in `common::capabilities` against drift: if a broker gains or loses a
// feature, this fails instead of tests being silently skipped forever.
#[tokio::test]
async fn inventory_matches_broker() {
    with_timeout(Box::pin(async {
        let Some(broker) = broker_name() else {
            println!("SKIP: MQTT_BROKER is unset, so there is no inventory to verify");
            return;
        };
        let endpoint = Endpoint::from_env(DEFAULT_PORT);
        let live = connect_tcp(&endpoint, "inventory_matches_broker").await;

        for &feature in Feature::ALL {
            assert_eq!(
                supports(feature),
                feature.advertised_by(&live.connack.properties),
                "inventory disagrees with what {broker} advertises for {feature:?}"
            );
        }

        live.disconnect_handle
            .disconnect(&DisconnectProperties::default())
            .expect("connection should still be running");
        let _ = live.connection.run_until_disconnect().await;
    }))
    .await;
}

// Real clock on purpose: unlike the offline suites, this drives actual network I/O.
#[tokio::test]
async fn connect_disconnect() {
    // Boxed because the client's connection state makes this future large (clippy::large_futures).
    with_timeout(Box::pin(async {
        let endpoint = Endpoint::from_env(DEFAULT_PORT);
        let live = connect_tcp(&endpoint, "connect_disconnect").await;

        assert!(
            live.connack.is_success(),
            "broker refused the connection: {:?}",
            live.connack
        );

        live.disconnect_handle
            .disconnect(&DisconnectProperties::default())
            .expect("connection should still be running");

        let (_connect_handle, event) = live.connection.run_until_disconnect().await;
        assert!(
            matches!(event, DisconnectedEvent::ApplicationDisconnect),
            "expected a client-initiated disconnect, got {event:?}"
        );
    }))
    .await;
}

// Skipped on brokers without subscription identifiers, which is what `require_feature!` is for.
#[tokio::test]
async fn subscribe_with_subscription_identifier() {
    require_feature!(Feature::SubscriptionIdentifiers);
    with_timeout(Box::pin(async {
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
            "broker rejected the subscription: {suback:?}"
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
    }))
    .await;
}
