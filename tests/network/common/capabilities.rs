// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Which broker supports what.
//!
//! Brokers do not implement all of MQTT 5, so tests declare the features they need and are
//! skipped where those are missing. [`UNSUPPORTED`] is the inventory consulted when skipping;
//! because MQTT 5 makes brokers advertise these in CONNACK, `inventory_matches_broker`
//! checks the inventory against reality so it cannot drift silently.

use ms_mqtt_client::packet::{ConnAckProperties, QoS};

/// A broker behavior that some brokers don't implement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Feature {
    /// QoS 2, exactly-once delivery.
    Qos2,
    /// Subscription identifiers returned on matching publishes.
    SubscriptionIdentifiers,
}

impl Feature {
    pub(crate) const ALL: &'static [Feature] = &[Feature::Qos2, Feature::SubscriptionIdentifiers];

    /// Whether the broker's own CONNACK claims this feature.
    pub(crate) fn advertised_by(self, properties: &ConnAckProperties) -> bool {
        match self {
            Feature::Qos2 => properties.maximum_qos == QoS::ExactlyOnce,
            Feature::SubscriptionIdentifiers => properties.subscription_identifiers_available,
        }
    }
}

/// Features each broker lacks. Brokers absent from this list are assumed to support everything.
const UNSUPPORTED: &[(&str, &[Feature])] =
    &[("aio-mq", &[Feature::Qos2, Feature::SubscriptionIdentifiers])];

/// The broker under test, from `MQTT_BROKER` (set by `make network-test`).
pub(crate) fn broker_name() -> Option<String> {
    std::env::var("MQTT_BROKER").ok().filter(|n| !n.is_empty())
}

/// An unnamed broker is assumed to support everything: a real failure is more useful than a
/// silent skip.
pub(crate) fn supports(feature: Feature) -> bool {
    let Some(broker) = broker_name() else {
        return true;
    };
    !UNSUPPORTED
        .iter()
        .any(|(name, missing)| *name == broker && missing.contains(&feature))
}

/// Skips the calling test when the broker under test lacks `$feature`.
#[macro_export]
macro_rules! require_feature {
    ($feature:expr) => {
        if !$crate::common::capabilities::supports($feature) {
            // Printed rather than silent so a skip is visible with --nocapture.
            println!(
                "SKIP: {} does not support {:?}",
                $crate::common::capabilities::broker_name().unwrap_or_else(|| "<unknown>".into()),
                $feature,
            );
            return;
        }
    };
}
