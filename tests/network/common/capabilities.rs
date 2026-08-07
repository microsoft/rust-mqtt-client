// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Which MQTT server supports what.
//!
//! Servers do not inherently implement all of MQTT 5, so tests declare the features they need and are
//! skipped where those are missing. [`UNSUPPORTED`] is the inventory consulted when skipping;
//! because MQTT 5 makes servers advertise these in CONNACK, `inventory_matches_server`
//! checks the inventory against reality so it cannot drift silently.

use ms_mqtt_client::packet::{ConnAckProperties, QoS};

/// An MQTT server behavior that some servers don't implement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Feature {
    /// QoS 2, exactly-once delivery.
    Qos2,
    /// Subscription identifiers returned on matching publishes.
    SubscriptionIdentifiers,
    /// Topic filters containing single-level or multi-level wildcards.
    WildcardSubscriptions,
}

impl Feature {
    pub(crate) const ALL: &'static [Feature] = &[
        Feature::Qos2,
        Feature::SubscriptionIdentifiers,
        Feature::WildcardSubscriptions,
    ];

    /// Whether the server's own CONNACK claims this feature.
    pub(crate) fn advertised_by(self, properties: &ConnAckProperties) -> bool {
        match self {
            Feature::Qos2 => properties.maximum_qos == QoS::ExactlyOnce,
            Feature::SubscriptionIdentifiers => properties.subscription_identifiers_available,
            Feature::WildcardSubscriptions => properties.wildcard_subscription_available,
        }
    }
}

/// Features each server fixture lacks. Servers absent from this list are assumed to support
/// everything.
const UNSUPPORTED: &[(&str, &[Feature])] =
    &[("aio-mq", &[Feature::Qos2, Feature::SubscriptionIdentifiers])];

/// The server fixture under test, from `MQTT_SERVER` (set by `make network-test`).
pub(crate) fn server_name() -> Option<String> {
    std::env::var("MQTT_SERVER").ok().filter(|n| !n.is_empty())
}

/// An unnamed server is assumed to support everything: a real failure is more useful than a
/// silent skip.
pub(crate) fn supports(feature: Feature) -> bool {
    let Some(server) = server_name() else {
        return true;
    };
    !UNSUPPORTED
        .iter()
        .any(|(name, missing)| *name == server && missing.contains(&feature))
}

/// Skips the calling test when the MQTT server under test lacks `$feature`.
#[macro_export]
macro_rules! require_server_feature {
    ($feature:expr) => {
        if !$crate::common::capabilities::supports($feature) {
            // Printed rather than silent so a skip is visible with --nocapture.
            println!(
                "SKIP: {} does not support {:?}",
                $crate::common::capabilities::server_name().unwrap_or_else(|| "<unknown>".into()),
                $feature,
            );
            return;
        }
    };
}
