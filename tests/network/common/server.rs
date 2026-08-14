// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Which MQTT server supports what.
//!
//! Servers do not inherently implement all of MQTT 5, so tests declare the features they need
//! and are skipped where those are missing. [`UNSUPPORTED`] is the inventory consulted when skipping;
//! because MQTT 5 makes servers advertise these in CONNACK, `inventory_matches_server`
//! checks the inventory against reality so it cannot drift silently.

use ms_mqtt_client::packet::{ConnAckProperties, QoS};

use super::ENV_MQTT_SERVER;

pub(crate) const MOSQUITTO: &str = "mosquitto";
pub(crate) const EMQX: &str = "emqx";
pub(crate) const HIVEMQ_CE: &str = "hivemq-ce";
pub(crate) const AIO_MQ: &str = "aio-mq";

/// An MQTT server behavior that some servers don't implement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ServerFeature {
    /// QoS 2, exactly-once delivery.
    Qos2,
    /// Subscription identifiers returned on matching publishes.
    SubscriptionIdentifiers,
    /// Topic filters containing single-level or multi-level wildcards.
    WildcardSubscriptions,
}

impl ServerFeature {
    pub(crate) const ALL: &'static [ServerFeature] = &[
        ServerFeature::Qos2,
        ServerFeature::SubscriptionIdentifiers,
        ServerFeature::WildcardSubscriptions,
    ];

    /// Whether the server's own CONNACK claims this feature.
    pub(crate) fn advertised_by(self, properties: &ConnAckProperties) -> bool {
        match self {
            ServerFeature::Qos2 => properties.maximum_qos == QoS::ExactlyOnce,
            ServerFeature::SubscriptionIdentifiers => properties.subscription_identifiers_available,
            ServerFeature::WildcardSubscriptions => properties.wildcard_subscription_available,
        }
    }
}

/// Features each server fixture lacks. Servers absent from this list are assumed to support
/// everything.
const UNSUPPORTED: &[(&str, &[ServerFeature])] = &[(
    AIO_MQ,
    &[ServerFeature::Qos2, ServerFeature::SubscriptionIdentifiers],
)];

/// The server fixture under test, from `MQTT_SERVER` (set by `make network-test`).
pub(crate) fn server_name() -> Option<String> {
    std::env::var(ENV_MQTT_SERVER)
        .ok()
        .filter(|n| !n.is_empty())
}

/// An unnamed server is assumed to support everything: a real failure is more useful than a
/// silent skip.
pub(crate) fn supports(feature: ServerFeature) -> bool {
    let Some(server) = server_name() else {
        return true;
    };
    !UNSUPPORTED
        .iter()
        .any(|(name, missing)| *name == server && missing.contains(&feature))
}

/// Whether the server honors a DISCONNECT session-expiry override.
pub(crate) fn supports_disconnect_session_expiry_override() -> bool {
    server_name().as_deref() != Some(HIVEMQ_CE)
}

/// Skips the calling test when the MQTT server under test lacks `$feature`.
#[macro_export]
macro_rules! require_server_feature {
    ($feature:expr) => {
        if !$crate::common::server::supports($feature) {
            // Printed rather than silent so a skip is visible with --nocapture.
            println!(
                "SKIP: {} does not support {:?}",
                $crate::common::server::server_name().unwrap_or_else(|| "<unknown>".into()),
                $feature,
            );
            return;
        }
    };
}
