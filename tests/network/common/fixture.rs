// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Capabilities provisioned by live test fixtures.

use super::server::{AIO_MQ, EMQX, HIVEMQ_CE, MOSQUITTO, server_name};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FixtureCapability {
    EnhancedAuthMethod(EnhancedAuthMethod),
    MutualTls,
    WebSocketPathValidation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EnhancedAuthMethod {
    CustomCounter,
    K8sSat,
}

pub(crate) fn supports_capability(capability: FixtureCapability) -> bool {
    let server = server_name();
    match capability {
        FixtureCapability::EnhancedAuthMethod(EnhancedAuthMethod::CustomCounter) => {
            // TODO: EMQX can also support this, but requires more advanced version management
            // of the fixtures to do without introducing brittleness.
            matches!(server.as_str(), MOSQUITTO | HIVEMQ_CE)
        }
        FixtureCapability::EnhancedAuthMethod(EnhancedAuthMethod::K8sSat) => server == AIO_MQ,
        FixtureCapability::MutualTls => server == MOSQUITTO,
        FixtureCapability::WebSocketPathValidation => {
            matches!(server.as_str(), EMQX | HIVEMQ_CE)
        }
    }
}

#[macro_export]
macro_rules! require_fixture_capability {
    ($capability:expr) => {
        if !$crate::common::fixture::supports_capability($capability) {
            println!(
                "SKIP: {} server fixture does not provision {:?}",
                $crate::common::server::server_name(),
                $capability,
            );
            return;
        }
    };
}
