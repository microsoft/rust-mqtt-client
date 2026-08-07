// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Capabilities provisioned by live test fixtures.

use super::server::{AIO_MQ, EMQX, HIVEMQ_CE, MOSQUITTO, server_name};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FixtureCapability {
    MutualTls,
    WebSocketPathValidation,
}

pub(crate) fn supports_capability(capability: FixtureCapability) -> bool {
    matches!(
        (server_name().as_deref(), capability),
        (None | Some(MOSQUITTO), FixtureCapability::MutualTls)
            | (
                Some(EMQX | HIVEMQ_CE),
                FixtureCapability::WebSocketPathValidation
            )
    )
}

#[macro_export]
macro_rules! require_fixture_capability {
    ($capability:expr) => {
        if !$crate::common::fixture::supports_capability($capability) {
            println!(
                "SKIP: {} server fixture does not provision {:?}",
                $crate::common::server::server_name().unwrap_or_else(|| "<unknown>".into()),
                $capability,
            );
            return;
        }
    };
}
