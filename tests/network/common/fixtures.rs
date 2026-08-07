// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Capabilities provisioned by live test fixtures and quirks requiring test workarounds.

use super::capabilities::server_name;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FixtureCapability {
    MutualTls,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FixtureQuirk {
    FailedTlsHandshakeDestabilizesServer,
    RequiresSerialTransportTests,
}

pub(crate) fn supports_capability(capability: FixtureCapability) -> bool {
    matches!(
        (server_name().as_deref(), capability),
        (None | Some("mosquitto"), FixtureCapability::MutualTls)
    )
}

pub(crate) fn has_quirk(quirk: FixtureQuirk) -> bool {
    matches!(
        (server_name().as_deref(), quirk),
        (
            Some("aio-mq"),
            FixtureQuirk::FailedTlsHandshakeDestabilizesServer
                | FixtureQuirk::RequiresSerialTransportTests
        )
    )
}

#[macro_export]
macro_rules! require_fixture_capability {
    ($capability:expr) => {
        if !$crate::common::fixtures::supports_capability($capability) {
            println!(
                "SKIP: {} server fixture does not provision {:?}",
                $crate::common::capabilities::server_name().unwrap_or_else(|| "<unknown>".into()),
                $capability,
            );
            return;
        }
    };
}

#[macro_export]
macro_rules! skip_for_fixture_quirk {
    ($quirk:expr) => {
        if $crate::common::fixtures::has_quirk($quirk) {
            println!(
                "SKIP: {} server fixture has {:?}",
                $crate::common::capabilities::server_name().unwrap_or_else(|| "<unknown>".into()),
                $quirk,
            );
            return;
        }
    };
}
