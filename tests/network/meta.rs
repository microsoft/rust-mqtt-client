// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Meta-tests that validate the live test fixtures and suite assumptions.

use crate::common::server::{ServerFeature, server_name, supports};
use crate::common::{ENV_MQTT_SERVER, Endpoint, connect_tcp};

/// Verifies that the fixture's server-capability inventory matches the capabilities advertised
/// by the live server in CONNACK.
#[tokio::test]
async fn inventory_matches_server() {
    crate::test_timeout! {
        let Some(server) = server_name() else {
            println!("SKIP: {ENV_MQTT_SERVER} is unset, so there is no inventory to verify");
            return;
        };
        let endpoint = Endpoint::from_env();
        let live = connect_tcp(&endpoint, "inventory_matches_server").await;

        for &feature in ServerFeature::ALL {
            assert_eq!(
                supports(feature),
                feature.advertised_by(&live.connack.properties),
                "inventory disagrees with what {server} advertises for {feature:?}"
            );
        }

        let _ = live.disconnect().await;
    }
}
