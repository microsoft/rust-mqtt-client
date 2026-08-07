// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Meta-tests that validate the live test fixtures and suite assumptions.

use ms_mqtt_client::packet::DisconnectProperties;

use crate::common::capabilities::{Feature, server_name, supports};
use crate::common::{Endpoint, connect_tcp};

const DEFAULT_PORT: u16 = 1883;

/// Verifies that the fixture's server-capability inventory matches the capabilities advertised
/// by the live server in CONNACK.
#[tokio::test]
async fn inventory_matches_server() {
    crate::test_timeout! {
        let Some(server) = server_name() else {
            println!("SKIP: MQTT_SERVER is unset, so there is no inventory to verify");
            return;
        };
        let endpoint = Endpoint::from_env(DEFAULT_PORT);
        let live = connect_tcp(&endpoint, "inventory_matches_server").await;

        for &feature in Feature::ALL {
            assert_eq!(
                supports(feature),
                feature.advertised_by(&live.connack.properties),
                "inventory disagrees with what {server} advertises for {feature:?}"
            );
        }

        live.disconnect_handle
            .disconnect(&DisconnectProperties::default())
            .expect("connection should still be running");
        let _ = live.connection.run_until_disconnect().await;
    }
}
