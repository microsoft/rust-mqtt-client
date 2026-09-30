// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! MQTT 5 enhanced authentication through a live server.

use bytes::Bytes;
use ms_mqtt_client::client::{
    ClientOptions, ConnectEnhancedAuthResult, DisconnectedEvent, KeepAliveConfig, ReauthResult,
    new_client,
};
use ms_mqtt_client::packet::{AuthReason, AuthenticationInfo, DisconnectProperties};
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};

use crate::common::fixture::FixtureCapability;
use crate::common::{Endpoint, RESPONSE_TIMEOUT};

/// Fixture-provided method: the client sends "1", the server challenges with "2", and the client
/// answers "3".
const CUSTOM_COUNTER_METHOD: &str = "CUSTOM-COUNTER-METHOD";

fn custom_counter_step(value: &'static [u8]) -> AuthenticationInfo {
    AuthenticationInfo {
        method: CUSTOM_COUNTER_METHOD.to_string(),
        data: Some(Bytes::from_static(value)),
    }
}

/// Verifies a multi-step enhanced authentication exchange, and a re-authentication that repeats
/// it, using a custom method provided by the server fixture.
#[tokio::test]
async fn custom_enhanced_auth_counter_exchange() {
    // TODO: HiveMQ CE could also provide this method through an extension `EnhancedAuthenticator`.
    crate::require_fixture_capability!(FixtureCapability::CustomEnhancedAuth);
    crate::test_timeout! {
        let endpoint = Endpoint::from_env();
        let (_client, connect_handle, _receiver) = new_client(ClientOptions {
            client_id: Some("custom_enhanced_auth_counter_exchange".to_string()),
            ..Default::default()
        });

        let result = connect_handle
            .connect_enhanced_auth(
                ConnectionTransportConfig {
                    transport_type: ConnectionTransportType::Tcp {
                        hostname: endpoint.hostname,
                        port: endpoint.port,
                    },
                    timeout: Some(RESPONSE_TIMEOUT),
                    proxy: None,
                    tcp_nodelay: false,
                },
                true,
                KeepAliveConfig::Infinite,
                None,
                None,
                None,
                Default::default(),
                custom_counter_step(b"1"),
                Some(RESPONSE_TIMEOUT),
            )
            .await;
        let (challenge, auth_handle) = match result {
            ConnectEnhancedAuthResult::Continue(challenge, auth_handle) => (challenge, auth_handle),
            ConnectEnhancedAuthResult::Success(..) => {
                panic!("server accepted CONNECT without a challenge")
            }
            ConnectEnhancedAuthResult::Failure(_, err) => {
                panic!("enhanced authentication failed: {err}")
            }
        };
        assert_eq!(challenge.reason, AuthReason::ContinueAuthentication);
        assert_eq!(challenge.authentication_info, Some(custom_counter_step(b"2")));

        let result = auth_handle
            .continue_auth(
                Some(Bytes::from_static(b"3")),
                Default::default(),
                Some(RESPONSE_TIMEOUT),
            )
            .await;
        let (connection, disconnect_handle, reauth_handle) = match result {
            ConnectEnhancedAuthResult::Success(connection, _, disconnect_handle, reauth_handle) => {
                (connection, disconnect_handle, reauth_handle)
            }
            ConnectEnhancedAuthResult::Continue(..) => {
                panic!("server challenged the final counter step")
            }
            ConnectEnhancedAuthResult::Failure(_, err) => {
                panic!("enhanced authentication failed: {err}")
            }
        };
        let runner = tokio::spawn(connection.run_until_disconnect());

        let result = reauth_handle
            .reauth(Some(Bytes::from_static(b"1")), Default::default())
            .await
            .expect("connection should still be running")
            .await
            .expect("re-authentication should complete");
        let (challenge, reauth_token) = match result {
            ReauthResult::Continue(challenge, reauth_token) => (challenge, reauth_token),
            other => panic!("server should challenge re-authentication, got {other:?}"),
        };
        assert_eq!(challenge.reason, AuthReason::ContinueAuthentication);
        assert_eq!(challenge.authentication_info, Some(custom_counter_step(b"2")));

        let result = reauth_token
            .continue_reauth(Some(Bytes::from_static(b"3")), Default::default())
            .await
            .expect("connection should still be running")
            .await
            .expect("re-authentication should complete");
        assert!(
            matches!(result, ReauthResult::Success(_)),
            "server should accept re-authentication, got {result:?}"
        );

        disconnect_handle
            .disconnect(&DisconnectProperties::default())
            .expect("connection should still be running");
        let (_, event) = runner.await.expect("connection runner should not panic");
        assert!(matches!(event, DisconnectedEvent::ApplicationDisconnect));
    }
}
