// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::num::NonZero;
use std::pin::pin;
use std::time::Duration;

use bytes::Bytes;
use matches::assert_matches;
use ms_mqtt_client::client::{
    ClientOptions, ConnectEnhancedAuthResult, ConnectionTransportConfig, ConnectionTransportType,
    KeepAliveConfig, ReauthResult, new_client,
};
use ms_mqtt_client::mqtt_proto::{
    self, AuthenticateReasonCode, Authentication, ConnectOtherProperties, ConnectReasonCode, Packet,
};
use ms_mqtt_client::packet::{Auth, AuthReason, AuthenticationInfo, ConnAck};
use tokio::sync::mpsc::unbounded_channel;

mod common;
use common::run_with_connection;

#[tokio::test(start_paused = true)]
async fn auth_reauth() {
    let options = ClientOptions {
        client_id: Some("foo".to_string()),
        ..Default::default()
    };
    let (_client, connect_handle, _receiver) = new_client(options);

    let (incoming_packets_tx, incoming_packets_rx) = unbounded_channel();
    let (outgoing_packets_tx, mut outgoing_packets_rx) = unbounded_channel();

    // First exchange: client sends CONNECT, server responds with AUTH (continue)
    incoming_packets_tx
        .send(Packet::Auth(mqtt_proto::Auth {
            reason_code: AuthenticateReasonCode::ContinueAuthentication,
            authentication: Some(Authentication {
                method: "some method".into(),
                data: Some(b"some server data 1".into()),
            }),
            reason_string: None,
            user_properties: Default::default(),
        }))
        .unwrap();

    let keep_alive_time = NonZero::<u16>::new(5).unwrap();

    let ConnectEnhancedAuthResult::Continue(incoming_auth, connect_handle) = connect_handle
        .connect_enhanced_auth(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Test {
                    incoming_packets: incoming_packets_rx,
                    outgoing_packets: outgoing_packets_tx,
                },
                timeout: None,
            },
            false,
            KeepAliveConfig::Duration {
                ping_after: keep_alive_time,
                response_timeout: Duration::from_secs(5),
            },
            None,
            None,
            None,
            Default::default(),
            AuthenticationInfo {
                method: "some method".to_owned(),
                data: Some(Bytes::from_static(b"some client data 1")),
            },
            None,
        )
        .await
    else {
        panic!("expected AUTH")
    };
    let outgoing_packet = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(
        outgoing_packet,
        Packet::Connect(mqtt_proto::Connect {
            other_properties: ConnectOtherProperties {
                authentication: Some(Authentication {
                    method,
                    data: Some(data),
                }),
                ..
            },
            ..
        }) if method == "some method" && data == b"some client data 1"[..]
    );
    assert_matches!(incoming_auth, Auth {
        reason: AuthReason::ContinueAuthentication,
        authentication_info: Some(AuthenticationInfo {
            method,
            data: Some(data),
        }),
        properties: _,
    } if method == "some method" && data == b"some server data 1"[..]);

    // Second exchange: client sends AUTH (continue), server responds with AUTH (continue)
    incoming_packets_tx
        .send(Packet::Auth(mqtt_proto::Auth {
            reason_code: AuthenticateReasonCode::ContinueAuthentication,
            authentication: Some(Authentication {
                method: "some method".into(),
                data: Some(b"some server data 2".into()),
            }),
            reason_string: None,
            user_properties: Default::default(),
        }))
        .unwrap();

    let ConnectEnhancedAuthResult::Continue(incoming_auth, connect_handle) = connect_handle
        .continue_auth(
            Some(Bytes::from_static(b"some client data 2")),
            Default::default(),
            None,
        )
        .await
    else {
        panic!("expected AUTH")
    };
    let outgoing_packet = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(
        outgoing_packet,
        Packet::Auth(mqtt_proto::Auth {
            reason_code: AuthenticateReasonCode::ContinueAuthentication,
            authentication: Some(Authentication {
                method,
                data: Some(data),
            }),
            ..
        }) if method == "some method" && data == b"some client data 2"[..]
    );
    assert_matches!(incoming_auth, Auth {
        reason: AuthReason::ContinueAuthentication,
        authentication_info: Some(AuthenticationInfo {
            method,
            data: Some(data),
        }),
        properties: _,
    } if method == "some method" && data == b"some server data 2"[..]);

    // Third exchange: client sends AUTH (continue), server responds with CONNACK
    incoming_packets_tx
        .send(Packet::ConnAck(mqtt_proto::ConnAck {
            reason_code: ConnectReasonCode::Success {
                session_present: false,
            },
            other_properties: Default::default(),
        }))
        .unwrap();

    let ConnectEnhancedAuthResult::Success(connection, connack, _disconnect_handle, reauth_handle) =
        connect_handle
            .continue_auth(
                Some(Bytes::from_static(b"some client data 3")),
                Default::default(),
                None,
            )
            .await
    else {
        panic!("expected AUTH")
    };
    let outgoing_packet = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(
        outgoing_packet,
        Packet::Auth(mqtt_proto::Auth {
            reason_code: AuthenticateReasonCode::ContinueAuthentication,
            authentication: Some(Authentication {
                method,
                data: Some(data),
            }),
            ..
        }) if method == "some method" && data == b"some client data 3"[..]
    );
    assert_matches!(connack, ConnAck { .. });

    let mut connection = pin!(connection.run_until_disconnect());

    // Connection should be idle now until the next PINGREQ.
    _ = tokio::time::timeout(
        Duration::from_secs(u64::from(keep_alive_time.get() + 1)),
        &mut connection,
    )
    .await;

    let outgoing_packet = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(outgoing_packet, Packet::PingReq(mqtt_proto::PingReq));

    incoming_packets_tx
        .send(Packet::PingResp(mqtt_proto::PingResp))
        .unwrap();

    // Fourth exchange: client sends AUTH (reauthenticate), server responds with AUTH (continue)
    incoming_packets_tx
        .send(Packet::Auth(mqtt_proto::Auth {
            reason_code: AuthenticateReasonCode::ContinueAuthentication,
            authentication: Some(Authentication {
                method: "some method".into(),
                data: Some(b"some server data reauth 1".into()),
            }),
            reason_string: None,
            user_properties: Default::default(),
        }))
        .unwrap();

    let reauth_token = reauth_handle
        .reauth(
            Some(Bytes::from_static(b"some client data reauth 1")),
            Default::default(),
        )
        .await
        .unwrap();
    let ReauthResult::Continue(incoming_auth, reauth_token) =
        run_with_connection(&mut connection, reauth_token)
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("could not reauth");
    };
    let outgoing_packet = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(
        outgoing_packet,
        Packet::Auth(mqtt_proto::Auth {
            reason_code: AuthenticateReasonCode::ReAuthenticate,
            authentication: Some(Authentication {
                method,
                data: Some(data),
            }),
            ..
        }) if method == "some method" && data == b"some client data reauth 1"[..]
    );
    assert_matches!(incoming_auth, Auth {
        reason: AuthReason::ContinueAuthentication,
        authentication_info: Some(AuthenticationInfo {
            method,
            data: Some(data),
        }),
        properties: _,
    } if method == "some method" && data == b"some server data reauth 1"[..]);

    // Fifth exchange: client sends AUTH (continue), server responds with AUTH (success)
    incoming_packets_tx
        .send(Packet::Auth(mqtt_proto::Auth {
            reason_code: AuthenticateReasonCode::Success,
            authentication: Some(Authentication {
                method: "some method".into(),
                data: Some(b"some server data reauth 2".into()),
            }),
            reason_string: None,
            user_properties: Default::default(),
        }))
        .unwrap();

    let reauth_token = reauth_token
        .continue_reauth(
            Some(Bytes::from_static(b"some client data reauth 2")),
            Default::default(),
        )
        .await
        .unwrap();
    let ReauthResult::Success(incoming_auth) = run_with_connection(&mut connection, reauth_token)
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("could not reauth");
    };
    let outgoing_packet = outgoing_packets_rx.recv().await.unwrap();
    assert_matches!(
        outgoing_packet,
        Packet::Auth(mqtt_proto::Auth {
            reason_code: AuthenticateReasonCode::ContinueAuthentication,
            authentication: Some(Authentication {
                method,
                data: Some(data),
            }),
            ..
        }) if method == "some method" && data == b"some client data reauth 2"[..]
    );
    assert_matches!(incoming_auth, Auth {
        reason: AuthReason::Success,
        authentication_info: Some(AuthenticationInfo {
            method,
            data: Some(data),
        }),
        properties: _,
    } if method == "some method" && data == b"some server data reauth 2"[..]);
}
