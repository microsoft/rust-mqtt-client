// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! MQTT 5 enhanced authentication through a live server.

use bytes::Bytes;
use ms_mqtt_client::client::{
    ClientOptions, ConnectEnhancedAuthResult, Connection, DisconnectHandle, DisconnectedEvent,
    KeepAliveConfig, ReauthHandle, ReauthResult, new_client,
};
use ms_mqtt_client::error::{CompletionError, ConnectError};
use ms_mqtt_client::packet::{Auth, AuthReason, AuthenticationInfo, DisconnectProperties};
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};
use openssl::hash::MessageDigest;
use openssl::pkcs5::pbkdf2_hmac;
use openssl::pkey::PKey;
use openssl::sha::sha256;
use openssl::sign::Signer;

use crate::common::fixture::{EnhancedAuthMethod, FixtureCapability};
use crate::common::{
    ENV_MQTT_SAT_PORT, ENV_MQTT_SCRAM_PORT, Endpoint, RESPONSE_TIMEOUT, SAT_PORT, SCRAM_PORT,
    credential_path, port_from_env,
};

trait EnhancedAuthExchange {
    fn start(&mut self) -> AuthenticationInfo;

    fn respond(&mut self, challenge: &Auth) -> Option<Bytes>;

    fn verify_success(&mut self, _server_info: Option<&AuthenticationInfo>) {}

    fn verify_rejection(&mut self) {}
}

struct EnhancedAuthConnection {
    connection: Connection,
    disconnect_handle: DisconnectHandle,
    reauth_handle: ReauthHandle,
    method: String,
}

async fn connect_enhanced_auth<E>(
    client_id: &str,
    endpoint: Endpoint,
    mut exchange: E,
) -> EnhancedAuthConnection
where
    E: EnhancedAuthExchange,
{
    let (_client, connect_handle, _receiver) = new_client(ClientOptions {
        client_id: Some(client_id.to_string()),
        ..Default::default()
    });

    let initial = exchange.start();
    let method = initial.method.clone();
    let mut result = connect_handle
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
            initial,
            Some(RESPONSE_TIMEOUT),
        )
        .await;

    loop {
        match result {
            ConnectEnhancedAuthResult::Continue(challenge, auth_handle) => {
                assert_eq!(challenge.reason, AuthReason::ContinueAuthentication);
                result = auth_handle
                    .continue_auth(
                        exchange.respond(&challenge),
                        Default::default(),
                        Some(RESPONSE_TIMEOUT),
                    )
                    .await;
            }
            ConnectEnhancedAuthResult::Success(
                connection,
                connack,
                disconnect_handle,
                reauth_handle,
            ) => {
                exchange.verify_success(connack.properties.authentication_info.as_ref());
                return EnhancedAuthConnection {
                    connection,
                    disconnect_handle,
                    reauth_handle,
                    method,
                };
            }
            ConnectEnhancedAuthResult::Failure(_, err) => {
                panic!("enhanced authentication failed: {err}")
            }
        }
    }
}

async fn exercise_enhanced_auth<E>(
    client_id: &str,
    endpoint: Endpoint,
    make_exchange: impl Fn() -> E,
) where
    E: EnhancedAuthExchange,
{
    let EnhancedAuthConnection {
        connection,
        disconnect_handle,
        reauth_handle,
        method,
    } = connect_enhanced_auth(client_id, endpoint, make_exchange()).await;
    let runner = tokio::spawn(connection.run_until_disconnect());

    let mut exchange = make_exchange();
    let AuthenticationInfo {
        method: reauth_method,
        data,
    } = exchange.start();
    assert_eq!(reauth_method, method);
    let mut result = reauth_handle
        .reauth(data, Default::default())
        .await
        .expect("connection should still be running")
        .await
        .expect("re-authentication should complete");

    loop {
        match result {
            ReauthResult::Continue(challenge, reauth_token) => {
                assert_eq!(challenge.reason, AuthReason::ContinueAuthentication);
                result = reauth_token
                    .continue_reauth(exchange.respond(&challenge), Default::default())
                    .await
                    .expect("connection should still be running")
                    .await
                    .expect("re-authentication should complete");
            }
            ReauthResult::Success(auth) => {
                assert_eq!(auth.reason, AuthReason::Success);
                exchange.verify_success(auth.authentication_info.as_ref());
                break;
            }
            ReauthResult::Failure => panic!("re-authentication failed"),
        }
    }

    disconnect_handle
        .disconnect(&DisconnectProperties::default())
        .expect("connection should still be running");
    let (_, event) = runner.await.expect("connection runner should not panic");
    assert!(matches!(event, DisconnectedEvent::ApplicationDisconnect));
}

async fn exercise_rejected_enhanced_auth<E>(client_id: &str, endpoint: Endpoint, mut exchange: E)
where
    E: EnhancedAuthExchange,
{
    let (_client, connect_handle, _receiver) = new_client(ClientOptions {
        client_id: Some(client_id.to_string()),
        ..Default::default()
    });
    let mut result = connect_handle
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
            exchange.start(),
            Some(RESPONSE_TIMEOUT),
        )
        .await;

    loop {
        match result {
            ConnectEnhancedAuthResult::Continue(challenge, auth_handle) => {
                assert_eq!(challenge.reason, AuthReason::ContinueAuthentication);
                result = auth_handle
                    .continue_auth(
                        exchange.respond(&challenge),
                        Default::default(),
                        Some(RESPONSE_TIMEOUT),
                    )
                    .await;
            }
            ConnectEnhancedAuthResult::Failure(_, ConnectError::Rejected(connack)) => {
                assert!(!connack.is_success());
                break;
            }
            ConnectEnhancedAuthResult::Failure(_, err) => {
                panic!("expected authentication rejection, got: {err}")
            }
            ConnectEnhancedAuthResult::Success(..) => {
                panic!("invalid authentication unexpectedly succeeded")
            }
        }
    }

    exchange.verify_rejection();
}

async fn exercise_rejected_reauth<C, R>(
    client_id: &str,
    endpoint: Endpoint,
    connect_exchange: C,
    mut rejected_exchange: R,
) where
    C: EnhancedAuthExchange,
    R: EnhancedAuthExchange,
{
    let EnhancedAuthConnection {
        connection,
        disconnect_handle: _,
        reauth_handle,
        method,
    } = connect_enhanced_auth(client_id, endpoint, connect_exchange).await;
    let runner = tokio::spawn(connection.run_until_disconnect());

    let AuthenticationInfo {
        method: reauth_method,
        data,
    } = rejected_exchange.start();
    assert_eq!(reauth_method, method);
    let mut completion = reauth_handle
        .reauth(data, Default::default())
        .await
        .expect("connection should still be running");

    loop {
        match completion.await {
            Ok(ReauthResult::Continue(challenge, reauth_token)) => {
                assert_eq!(challenge.reason, AuthReason::ContinueAuthentication);
                completion = reauth_token
                    .continue_reauth(rejected_exchange.respond(&challenge), Default::default())
                    .await
                    .expect("connection should still be running");
            }
            Ok(ReauthResult::Failure) | Err(CompletionError::Canceled(_)) => break,
            Ok(ReauthResult::Success(_)) => {
                panic!("invalid re-authentication unexpectedly succeeded")
            }
            Err(err) => panic!("unexpected re-authentication completion error: {err}"),
        }
    }

    let (_, event) = runner.await.expect("connection runner should not panic");
    assert!(
        matches!(event, DisconnectedEvent::ServerDisconnect(_)),
        "server did not reject re-authentication with DISCONNECT: {event:?}"
    );
    rejected_exchange.verify_rejection();
}

/// Fixture-provided method: the client sends "1", the server challenges with "2", and the client
/// answers "3".
const CUSTOM_COUNTER_METHOD: &str = "CUSTOM-COUNTER-METHOD";

fn custom_counter_step(value: &'static [u8]) -> AuthenticationInfo {
    AuthenticationInfo {
        method: CUSTOM_COUNTER_METHOD.to_string(),
        data: Some(Bytes::from_static(value)),
    }
}

#[derive(Default)]
struct CustomCounterExchange {
    challenged: bool,
}

impl EnhancedAuthExchange for CustomCounterExchange {
    fn start(&mut self) -> AuthenticationInfo {
        custom_counter_step(b"1")
    }

    fn respond(&mut self, challenge: &Auth) -> Option<Bytes> {
        assert!(!self.challenged, "server challenged the final counter step");
        assert_eq!(
            challenge.authentication_info,
            Some(custom_counter_step(b"2"))
        );
        self.challenged = true;
        Some(Bytes::from_static(b"3"))
    }

    fn verify_success(&mut self, _server_info: Option<&AuthenticationInfo>) {
        assert!(
            self.challenged,
            "server accepted without a counter challenge"
        );
    }
}

#[derive(Default)]
struct WrongCustomCounterExchange {
    challenged: bool,
}

impl EnhancedAuthExchange for WrongCustomCounterExchange {
    fn start(&mut self) -> AuthenticationInfo {
        custom_counter_step(b"1")
    }

    fn respond(&mut self, challenge: &Auth) -> Option<Bytes> {
        assert!(
            !self.challenged,
            "server challenged the invalid counter step"
        );
        assert_eq!(
            challenge.authentication_info,
            Some(custom_counter_step(b"2"))
        );
        self.challenged = true;
        Some(Bytes::from_static(b"4"))
    }

    fn verify_rejection(&mut self) {
        assert!(
            self.challenged,
            "server rejected before receiving the invalid counter response"
        );
    }
}

/// Verifies a multi-step enhanced authentication exchange, and a re-authentication that repeats
/// it, using a custom method provided by the server fixture.
#[tokio::test]
async fn custom_enhanced_auth_counter_exchange() {
    crate::require_fixture_capability!(FixtureCapability::EnhancedAuthMethod(
        EnhancedAuthMethod::CustomCounter
    ));
    crate::test_timeout! {
        exercise_enhanced_auth(
            "custom_enhanced_auth_counter_exchange",
            Endpoint::from_env(),
            CustomCounterExchange::default,
        )
        .await;
    }
}

/// Verifies that the custom counter method rejects an incorrect final counter value.
#[tokio::test]
async fn custom_enhanced_auth_counter_rejects_wrong_response_during_connect() {
    crate::require_fixture_capability!(FixtureCapability::EnhancedAuthMethod(
        EnhancedAuthMethod::CustomCounter
    ));
    crate::test_timeout! {
        exercise_rejected_enhanced_auth(
            "custom_enhanced_auth_counter_rejects_wrong_response_during_connect",
            Endpoint::from_env(),
            WrongCustomCounterExchange::default(),
        )
        .await;
    }
}

/// Verifies that the custom counter method rejects an incorrect value during re-authentication.
#[tokio::test]
async fn custom_enhanced_auth_counter_rejects_wrong_response_during_reauth() {
    crate::require_fixture_capability!(FixtureCapability::EnhancedAuthMethod(
        EnhancedAuthMethod::CustomCounter
    ));
    crate::test_timeout! {
        exercise_rejected_reauth(
            "custom_enhanced_auth_counter_rejects_wrong_response_during_reauth",
            Endpoint::from_env(),
            CustomCounterExchange::default(),
            WrongCustomCounterExchange::default(),
        )
        .await;
    }
}

/// EMQX's built-in SCRAM method; the server proves that it knows the password in its final data.
const SCRAM_SHA_256_METHOD: &str = "SCRAM-SHA-256";
/// The user that the EMQX fixture provisions in its built-in database.
const SCRAM_USERNAME: &str = "network-scram-user";

/// Reads the password that the EMQX fixture generates for [`SCRAM_USERNAME`].
fn scram_password() -> String {
    let path = credential_path("scram.password");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read SCRAM password {path}: {err}"))
}

/// Generates a password that the EMQX fixture does not accept for [`SCRAM_USERNAME`].
fn wrong_scram_password() -> String {
    let mut password = [0; 16];
    openssl::rand::rand_bytes(&mut password).expect("random password should be generated");
    openssl::base64::encode_block(&password)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = PKey::hmac(key).expect("HMAC key should be valid");
    Signer::new(MessageDigest::sha256(), &key)
        .and_then(|mut signer| signer.sign_oneshot_to_vec(data))
        .expect("HMAC-SHA-256 should succeed")
}

/// Client side of SCRAM-SHA-256 without channel binding (RFC 5802, RFC 7677).
struct ScramSha256Exchange {
    password: String,
    client_first_bare: String,
    expected_server_final: Option<String>,
}

impl ScramSha256Exchange {
    fn new(password: String) -> Self {
        Self {
            password,
            client_first_bare: String::new(),
            expected_server_final: None,
        }
    }
}

impl EnhancedAuthExchange for ScramSha256Exchange {
    fn start(&mut self) -> AuthenticationInfo {
        let mut nonce = [0; 18];
        openssl::rand::rand_bytes(&mut nonce).expect("random nonce should be generated");
        // Base64 contains no commas, so it is a valid SCRAM nonce.
        let nonce = openssl::base64::encode_block(&nonce);
        self.client_first_bare = format!("n={SCRAM_USERNAME},r={nonce}");
        self.expected_server_final = None;
        AuthenticationInfo {
            method: SCRAM_SHA_256_METHOD.to_string(),
            data: Some(Bytes::from(format!("n,,{}", self.client_first_bare))),
        }
    }

    fn respond(&mut self, challenge: &Auth) -> Option<Bytes> {
        assert!(
            self.expected_server_final.is_none(),
            "server challenged the SCRAM client-final message"
        );
        let server_first = challenge
            .authentication_info
            .as_ref()
            .and_then(|info| info.data.as_deref())
            .expect("server challenge should contain the SCRAM server-first message");
        let server_first =
            std::str::from_utf8(server_first).expect("SCRAM server-first message should be UTF-8");
        let attribute = |name: &str| {
            server_first
                .split(',')
                .find_map(|attribute| attribute.strip_prefix(name))
                .unwrap_or_else(|| {
                    panic!("SCRAM server-first message lacks {name}: {server_first}")
                })
        };
        let nonce = attribute("r=");
        let (_, client_nonce) = self
            .client_first_bare
            .split_once(",r=")
            .expect("SCRAM client-first message should contain a nonce");
        assert!(
            nonce.starts_with(client_nonce) && nonce.len() > client_nonce.len(),
            "server nonce must extend the client nonce"
        );
        let salt = openssl::base64::decode_block(attribute("s="))
            .expect("SCRAM salt should be base64-encoded");
        let iterations = attribute("i=")
            .parse()
            .expect("SCRAM iteration count should be an integer");

        let mut salted_password = [0; 32];
        pbkdf2_hmac(
            self.password.as_bytes(),
            &salt,
            iterations,
            MessageDigest::sha256(),
            &mut salted_password,
        )
        .expect("PBKDF2-HMAC-SHA-256 should succeed");
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        // "biws" is the base64 encoding of the "n,," GS2 header.
        let client_final_without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!(
            "{},{server_first},{client_final_without_proof}",
            self.client_first_bare
        );
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(&client_signature)
            .map(|(key, signature)| key ^ signature)
            .collect();
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        let server_signature = hmac_sha256(&server_key, auth_message.as_bytes());
        self.expected_server_final = Some(format!(
            "v={}",
            openssl::base64::encode_block(&server_signature)
        ));

        Some(Bytes::from(format!(
            "{client_final_without_proof},p={}",
            openssl::base64::encode_block(&client_proof)
        )))
    }

    fn verify_success(&mut self, server_info: Option<&AuthenticationInfo>) {
        let expected_server_final = self
            .expected_server_final
            .as_deref()
            .expect("server accepted before receiving the SCRAM client proof");
        let server_final = server_info
            .and_then(|info| info.data.as_deref())
            .expect("server success should contain the SCRAM server-final message");
        assert_eq!(
            String::from_utf8_lossy(server_final),
            expected_server_final,
            "SCRAM server signature did not verify"
        );
    }

    fn verify_rejection(&mut self) {
        assert!(
            self.expected_server_final.is_some(),
            "server rejected before receiving the SCRAM client proof"
        );
    }
}

fn scram_endpoint() -> Endpoint {
    Endpoint {
        port: port_from_env(ENV_MQTT_SCRAM_PORT, SCRAM_PORT),
        ..Endpoint::from_env()
    }
}

/// Verifies SCRAM-SHA-256 authentication and re-authentication, validating the server signature
/// from the successful CONNACK and from the final re-authentication AUTH.
#[tokio::test]
async fn scram_sha_256_enhanced_auth() {
    crate::require_fixture_capability!(FixtureCapability::EnhancedAuthMethod(
        EnhancedAuthMethod::ScramSha256
    ));
    crate::test_timeout! {
        let password = scram_password();
        exercise_enhanced_auth(
            "scram_sha_256_enhanced_auth",
            scram_endpoint(),
            || ScramSha256Exchange::new(password.clone()),
        )
        .await;
    }
}

/// Verifies that SCRAM-SHA-256 authentication rejects an incorrect password.
#[tokio::test]
async fn scram_sha_256_enhanced_auth_rejects_wrong_password_during_connect() {
    crate::require_fixture_capability!(FixtureCapability::EnhancedAuthMethod(
        EnhancedAuthMethod::ScramSha256
    ));
    crate::test_timeout! {
        exercise_rejected_enhanced_auth(
            "scram_sha_256_enhanced_auth_rejects_wrong_password_during_connect",
            scram_endpoint(),
            ScramSha256Exchange::new(wrong_scram_password()),
        )
        .await;
    }
}

/// Verifies that SCRAM-SHA-256 re-authentication rejects an incorrect password.
#[tokio::test]
async fn scram_sha_256_enhanced_auth_rejects_wrong_password_during_reauth() {
    crate::require_fixture_capability!(FixtureCapability::EnhancedAuthMethod(
        EnhancedAuthMethod::ScramSha256
    ));
    crate::test_timeout! {
        exercise_rejected_reauth(
            "scram_sha_256_enhanced_auth_rejects_wrong_password_during_reauth",
            scram_endpoint(),
            ScramSha256Exchange::new(scram_password()),
            ScramSha256Exchange::new(wrong_scram_password()),
        )
        .await;
    }
}

/// AIO MQ's method for Kubernetes service account tokens; the token is the authentication data.
const K8S_SAT_METHOD: &str = "K8S-SAT";

/// Reads the Kubernetes service account token that the AIO MQ fixture mints.
fn service_account_token() -> Bytes {
    let path = credential_path("sat.token");
    let token = std::fs::read(&path)
        .unwrap_or_else(|err| panic!("failed to read service account token {path}: {err}"));
    Bytes::from(token)
}

struct K8sSatExchange {
    token: Bytes,
}

impl EnhancedAuthExchange for K8sSatExchange {
    fn start(&mut self) -> AuthenticationInfo {
        AuthenticationInfo {
            method: K8S_SAT_METHOD.to_string(),
            data: Some(self.token.clone()),
        }
    }

    fn respond(&mut self, _challenge: &Auth) -> Option<Bytes> {
        panic!("server challenged a single-step method")
    }
}

/// Verifies single-step K8S-SAT authentication, and a re-authentication with the same token.
#[tokio::test]
async fn k8s_sat_enhanced_auth() {
    crate::require_fixture_capability!(FixtureCapability::EnhancedAuthMethod(
        EnhancedAuthMethod::K8sSat
    ));
    crate::test_timeout! {
        let token = service_account_token();
        exercise_enhanced_auth(
            "k8s_sat_enhanced_auth",
            Endpoint {
                port: port_from_env(ENV_MQTT_SAT_PORT, SAT_PORT),
                ..Endpoint::from_env()
            },
            || K8sSatExchange {
                token: token.clone(),
            },
        )
        .await;
    }
}

/// Verifies that K8S-SAT authentication rejects an invalid service account token.
#[tokio::test]
async fn k8s_sat_enhanced_auth_rejects_bad_token_during_connect() {
    crate::require_fixture_capability!(FixtureCapability::EnhancedAuthMethod(
        EnhancedAuthMethod::K8sSat
    ));
    crate::test_timeout! {
        exercise_rejected_enhanced_auth(
            "k8s_sat_enhanced_auth_rejects_bad_token_during_connect",
            Endpoint {
                port: port_from_env(ENV_MQTT_SAT_PORT, SAT_PORT),
                ..Endpoint::from_env()
            },
            K8sSatExchange {
                token: Bytes::from_static(b"not-a-service-account-token"),
            },
        )
        .await;
    }
}

/// Verifies that K8S-SAT re-authentication rejects an invalid service account token.
#[tokio::test]
async fn k8s_sat_enhanced_auth_rejects_bad_token_during_reauth() {
    crate::require_fixture_capability!(FixtureCapability::EnhancedAuthMethod(
        EnhancedAuthMethod::K8sSat
    ));
    crate::test_timeout! {
        let token = service_account_token();
        exercise_rejected_reauth(
            "k8s_sat_enhanced_auth_rejects_bad_token_during_reauth",
            Endpoint {
                port: port_from_env(ENV_MQTT_SAT_PORT, SAT_PORT),
                ..Endpoint::from_env()
            },
            K8sSatExchange { token },
            K8sSatExchange {
                token: Bytes::from_static(b"not-a-service-account-token"),
            },
        )
        .await;
    }
}
