// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! TCP, TLS, proxy, and optional WebSocket transport configuration.
//!
//! Pass a [`ConnectionTransportConfig`] to [`crate::client::ConnectHandle::connect`] or
//! [`crate::client::ConnectHandle::connect_enhanced_auth`]. Transport establishment and the MQTT
//! CONNECT exchange have separate timeout settings.

use std::{io, time::Duration};

#[cfg(feature = "__integration")]
use bytes::Bytes;
use openssl::{
    pkey::{PKey, Private},
    ssl::{SslConnector, SslConnectorBuilder, SslMethod, SslVersion},
    x509::X509,
};

#[cfg(feature = "__integration")]
use crate::mqtt_proto::Packet;

// Re-export some types from `async_tungstenite` for use in the current API.
// TODO: Consider a more elegant solution in the future.
#[cfg(feature = "websockets")]
pub use async_tungstenite::tungstenite::{
    client::{ClientRequestBuilder as WsRequestBuilder, IntoClientRequest as IntoWsRequest},
    handshake::client::Request as WsRequest,
    http::Uri as WsUri,
};

/// Parameters for establishing a new MQTT connection at transport layer.
pub struct ConnectionTransportConfig {
    /// Transport protocol and destination.
    pub transport_type: ConnectionTransportType,
    /// Optional timeout for transport establishment, including proxy, TLS, and WebSocket setup.
    ///
    /// This does not time out the MQTT CONNECT response; use the `response_timeout` argument on
    /// [`crate::client::ConnectHandle::connect`] for that phase.
    pub timeout: Option<Duration>,
    /// Optional HTTP or HTTPS CONNECT proxy.
    pub proxy: Option<Proxy>,
    /// Whether to disable Nagle's algorithm (`TCP_NODELAY`) on the underlying TCP socket.
    /// Setting this to `true` reduces latency for small, frequent packets at the cost of slightly
    /// more packet overhead.
    pub tcp_nodelay: bool, // TODO: Make this a defaultable SocketOptions
}

/// The type of transport to use for the new MQTT connection.
///
/// # Examples
///
/// Plain TCP:
///
/// ```
/// use ms_mqtt_client::transport::ConnectionTransportType;
///
/// let transport = ConnectionTransportType::Tcp {
///     hostname: "localhost".into(),
///     port: 1883,
/// };
/// ```
///
/// TLS with a PEM-encoded CA trust bundle:
///
/// ```no_run
/// use std::io;
///
/// use ms_mqtt_client::transport::{ConnectionTransportType, TlsConfig};
///
/// fn tls_transport(ca_trust_bundle: &[u8]) -> io::Result<ConnectionTransportType> {
///     Ok(ConnectionTransportType::Tls {
///         hostname: "mqtt.example.com".into(),
///         port: 8883,
///         tls_config: TlsConfig::from_pem(None, ca_trust_bundle)?,
///     })
/// }
/// ```
///
/// `WebSockets` require the `websockets` crate feature. Use a `ws` URI with no TLS configuration,
/// or a `wss` URI with a [`TlsConfig`]:
///
/// ```no_run
/// # #[cfg(feature = "websockets")]
/// # fn websocket_transport(ca_trust_bundle: &[u8]) -> Result<ms_mqtt_client::transport::ConnectionTransportType, Box<dyn std::error::Error>> {
/// use ms_mqtt_client::transport::{
///     ConnectionTransportType, IntoWsRequest as _, TlsConfig,
/// };
///
/// Ok(ConnectionTransportType::Ws {
///     request: "wss://mqtt.example.com:443/mqtt".into_client_request()?,
///     tls_config: Some(TlsConfig::from_pem(None, ca_trust_bundle)?),
/// })
/// # }
/// ```
// The `Ws` variant is large (it holds a full HTTP request), but it is feature-gated and
// constructed at most once per connection, so the size difference is not worth boxing.
#[allow(clippy::large_enum_variant)]
pub enum ConnectionTransportType {
    /// Unencrypted MQTT over TCP.
    Tcp { hostname: String, port: u16 },
    /// MQTT over TLS.
    Tls {
        hostname: String,
        port: u16,
        tls_config: TlsConfig,
    },
    /// MQTT over `WebSockets`. Available with the `websockets` crate feature.
    #[cfg(feature = "websockets")]
    #[doc(alias = "websocket")]
    #[doc(alias = "wss")]
    Ws {
        request: WsRequest,
        tls_config: Option<TlsConfig>,
    },
    #[cfg(feature = "__integration")]
    Test {
        incoming_packets: tokio::sync::mpsc::UnboundedReceiver<Packet<Bytes>>,
        outgoing_packets: tokio::sync::mpsc::UnboundedSender<Packet<Bytes>>,
    },
}

/// Proxy configuration for the connection.
/// Only supports static authentication, not challenge-based authentication
pub struct Proxy {
    pub endpoint: ProxyEndpoint,
    pub auth: ProxyAuthorization,
}

/// Proxy endpoint configuration, indicating the protocol to use to connect to the proxy
pub enum ProxyEndpoint {
    Http {
        hostname: String,
        port: u16,
    },
    Https {
        hostname: String,
        port: u16,
        tls_config: TlsConfig,
    },
    // TODO: SOCKS5?
}

/// Value that will be sent in the Proxy-Authorization header when connecting through a proxy
pub enum ProxyAuthorization {
    None,
    Basic { username: String, password: String },
    // TODO: custom
}

/// Parameters for establishing a TLS connection.
pub struct TlsConfig(pub(crate) SslConnectorBuilder);

impl TlsConfig {
    /// Constructs a [`TlsConfig`] with the given client certificate and CA trust bundle.
    ///
    /// The client certificate is specified as a tuple of the main client cert, its private key,
    /// and a list of zero or more chain certs that should be sent along with the main cert.
    pub fn new(
        client_cert: Option<(X509, PKey<Private>, Vec<X509>)>,
        ca_trust_bundle: Vec<X509>,
    ) -> io::Result<Self> {
        let mut connector = SslConnector::builder(SslMethod::tls_client())?;

        connector.set_min_proto_version(Some(SslVersion::TLS1_2))?;

        if let Some((cert, pkey, cert_chain)) = client_cert {
            connector.set_certificate(&cert)?;
            connector.set_private_key(&pkey)?;
            for cert in cert_chain {
                connector.add_extra_chain_cert(cert)?;
            }
        }

        if !ca_trust_bundle.is_empty() {
            let cert_store = connector.cert_store_mut();
            for cert in ca_trust_bundle {
                cert_store.add_cert(cert)?;
            }
        }

        Ok(Self(connector))
    }

    /// Constructs a [`TlsConfig`] with the client certificate and CA trust bundle
    /// parsed from the given PEM blobs.
    ///
    /// The client certificate is specified as a one blob containing the PEM-encoded cert chain
    /// (main cert followed by other certs in the chain) and one blob containing the PEM-encoded private key.
    pub fn from_pem(
        client_cert: Option<(&[u8], &[u8])>,
        ca_trust_bundle: &[u8],
    ) -> io::Result<Self> {
        let client_cert = if let Some((cert, pkey)) = client_cert {
            let mut client_cert_chain = X509::stack_from_pem(cert)?;
            client_cert_chain.reverse();
            let client_cert = client_cert_chain.pop().ok_or_else(|| {
                io::Error::other("client cert PEM does not contain any certificates")
            })?;
            client_cert_chain.reverse();

            let pkey = PKey::private_key_from_pem(pkey)?;

            Some((client_cert, pkey, client_cert_chain))
        } else {
            None
        };

        let ca_trust_bundle = X509::stack_from_pem(ca_trust_bundle)?;

        Self::new(client_cert, ca_trust_bundle)
    }
}

impl From<SslConnectorBuilder> for TlsConfig {
    fn from(connector: SslConnectorBuilder) -> Self {
        Self(connector)
    }
}
