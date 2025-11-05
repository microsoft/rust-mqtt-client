// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Structs and types that together provide the MQTT client functionality.

// TODO: Remove when possible.
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(clippy::unused_async)]

use std::future::Future;
use std::io;
use std::pin::pin;

use bytes::Bytes;
use futures_util::future::{self, FutureExt as _};
use openssl::{
    pkey::{PKey, Private},
    ssl::{SslConnector, SslConnectorBuilder, SslMethod, SslVersion},
    x509::X509,
};

use crate::buffer_pool::{BufferPool, BufferPoolImpl, OwnedImpl, SharedImpl};
use crate::client::token::completion::buffered::{CompletionToken, completion_pair};
use crate::client::{
    channel_data::{
        DisconnectRequest, IncomingPublish, PublishRequest, ReauthRequest, SubscriptionRequest,
    },
    session::{CompletedOperation, Session},
};
use crate::error::ClientError;
use crate::io::{Reader, Writer};
use crate::mqtt_proto::{
    self,
    // TODO: this gets too confusing with packet types. Can we abstract these away somehow?
    Connect,
    DisconnectOtherProperties,
    DisconnectReasonCode,
    Packet,
    ProtocolVersion,
};
use crate::packet::{
    Auth, AuthProperties, AuthReason, AuthenticationInfo, ConnAck, ConnectOptions,
    ConnectProperties, Disconnect, DisconnectProperties, KeepAlive, PacketIdentifier, PubAck,
    PubAckProperties, PubComp, PubCompProperties, PubRec, PubRecProperties, PubRejectReason,
    PubRel, PubRelProperties, Publish, PublishProperties, QoS, RetainHandling, SubAck,
    SubscribeProperties, UnsubAck, UnsubscribeProperties,
};
use crate::topic::{TopicFilter, TopicName};

// TODO: What should this module and factory function be called?
// The three components are the client collectively - so what should the outbound struct (currently called the Client) be?
// Should it be MqttSender or something? Or are we fine with the duplicate semantic?
// Alternatively, maybe we break up connect/disconnect/auth into a separate fourth component?

macro_rules! make_completion_token_ty {
    ($vis:vis struct $token_ty:ident $( < $($ty_param_name:ident : $ty_param_bound:path ),* > )? (CompletionToken< $element_ty:ty >)) => {
        #[derive(Debug)]
        $vis struct $token_ty $(< $($ty_param_name : $ty_param_bound),* >)? (pub(crate) CompletionToken<$element_ty>);

        impl $(< $($ty_param_name : $ty_param_bound),* >)? std::future::Future for $token_ty $(< $($ty_param_name ),* >)? {
            type Output = Result<$element_ty, $crate::client::token::completion::CompletionError>;

            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                std::pin::Pin::new(&mut self.0).poll(cx)
            }
        }
    };

    ($vis:vis struct $token_ty:ident (CompletionToken< $original_element_ty:ty > -> $element_ty:ty $map_fn:block )) => {
        #[derive(Debug)]
        $vis struct $token_ty(pub(crate) CompletionToken<$original_element_ty>);

        impl std::future::Future for $token_ty {
            type Output = Result<$element_ty, $crate::client::token::completion::CompletionError>;

            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                match std::pin::Pin::new(&mut self.0).poll(cx) {
                    std::task::Poll::Ready(Ok(value)) => {
                        std::task::Poll::Ready(Ok(($map_fn)(value)))
                    }
                    std::task::Poll::Ready(Err(_)) => {
                        std::task::Poll::Ready(Err($crate::client::token::completion::CompletionError::Detatched))
                    }
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            }
        }
    };
}

mod channel_data;

mod session;

pub mod token;
use token::{PubAckCompletionToken, PubCompConfirmCompletionToken, PubRecRejectCompletionToken};

/// Creates the three components needed to run the MQTT client
#[allow(clippy::needless_pass_by_value)] // TODO: Remove when implemented
pub fn new_client(options: ClientOptions) -> (Client, ConnectHandle, Receiver) {
    // NOTE: We use size 1 channels for outgoing data to avoid buffering packets that are not yet
    // owned by the internal session state. If this becomes a performance bottleneck, revisit.
    let (o_pub_tx, o_pub_rx) = tokio::sync::mpsc::channel(1);
    let (sub_tx, sub_rx) = tokio::sync::mpsc::channel(1);
    let (ack_tx, ack_rx) = tokio::sync::mpsc::channel(1);
    let (auth_tx, auth_rx) = tokio::sync::mpsc::channel(1);
    // NOTE: We use an unbounded channel for incoming publishes, as messages read off the network must go
    // somewhere.
    let (i_pub_tx, i_pub_rx) = tokio::sync::mpsc::unbounded_channel();
    let client = Client {
        pub_tx: o_pub_tx,
        sub_tx,
    };
    let reader_pool = BufferPoolImpl;
    let writer_pool = BufferPoolImpl;
    let owned = writer_pool.take_empty_owned();
    let session = Session::new(
        sub_rx,
        o_pub_rx,
        ack_rx,
        auth_rx,
        i_pub_tx,
        ack_tx,
        auth_tx,
        PacketIdentifier::new(100).expect("100 is always okay"), // TODO: customizable
        owned,
    );
    let connect_handle = ConnectHandle {
        session,
        reader_pool,
        writer_pool,
    };
    let receiver = Receiver { rx: i_pub_rx };
    (client, connect_handle, receiver)
}

/// Options for configuring the MQTT client
pub struct ClientOptions {
    /// MQTT Client Identifier. If None, the MQTT server will assign one.
    pub client_id: Option<String>,
    /// Maximum size of the outgoing message queue
    pub queue_size: usize,
    // Any other options can be added here, but there really ought not be many.
    // TODO: Use a builder pattern?
}

/// Parameters for establishing a new connection.
pub enum ConnectionTransportConfig {
    Tcp {
        hostname: String,
        port: u16,
    },
    Tls {
        hostname: String,
        port: u16,
        config: ConnectionTransportTlsConfig,
    },
    Ws {
        request: async_tungstenite::tungstenite::handshake::client::Request,
        tls_config: ConnectionTransportTlsConfig,
    },
}

/// Parameters for establishing a TLS connection.
pub struct ConnectionTransportTlsConfig(pub(crate) SslConnectorBuilder);

impl ConnectionTransportTlsConfig {
    /// Constructs a [`ConnectionTransportTlsConfig`] with the given client certificate and CA trust bundle.
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

    /// Constructs a [`ConnectionTransportTlsConfig`] with the client certificate and CA trust bundle
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
            if client_cert_chain.is_empty() {
                return Err(io::Error::other(
                    "client cert PEM does not contain any certificates",
                ));
            }
            let client_cert = client_cert_chain.remove(0);

            let pkey = PKey::private_key_from_pem(pkey)?;

            Some((client_cert, pkey, client_cert_chain))
        } else {
            None
        };

        let ca_trust_bundle = X509::stack_from_pem(ca_trust_bundle)?;

        Self::new(client_cert, ca_trust_bundle)
    }
}

impl From<SslConnectorBuilder> for ConnectionTransportTlsConfig {
    fn from(connector: SslConnectorBuilder) -> Self {
        Self(connector)
    }
}

// TODO: I don't like the naming of this as Client.
// MQTTHandle? Sender? OperationsInterface? Outgoing?

/// Sends outgoing data.
#[derive(Clone)]
#[allow(clippy::struct_field_names)]
pub struct Client {
    // NOTE: We use different channels for publishes vs. control packets to allow for
    // prioritization of operations by the receiver.
    /// Channel that transmits outgoing PUBLISH requests
    pub_tx: tokio::sync::mpsc::Sender<PublishRequest<SharedImpl>>,
    /// Channel that transmits outgoing SUBSCRIBE/UNSUBSCRIBE requests
    sub_tx: tokio::sync::mpsc::Sender<SubscriptionRequest<SharedImpl>>,
}

impl Client {
    /// Sends a PUBLISH packet to the broker at QoS 0.
    ///
    /// Returns a token that can be awaited for confirmation of the PUBLISH being sent.
    pub async fn publish_qos0(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        retain: bool,
        properties: PublishProperties,
    ) -> Result<PublishQoS0CompletionToken, ClientError> {
        let (notifier, token) = completion_pair();
        self.pub_tx
            .send(PublishRequest::PublishQoS0(
                notifier,
                topic_name.into_inner().into(),
                payload.into(),
                retain,
                properties.into(),
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(PublishQoS0CompletionToken(token))
    }

    /// Sends a PUBLISH packet to the broker at QoS 1
    ///
    /// Returns a token that can be awaited to receive the PUBACK response packet.
    pub async fn publish_qos1(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        retain: bool,
        properties: PublishProperties,
    ) -> Result<PublishQoS1CompletionToken, ClientError> {
        let (notifier, token) = completion_pair();
        self.pub_tx
            .send(PublishRequest::PublishQoS1(
                notifier,
                topic_name.into_inner().into(),
                payload.into(),
                retain,
                properties.into(),
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(PublishQoS1CompletionToken(token))
    }

    /// Sends a PUBLISH packet to the broker at QoS 2
    ///
    /// Returns a token that can be awaited to receive the PUBREC response packet and optionally a
    /// `PubRelToken` for sending a PUBREL packet if the PUBREC response indicates a success.
    pub async fn publish_qos2(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        retain: bool,
        properties: PublishProperties,
    ) -> Result<PublishQoS2CompletionToken, ClientError> {
        let (notifier, token) = completion_pair();
        self.pub_tx
            .send(PublishRequest::PublishQoS2(
                notifier,
                topic_name.into_inner().into(),
                payload.into(),
                retain,
                properties.into(),
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(PublishQoS2CompletionToken(token))
    }

    /// Send a SUBSCRIBE packet to the broker.
    ///
    /// Returns a token that can be awaited to receive the SUBACK response packet.
    pub async fn subscribe(
        &self,
        topic_filter: TopicFilter,
        max_qos: QoS,
        no_local: bool,
        retain_as_published: bool,
        retain_handling: RetainHandling,
        properties: SubscribeProperties,
    ) -> Result<SubscribeCompletionToken, ClientError> {
        let (notifier, token) = completion_pair();

        let options = mqtt_proto::SubscribeOptions {
            maximum_qos: max_qos.into(),
            other_properties: mqtt_proto::SubscribeOptionsOtherProperties {
                no_local,
                retain_as_published,
                retain_handling,
            },
        };

        self.sub_tx
            .send(SubscriptionRequest::Subscribe(
                notifier,
                topic_filter.into_inner().into(),
                options,
                properties.into(),
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(SubscribeCompletionToken(token))
    }

    /// Send an UNSUBSCRIBE packet to the broker.
    ///
    /// Returns a token that can be awaited to receive the UNSUBACK response packet.
    pub async fn unsubscribe(
        &self,
        topic_filter: TopicFilter,
        properties: UnsubscribeProperties,
    ) -> Result<UnsubscribeCompletionToken, ClientError> {
        let (notifier, token) = completion_pair();
        self.sub_tx
            .send(SubscriptionRequest::Unsubscribe(
                notifier,
                topic_filter.into_inner().into(),
                properties.into(),
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(UnsubscribeCompletionToken(token))
    }
}

make_completion_token_ty!(pub struct PublishQoS0CompletionToken(CompletionToken<()>));

make_completion_token_ty!(pub struct PublishQoS1CompletionToken(CompletionToken<crate::mqtt_proto::PubAck<SharedImpl>> -> PubAck { Into::into }));

make_completion_token_ty!(pub struct PublishQoS2CompletionToken(
    CompletionToken<(
        crate::mqtt_proto::PubRec<SharedImpl>,
        Option<token::PubRelToken<SharedImpl>>,
    )> -> (
        PubRec,
        Option<PubRelToken>,
    ) {
        |(pubrec, token): (_, Option<_>)| (PubRec::from(pubrec), token.map(PubRelToken))
    }
));

make_completion_token_ty!(pub struct SubscribeCompletionToken(CompletionToken<crate::mqtt_proto::SubAck<SharedImpl>> -> SubAck { Into::into }));

make_completion_token_ty!(pub struct UnsubscribeCompletionToken(CompletionToken<crate::mqtt_proto::UnsubAck<SharedImpl>> -> UnsubAck { Into::into }));

/// Receives incoming Application Messages as `Publish`es.
pub struct Receiver {
    /// Channel for receiving incoming PUBLISH packets
    rx: tokio::sync::mpsc::UnboundedReceiver<IncomingPublish<SharedImpl>>,
}

impl Receiver {
    /// Receive an incoming `Publish`, and any `AckToken` that may be associated with it.
    ///
    /// `AckToken` will only be present if the Publish has a QoS of 1 or 2.
    ///
    /// Receiving None indicates that the client has been dropped, and no more messages will be received.
    pub async fn recv(&mut self) -> Option<(Publish, AckHandle)> {
        self.rx
            .recv()
            .await
            .map(|(publish, ack_handle)| (publish.into(), ack_handle.into()))
    }
}

/// Handle providing MQTT CONNECT functionality.
pub struct ConnectHandle {
    session: Session<OwnedImpl>,
    reader_pool: BufferPoolImpl,
    writer_pool: BufferPoolImpl,
}

impl ConnectHandle {
    pub async fn connect_enhanced_auth(
        mut self,
        connection_transport: ConnectionTransportConfig,
        clean_start: bool,
        keep_alive: KeepAlive,
        options: ConnectOptions,
        properties: ConnectProperties,
        authentication_info: AuthenticationInfo,
    ) -> AuthResponse {
        // TODO: Even with enhanced auth, we may need skip the intermediate auth step if we get a connack
        let (mut reader, mut writer) = self.transport_connect(connection_transport).await;
        self.mqtt_connect(&mut writer, clean_start, keep_alive, options, properties)
            .await;

        match self.mqtt_receive(&mut reader).await {
            Packet::ConnAck(connack) => {
                self.session.incoming_connack(connack.clone());

                if connack.is_success() {
                    let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel();
                    let auth_tx = self.session.ch.auth_tx.clone();
                    self.session.ch.disconnect_rx = Some(disconnect_rx);
                    let connection = Connection {
                        session: self.session,
                        reader_pool: self.reader_pool,
                        writer_pool: self.writer_pool,
                        reader,
                        writer,
                    };
                    AuthResponse::Success(
                        connection,
                        connack.into(),
                        ReauthHandle {
                            method: authentication_info.method,
                            tx: auth_tx,
                        },
                        DisconnectHandle(disconnect_tx),
                    )
                } else {
                    AuthResponse::Failure(self, Some(connack.into()))
                }
            }
            Packet::Auth(auth) => {
                let auth_handle = AuthHandle {
                    session: self.session,
                    reader_pool: self.reader_pool,
                    writer_pool: self.writer_pool,
                    reader,
                    writer,
                    auth_method: authentication_info.method,
                };
                AuthResponse::Continue(auth.into(), auth_handle)
            }
            _ => panic!("TODO: error handling"),
        }
    }

    // TODO: Return something like Result<(Connection, ConnAck, DisconnectHandle), (ConnectHandle, ConnAck)>
    pub async fn connect(
        mut self,
        connection_transport: ConnectionTransportConfig,
        clean_start: bool,
        keep_alive: KeepAlive,
        options: ConnectOptions,
        properties: ConnectProperties,
    ) -> (Connection, ConnAck, DisconnectHandle) {
        let (mut reader, mut writer) = self.transport_connect(connection_transport).await;
        self.mqtt_connect(&mut writer, clean_start, keep_alive, options, properties)
            .await;
        let Packet::ConnAck(connack) = self.mqtt_receive(&mut reader).await else {
            panic!("TODO: error handling");
        };
        self.session.incoming_connack(connack.clone());
        let connack = connack.into();

        let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel();

        self.session.ch.disconnect_rx = Some(disconnect_rx);
        (
            Connection {
                session: self.session,
                reader_pool: self.reader_pool,
                writer_pool: self.writer_pool,
                reader,
                writer,
            },
            connack,
            DisconnectHandle(disconnect_tx),
        )
    }

    async fn transport_connect(
        &self,
        transport_config: ConnectionTransportConfig,
    ) -> (Reader<BufferPoolImpl>, Writer<BufferPoolImpl>) {
        match transport_config {
            ConnectionTransportConfig::Tcp { hostname, port } => crate::io::tokio_tcp::connect(
                (hostname, port),
                &self.reader_pool,
                &self.writer_pool,
            )
            .await
            .expect("TODO: error handling"),

            ConnectionTransportConfig::Tls {
                hostname,
                port,
                config,
            } => crate::io::tokio_tls::connect(
                &hostname,
                port,
                config,
                &self.reader_pool,
                &self.writer_pool,
            )
            .await
            .expect("TODO: error handling"),

            ConnectionTransportConfig::Ws {
                request,
                tls_config,
            } => crate::io::tokio_ws::connect(request, tls_config, &self.reader_pool)
                .await
                .expect("TODO: error handling"),
        }
    }

    async fn mqtt_connect(
        &self,
        writer: &mut Writer<BufferPoolImpl>,
        clean_start: bool,
        keep_alive: KeepAlive,
        options: ConnectOptions,
        properties: ConnectProperties,
    ) {
        // Transport has been established. Send CONNECT and wait for CONNACK.
        // TODO: Get values from options
        let connect = Packet::Connect(Connect {
            username: None, // TODO from options
            password: None, // TODO from options
            will: None,
            client_id: None, // TODO from client-wide config
            clean_start,
            keep_alive,
            other_properties: properties.into(),
        });
        writer
            .write(&connect, ProtocolVersion::V5)
            .await
            .expect("TODO: error handling");
        writer.flush().await.expect("TODO: error handling");
    }

    async fn mqtt_receive(&self, reader: &mut Reader<BufferPoolImpl>) -> Packet<SharedImpl> {
        let mut raw_packet = reader.read().await.expect("TODO: error handling");
        Packet::decode(
            raw_packet.first_byte,
            &mut raw_packet.rest,
            ProtocolVersion::V5,
        )
        .expect("TODO: error handling")
    }
}

/// Handle for the intermediate step of an MQTT CONNECT with enhanced authentication.
pub struct AuthHandle {
    session: Session<OwnedImpl>,
    reader_pool: BufferPoolImpl,
    writer_pool: BufferPoolImpl,
    reader: Reader<BufferPoolImpl>,
    writer: Writer<BufferPoolImpl>,
    auth_method: String,
}

impl AuthHandle {
    pub async fn continue_auth(
        mut self,
        authentication_data: Option<Bytes>,
        properties: AuthProperties,
    ) -> AuthResponse {
        // Send auth
        let auth = Packet::Auth(
            Auth {
                reason: AuthReason::ContinueAuthentication,
                authentication_info: Some(AuthenticationInfo {
                    method: self.auth_method.clone(),
                    data: authentication_data,
                }),
                properties,
            }
            .into(),
        );
        self.writer
            .write(&auth, ProtocolVersion::V5)
            .await
            .expect("TODO: error handling");
        self.writer.flush().await.expect("TODO: error handling");

        // Wait for next response
        let mut raw_packet = self.reader.read().await.expect("TODO: error handling");
        let packet = Packet::decode(
            raw_packet.first_byte,
            &mut raw_packet.rest,
            ProtocolVersion::V5,
        )
        .expect("TODO: error handling");

        match packet {
            Packet::ConnAck(connack) => {
                self.session.incoming_connack(connack.clone());

                if connack.is_success() {
                    let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel();
                    let auth_tx = self.session.ch.auth_tx.clone();
                    self.session.ch.disconnect_rx = Some(disconnect_rx);
                    let connection = Connection {
                        session: self.session,
                        reader_pool: self.reader_pool,
                        writer_pool: self.writer_pool,
                        reader: self.reader,
                        writer: self.writer,
                    };
                    AuthResponse::Success(
                        connection,
                        connack.into(),
                        ReauthHandle {
                            method: self.auth_method.clone(),
                            tx: auth_tx,
                        },
                        DisconnectHandle(disconnect_tx),
                    )
                } else {
                    let connect_handle = ConnectHandle {
                        session: self.session,
                        reader_pool: self.reader_pool,
                        writer_pool: self.writer_pool,
                    };
                    AuthResponse::Failure(connect_handle, Some(connack.into()))
                }
            }
            Packet::Auth(auth) => AuthResponse::Continue(auth.into(), self),
            _ => panic!("TODO: error handling"),
        }
    }
}

/// Runs the MQTT client event loop, keeping the client operational.
pub struct Connection {
    session: Session<OwnedImpl>,
    reader_pool: BufferPoolImpl,
    writer_pool: BufferPoolImpl,
    reader: Reader<BufferPoolImpl>,
    writer: Writer<BufferPoolImpl>,
}

impl Connection {
    /// Drives this connection until it is disconnected.
    /// Packets will only be sent and received while this future is running.
    pub async fn run_until_disconnect(mut self) -> (ConnectHandle, DisconnectedEvent) {
        let (reader, writer) = (&mut self.reader, &mut self.writer);

        loop {
            // Check for outgoing packets from the session or incoming packets from the reader.
            let next = {
                let next_outgoing_packet_f = pin!(self.session.next_outgoing_packet());
                let read_f = pin!(reader.read());
                let f = future::select(next_outgoing_packet_f, read_f);
                match f.await {
                    future::Either::Left((packet, _)) => future::Either::Left(packet),
                    future::Either::Right((Ok(raw_packet), _)) => {
                        future::Either::Right(Ok(raw_packet))
                    }
                    future::Either::Right((Err(err), _)) => future::Either::Right(Err(err)),
                }
            };
            match next {
                // Outgoing packet from session
                future::Either::Left(mut packet) => {
                    let mut disconnect = false;
                    while let Some(packet_) = packet {
                        if let Packet::Disconnect(disconnect_) = &packet_ {
                            disconnect = true;
                            self.session.client_disconnect(disconnect_);
                        }
                        writer
                            .write(&packet_, ProtocolVersion::V5)
                            .await
                            .expect("TODO: error handling");
                        if disconnect {
                            break;
                        }
                        packet = self.session.next_outgoing_packet().now_or_never().flatten();
                    }
                    writer.flush().await.expect("TODO: error handling");
                    // If we wrote a DISCONNECT packet, also close the connection.
                    if disconnect {
                        return (
                            ConnectHandle {
                                session: self.session,
                                reader_pool: self.reader_pool,
                                writer_pool: self.writer_pool,
                            },
                            DisconnectedEvent::UserRequested,
                        );
                    }
                }

                // Incoming packet from reader
                future::Either::Right(Ok(mut raw_packet)) => {
                    let packet = Packet::decode(
                        raw_packet.first_byte,
                        &mut raw_packet.rest,
                        ProtocolVersion::V5,
                    )
                    .expect("TODO: error handling");

                    match packet {
                        Packet::SubAck(suback) => self
                            .session
                            .complete_inflight(CompletedOperation::Subscribe(suback)),

                        Packet::UnsubAck(unsuback) => self
                            .session
                            .complete_inflight(CompletedOperation::Unsubscribe(unsuback)),

                        Packet::PubAck(puback) => self
                            .session
                            .complete_inflight(CompletedOperation::PublishQoS1(puback)),

                        Packet::PubRec(pubrec) => self
                            .session
                            .complete_inflight(CompletedOperation::PublishQoS2(pubrec)),

                        Packet::Disconnect(disconnect) => {
                            self.session.server_disconnect(&disconnect);
                            return (
                                ConnectHandle {
                                    session: self.session,
                                    reader_pool: self.reader_pool,
                                    writer_pool: self.writer_pool,
                                },
                                DisconnectedEvent::ServerRequested(disconnect.into()),
                            );
                        }

                        Packet::Publish(publish) => self.session.incoming_publish(publish),

                        Packet::PingResp(_) => (),

                        packet => todo!("unhandled packet {packet:?}"),
                    }
                }

                future::Either::Right(Err(err)) => {
                    self.session.transport_disconnect(&err);
                    return (
                        ConnectHandle {
                            session: self.session,
                            reader_pool: self.reader_pool,
                            writer_pool: self.writer_pool,
                        },
                        DisconnectedEvent::Transport,
                    );
                }
            }
        }
    }
}

pub struct DisconnectHandle(tokio::sync::oneshot::Sender<DisconnectRequest<SharedImpl>>);

impl DisconnectHandle {
    pub fn disconnect(self, properties: &DisconnectProperties) -> Result<(), ClientError> {
        let DisconnectProperties {
            session_expiry_interval,
            reason_string,
            user_properties,
            server_reference,
        } = properties;
        let req = DisconnectRequest(crate::mqtt_proto::Disconnect {
            reason_code: DisconnectReasonCode::Normal, // TODO: Get from DisconnectProperties
            other_properties: DisconnectOtherProperties {
                session_expiry_interval: *session_expiry_interval,
                reason_string: reason_string.as_deref().map(Into::into),
                user_properties: user_properties
                    .iter()
                    .map(|(key, value)| (key.as_str().into(), value.as_str().into()))
                    .collect(),
                server_reference: server_reference.as_deref().map(Into::into),
            },
        });

        self.0.send(req).map_err(|_| ClientError::DetachedClient)
    }
}

// TODO: Determine where some of these auth structures should live, and what a token vs. handle is semantically.

pub struct ReauthHandle {
    method: String,
    tx: tokio::sync::mpsc::Sender<ReauthRequest<SharedImpl>>,
}

impl ReauthHandle {
    pub async fn reauth(
        &self,
        authentication_data: Option<Bytes>,
        properties: AuthProperties,
    ) -> Result<ReauthCompletionToken, ClientError> {
        let (notifier, token) = completion_pair();
        let auth = Auth {
            reason: AuthReason::Reauthenticate,
            authentication_info: Some(AuthenticationInfo {
                method: self.method.clone(),
                data: authentication_data,
            }),
            properties,
        };
        self.tx
            .send(ReauthRequest(notifier, auth.into()))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(ReauthCompletionToken(token))
    }
}

make_completion_token_ty!(pub struct ReauthCompletionToken(CompletionToken<channel_data::ReauthResponse<SharedImpl>> -> ReauthResponse { Into::into }));

// ConnectEnahncedAuthResult? implement unwrap?
pub enum AuthResponse {
    Continue(Auth, AuthHandle),
    Success(Connection, ConnAck, ReauthHandle, DisconnectHandle),
    Failure(ConnectHandle, Option<ConnAck>),
}

pub enum ReauthResponse {
    // TODO: should this be in channel data and merely re-exported?
    Continue(Auth, ReauthToken),
    Success(Auth),
    Failure, // Cannot provide Disconnect packet here because it is not guaranteed to be sent by server
}

impl From<channel_data::ReauthResponse<SharedImpl>> for ReauthResponse {
    fn from(token: channel_data::ReauthResponse<SharedImpl>) -> Self {
        match token {
            channel_data::ReauthResponse::Continue(auth, token) => {
                Self::Continue(auth.into(), ReauthToken(token))
            }
            channel_data::ReauthResponse::Success(auth) => Self::Success(auth.into()),
            channel_data::ReauthResponse::Failure => Self::Failure,
        }
    }
}

// TODO: Should this live in token module? Probably, but is the module even a good idea at this point?
pub struct ReauthToken(channel_data::ReauthToken<SharedImpl>);

impl ReauthToken {
    pub async fn continue_reauth(
        self,
        authentication_data: Option<Bytes>,
        properties: AuthProperties,
    ) -> Result<ReauthCompletionToken, ClientError> {
        let token = self
            .0
            .continue_reauth(
                authentication_data.as_deref().map(Into::into),
                properties.reason_string.as_deref().map(Into::into),
                crate::packet::map_user_properties_to_bytestr(properties.user_properties),
            )
            .await?;
        Ok(ReauthCompletionToken(token.0))
    }
}

/// Details about a client disconnect
pub enum DisconnectedEvent {
    Transport,
    UserRequested,
    ServerRequested(Disconnect),
}

pub enum AckHandle {
    QoS0,
    QoS1(PubAckToken),
    QoS2(PubRecToken),
}

impl From<token::AckHandle<SharedImpl>> for AckHandle {
    fn from(inner: token::AckHandle<SharedImpl>) -> Self {
        match inner {
            token::AckHandle::QoS0 => Self::QoS0,
            token::AckHandle::QoS1(token) => Self::QoS1(PubAckToken(token)),
            token::AckHandle::QoS2(token) => Self::QoS2(PubRecToken(token)),
        }
    }
}

#[derive(Debug)]
pub struct PubAckToken(token::PubAckToken<SharedImpl>);

impl PubAckToken {
    /// Accept the received PUBLISH by issuing a PUBACK indicating success.
    ///
    /// Consumes itself on call, so it cannot be used again.
    ///
    /// Returns once the PUBACK has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBACK is sent (*after* any ordering necessary).
    ///
    /// Can only be successfully used during the same connection epoch on which it was received.
    pub fn accept(
        self,
        properties: PubAckProperties,
    ) -> impl Future<Output = Result<PubAckCompletionToken, ClientError>> {
        self.0.accept(properties.into())
    }

    /// Reject the received PUBLISH by issuing a PUBACK with an error reason code.
    ///
    /// Consumes itself on call so it cannot be used again.
    ///
    /// Returns once the PUBACK has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBACK is sent (*after* any ordering necessary).
    pub fn reject(
        self,
        reason: PubRejectReason,
        properties: PubAckProperties,
    ) -> impl Future<Output = Result<PubAckCompletionToken, ClientError>> {
        self.0.reject(reason.into(), properties.into())
    }
}

#[derive(Debug)]
pub struct PubRecToken(token::PubRecToken<SharedImpl>);

impl PubRecToken {
    /// Accept the received PUBLISH by issuing a PUBREC indicating success.
    ///
    /// Consumes itself on call, so it cannot be used again.
    ///
    /// Returns once the PUBREC has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBREC is sent (*after* any ordering necessary).
    ///
    /// Can only be successfully used during the same session epoch on which it was received.
    pub async fn accept(
        self,
        properties: PubRecProperties,
    ) -> Result<PubRecAcceptCompletionToken, ClientError> {
        self.0
            .accept(properties.into())
            .await
            .map(|token| PubRecAcceptCompletionToken(token.0))
    }

    /// Reject the received PUBLISH by issuing a PUBREC with an error reason code.
    ///
    /// Consumes itself on call so it cannot be used again.
    ///
    /// Returns once the PUBREC has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBREC is sent (*after* any ordering necessary).
    ///
    /// Can only be successfully used during the same session epoch on which it was received.
    pub fn reject(
        self,
        reason: PubRejectReason,
        properties: PubRecProperties,
    ) -> impl Future<Output = Result<PubRecRejectCompletionToken, ClientError>> {
        self.0.reject(reason.into(), properties.into())
    }
}

make_completion_token_ty!(pub struct PubRecAcceptCompletionToken(
    CompletionToken<(
        crate::mqtt_proto::PubRel<SharedImpl>,
        token::PubCompToken<SharedImpl>,
    )> -> (
        PubRel,
        PubCompToken,
    ) {
        |(pubrel, pubcomp_token)| (PubRel::from(pubrel), PubCompToken(pubcomp_token))
    }
));

/// Token that allows the user to acknowledge a received PUBREC with a PUBREL (QoS 2).
#[derive(Debug)]
pub struct PubRelToken(token::PubRelToken<SharedImpl>);

impl PubRelToken {
    /// Confirm the PUBREC was received by issuing a PUBREL.
    ///
    /// Consumes itself on call so it cannot be used again.
    ///
    /// Returns once the PUBREL has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBREL is sent (*after* any ordering necessary).
    ///
    /// Can only be successfully used during the same session epoch on which it was received.
    pub async fn confirm(
        self,
        properties: PubRelProperties,
    ) -> Result<PubRelCompletionToken, ClientError> {
        self.0
            .confirm(properties.into())
            .await
            .map(|token| PubRelCompletionToken(token.0))
    }
}

make_completion_token_ty!(pub struct PubRelCompletionToken(CompletionToken<crate::mqtt_proto::PubComp<SharedImpl>> -> PubComp { Into::into }));

/// Token that allows the user to acknowledge a received PUBREL with a PUBCOMP (QoS 2).
#[derive(Debug)]
pub struct PubCompToken(token::PubCompToken<SharedImpl>);

impl PubCompToken {
    /// Confirm the PUBREL was received by issuing a PUBCOMP.
    ///
    /// Consumes itself on call so it cannot be used again.
    ///
    /// Returns once the PUBCOMP has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBCOMP is sent (*after* any ordering necessary).
    ///
    /// Can only be successfully used during the same session epoch on which it was received.
    pub fn confirm(
        self,
        properties: PubCompProperties,
    ) -> impl Future<Output = Result<PubCompConfirmCompletionToken, ClientError>> {
        self.0.confirm(properties.into())
    }
}
