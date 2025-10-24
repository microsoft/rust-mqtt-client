// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Structs and types that together provide the MQTT client functionality.

// TODO: Remove when possible.
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(clippy::unused_async)]

use std::pin::pin;

use bytes::Bytes;
use futures_util::future::{self, FutureExt as _};

use crate::buffer_pool::{BufferPool, BufferPoolImpl, OwnedImpl, SharedImpl};
use crate::client::token::{
    CompletionToken, PubAckToken, PubRecToken, PubRelToken, completion_pair,
};
use crate::client::{
    channel_data::{AuthRequest, DisconnectRequest, PublishRequest, SubscriptionRequest},
    session::{CompletedOperation, Session},
};
use crate::error::ClientError;
use crate::io::{Reader, Writer};
use crate::mqtt_proto::{
    // TODO: this gets too confusing with packet types. Can we abstract these away somehow?
    Connect,
    ConnectOtherProperties,
    KeepAlive,
    Packet,
    ProtocolVersion,
    SessionExpiryInterval,
};
use crate::packet::{
    Auth, AuthProperties, AuthReason, AuthenticationInfo, ConnAck, ConnectProperties, Disconnect,
    DisconnectProperties, IntoBuffered, PacketIdentifier, PubAck, PubRec, Publish,
    PublishProperties, QoS, SubAck, SubscribeProperties, UnsubAck, UnsubscribeProperties,
};
use crate::topic::{TopicFilter, TopicName};

mod channel_data;
mod session;
pub mod token;

// TODO: What should this module and factory function be called?
// The three components are the client collectively - so what should the outbound struct (currently called the Client) be?
// Should it be MqttSender or something? Or are we fine with the duplicate semantic?
// Alternatively, maybe we break up connect/disconnect/auth into a separate fourth component?

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
    /// MQTT Client Identifier
    pub client_id: String,
    /// Maximum size of the outgoing message queue
    pub queue_size: usize,
    // Any other options can be added here, but there really ought not be many.
    // TODO: Use a builder pattern?
    // TODO: How to represent authentication options?
}

/// Parameters for establishing a new connection.
pub enum ConnectionTransportConfig {
    Tcp {
        hostname: String,
        port: u16,
    },
    Tls {
        hostname: String,
    },
    Ws {
        request: async_tungstenite::tungstenite::handshake::client::Request,
    },
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
    pub_tx: tokio::sync::mpsc::Sender<PublishRequest>,
    /// Channel that transmits outgoing SUBSCRIBE/UNSUBSCRIBE requests
    sub_tx: tokio::sync::mpsc::Sender<SubscriptionRequest>,
}

impl Client {
    /// Sends a PUBLISH packet to the broker at QoS 0.
    ///
    /// Returns a token that can be awaited for confirmation of the PUBLISH being sent.
    pub async fn publish_qos0(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        properties: PublishProperties,
    ) -> Result<CompletionToken<()>, ClientError> {
        let (notifier, token) = completion_pair();
        self.pub_tx
            .send(PublishRequest::PublishQoS0(
                notifier, topic_name, payload, properties,
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }

    /// Sends a PUBLISH packet to the broker at QoS 1
    ///
    /// Returns a token that can be awaited to receive the PUBACK response packet.
    pub async fn publish_qos1(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        properties: PublishProperties,
    ) -> Result<CompletionToken<PubAck>, ClientError> {
        let (notifier, token) = completion_pair();
        self.pub_tx
            .send(PublishRequest::PublishQoS1(
                notifier, topic_name, payload, properties,
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }

    /// Sends a PUBLISH packet to the broker at QoS 2
    ///
    /// Returns a token that can be awaited to receive the PUBREC response packet and optionally a
    /// `PubRelToken` for sending a PUBREL packet if the PUBREC response indicates a success.
    pub async fn publish_qos2(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        properties: PublishProperties,
    ) -> Result<CompletionToken<(PubRec, Option<PubRelToken>)>, ClientError> {
        let (notifier, token) = completion_pair();
        self.pub_tx
            .send(PublishRequest::PublishQoS2(
                notifier, topic_name, payload, properties,
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }

    /// Send a SUBSCRIBE packet to the broker.
    ///
    /// Returns a token that can be awaited to receive the SUBACK response packet.
    pub async fn subscribe(
        &self,
        topic_filter: TopicFilter,
        qos: QoS,
        properties: SubscribeProperties,
    ) -> Result<CompletionToken<SubAck>, ClientError> {
        let (notifier, token) = completion_pair();
        self.sub_tx
            .send(SubscriptionRequest::Subscribe(
                notifier,
                topic_filter,
                qos,
                properties,
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }

    /// Send an UNSUBSCRIBE packet to the broker.
    ///
    /// Returns a token that can be awaited to receive the UNSUBACK response packet.
    pub async fn unsubscribe(
        &self,
        topic_filter: TopicFilter,
        properties: UnsubscribeProperties,
    ) -> Result<CompletionToken<UnsubAck>, ClientError> {
        let (notifier, token) = completion_pair();
        self.sub_tx
            .send(SubscriptionRequest::Unsubscribe(
                notifier,
                topic_filter,
                properties,
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }
}

/// Receives incoming Application Messages as `Publish`es.
pub struct Receiver {
    /// Channel for receiving incoming PUBLISH packets
    rx: tokio::sync::mpsc::UnboundedReceiver<(Publish, AckHandle)>,
}
impl Receiver {
    /// Receive an incoming `Publish`, and any `AckToken` that may be associated with it.
    ///
    /// `AckToken` will only be present if the Publish has a QoS of 1 or 2.
    ///
    /// Receiving None indicates that the client has been dropped, and no more messages will be received.
    pub async fn recv(&mut self) -> Option<(Publish, AckHandle)> {
        self.rx.recv().await
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
        properties: ConnectProperties,
        authentication_info: AuthenticationInfo,
    ) -> AuthResponse {
        // TODO: Even with enhanced auth, we may need skip the intermediate auth step if we get a connack
        let (mut reader, mut writer) = self.transport_connect(connection_transport).await;
        self.mqtt_connect(&mut writer, properties).await;

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
        properties: ConnectProperties,
    ) -> (Connection, ConnAck, DisconnectHandle) {
        let (mut reader, mut writer) = self.transport_connect(connection_transport).await;
        self.mqtt_connect(&mut writer, properties).await;
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

            ConnectionTransportConfig::Tls { hostname } => {
                crate::io::tokio_tls::connect(&hostname, &self.reader_pool, &self.writer_pool)
                    .await
                    .expect("TODO: error handling")
            }

            ConnectionTransportConfig::Ws { request } => {
                crate::io::tokio_ws::connect(request, &self.reader_pool)
                    .await
                    .expect("TODO: error handling")
            }
        }
    }

    async fn mqtt_connect(
        &self,
        writer: &mut Writer<BufferPoolImpl>,
        properties: ConnectProperties,
    ) {
        // Transport has been established. Send CONNECT and wait for CONNACK.
        // TODO: Get values from properties
        let connect = Packet::Connect(Connect {
            username: None,
            password: None,
            will: None,
            client_id: None,
            clean_start: true,
            keep_alive: KeepAlive::Infinite,
            other_properties: ConnectOtherProperties {
                session_expiry_interval: SessionExpiryInterval::Infinite,
                ..Default::default()
            },
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
            .into_buffered(&mut self.session.owned)
            .expect("TODO: error handling"),
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

pub struct DisconnectHandle(tokio::sync::oneshot::Sender<DisconnectRequest>);

impl DisconnectHandle {
    pub fn disconnect(self, properties: DisconnectProperties) -> Result<(), ClientError> {
        self.0
            .send(DisconnectRequest(properties))
            .map_err(|_| ClientError::DetachedClient)
    }
}

// TODO: Determine where some of these auth structures should live, and what a token vs. handle is semantically.

pub struct ReauthHandle {
    method: String,
    tx: tokio::sync::mpsc::Sender<AuthRequest>,
}

impl ReauthHandle {
    pub async fn reauth(
        &self,
        authentication_data: Option<Bytes>,
        properties: AuthProperties,
    ) -> Result<CompletionToken<ReauthResponse>, ClientError> {
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
            .send(AuthRequest::Reauth(notifier, auth))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }
}

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

// TODO: Should this live in token module? Probably, but is the module even a good idea at this point?
pub struct ReauthToken {
    method: String,
    tx: tokio::sync::mpsc::Sender<AuthRequest>,
}

impl ReauthToken {
    pub async fn continue_reauth(
        self,
        authentication_data: Option<Bytes>,
        properties: AuthProperties,
    ) -> Result<CompletionToken<ReauthResponse>, ClientError> {
        let (notifier, token) = completion_pair();
        let auth = Auth {
            reason: AuthReason::ContinueAuthentication,
            authentication_info: Some(AuthenticationInfo {
                method: self.method,
                data: authentication_data,
            }),
            properties,
        };
        self.tx
            .send(AuthRequest::Reauth(notifier, auth))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }
}

/// Details about a client disconnect
pub enum DisconnectedEvent {
    Transport,
    UserRequested,
    ServerRequested(Disconnect),
}

// TODO: where should this live?
pub enum AckHandle {
    QoS0,
    QoS1(PubAckToken),
    QoS2(PubRecToken),
}
