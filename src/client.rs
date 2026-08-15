// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! MQTT client construction, outgoing operations, incoming publishes, and connection lifecycle.
//!
//! Start with [`new_client`], then use [`ConnectHandle::connect`] to establish a connection and
//! continuously drive [`Connection::run_until_disconnect`] while sending through [`Client`] or
//! receiving through [`Receiver`]. The crate-level documentation contains a complete lifecycle
//! example and explains operation completion.

// TODO: Remove when possible.
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(clippy::unused_async)]

use std::{future::Future, io, num::NonZeroU16, pin::pin, time::Duration};

use bytes::{Bytes, BytesMut};
use futures_util::future::{self, FutureExt as _};
use thiserror::Error;

use crate::buffer_pool::{BufferPool, BytesPool};
use crate::client::{
    channel_data::{
        DisconnectRequest, IncomingPublishAndToken, PublishRequestQoS0, PublishRequestQoS1QoS2,
        ReauthRequest, SubscriptionRequest,
    },
    session::{CompletedOperation, Session},
    timer::Timer,
    token::{
        acknowledgement::{PubAckToken, PubRecToken},
        completion::buffered::completion_pair,
        completion::{
            PublishQoS0CompletionToken, PublishQoS1CompletionToken, PublishQoS2CompletionToken,
            ReauthCompletionToken, SubscribeCompletionToken, UnsubscribeCompletionToken,
        },
        reauth::ReauthToken,
    },
};
use crate::error::{ConnectError, DetachedError, ProtocolError, ProtocolErrorRepr};
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
    Auth, AuthProperties, AuthReason, AuthenticationInfo, ConnAck, ConnectProperties, Disconnect,
    DisconnectProperties, KeepAlive, PacketIdentifier, Publish, PublishProperties, QoS,
    RetainOptions, SubscribeProperties, UnsubscribeProperties, Will,
};
use crate::topic::{TopicFilter, TopicName};
use crate::transport::{ConnectionTransportConfig, ConnectionTransportType};

// TODO: What should this module and factory function be called?
// The three components are the client collectively - so what should the outbound struct (currently called the Client) be?
// Should it be MqttSender or something? Or are we fine with the duplicate semantic?
// Alternatively, maybe we break up connect/disconnect/auth into a separate fourth component?

mod channel_data;
mod session;
mod timer;
pub mod token;

/// Creates the three independently owned components needed to run the MQTT client.
///
/// The returned [`Client`] submits outgoing operations, the [`ConnectHandle`] establishes and
/// re-establishes connections, and the [`Receiver`] yields incoming publishes. After connecting,
/// the application must continuously drive [`Connection::run_until_disconnect`]; the library does
/// not start a background connection task.
pub fn new_client(options: ClientOptions) -> (Client, ConnectHandle, Receiver) {
    let (o_pub_q12_tx, o_pub_q12_rx) =
        tokio::sync::mpsc::channel(options.publish_qos1_qos2_queue_size);
    let (o_pub_q0_tx, o_pub_q0_rx) = tokio::sync::mpsc::channel(options.publish_qos0_queue_size);
    // NOTE: We use size 1 channels for outgoing data that cannot be submitted from Drop to avoid
    // buffering packets that are not yet owned by the internal session state.
    let (sub_tx, sub_rx) = tokio::sync::mpsc::channel(1);
    let (auth_tx, auth_rx) = tokio::sync::mpsc::channel(1);
    // NOTE: We use an unbounded channel for acknowledgements, as there could be many ocurring simultaneously
    // and the fallback Drop implementation cannot await channel capacity without spawning many tasks/threads
    // in a way which severely affects performance.
    let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();
    // NOTE: We use an unbounded channel for incoming publishes, as messages read off the network must go
    // somewhere.
    let (i_pub_tx, i_pub_rx) = tokio::sync::mpsc::unbounded_channel();
    let client = Client {
        pub_qos0_tx: o_pub_q0_tx,
        pub_qos12_tx: o_pub_q12_tx,
        sub_tx,
    };
    let reader_pool = BytesPool;
    let writer_pool = BytesPool;
    let owned = writer_pool.take_empty_owned();
    let session = Session::new(
        sub_rx,
        o_pub_q0_rx,
        o_pub_q12_rx,
        ack_rx,
        auth_rx,
        i_pub_tx,
        ack_tx,
        auth_tx,
        options.max_packet_identifier,
        owned,
    );
    let connect_handle = ConnectHandle {
        session,
        reader_pool,
        writer_pool,
        cfg_client_id: options.client_id,
    };
    let receiver = Receiver { rx: i_pub_rx };
    (client, connect_handle, receiver)
}

/// Options for configuring the MQTT client
pub struct ClientOptions {
    /// MQTT Client Identifier. If None, the MQTT server will assign one.
    pub client_id: Option<String>,
    /// Maximum packet identifier
    pub max_packet_identifier: PacketIdentifier,
    /// Maximum size of the outgoing queue for QoS 0 PUBLISH packets.
    pub publish_qos0_queue_size: usize,
    /// Maximum size of the outgoing queue for QoS 1 and 2 PUBLISH packets.
    pub publish_qos1_qos2_queue_size: usize,
    // TODO: Consider using a Builder pattern?
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            client_id: None,
            max_packet_identifier: PacketIdentifier::MAX,
            publish_qos0_queue_size: 100,
            publish_qos1_qos2_queue_size: 100,
        }
    }
}

/// Configures whether and how a connection uses MQTT keep alive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeepAliveConfig {
    /// Requests no client keep alive. The server can still provide a keep-alive interval in
    /// CONNACK.
    Infinite,
    /// Requests a keep-alive interval. The server can replace this interval in CONNACK.
    Duration {
        /// Keep-alive interval in seconds, as encoded in the MQTT CONNECT packet.
        ping_after: NonZeroU16,
        /// Maximum time to wait for PINGRESP before ending the connection.
        response_timeout: Duration,
    },
}

impl From<KeepAliveConfig> for KeepAlive {
    fn from(value: KeepAliveConfig) -> Self {
        match value {
            KeepAliveConfig::Infinite => KeepAlive::Infinite,
            KeepAliveConfig::Duration {
                ping_after,
                response_timeout,
            } => KeepAlive::Duration(ping_after),
        }
    }
}

// TODO: I don't like the naming of this as Client.
// MQTTHandle? Sender? OperationsInterface? Outgoing?

/// Sends outgoing operations.
///
/// On success, an operation has been submitted to the client and a completion token is returned.
/// Awaiting the completion token observes the operation-specific completion event and may return
/// [`crate::error::CompletionError`] if the accepted operation cannot complete. Dropping the token
/// does not cancel the accepted operation.
#[derive(Clone)]
#[allow(clippy::struct_field_names)]
pub struct Client {
    // NOTE: We use different channels for publishes vs. control packets to allow for
    // prioritization of operations by the receiver.
    /// Channel that transmits outgoing PUBLISH requests at QoS 0
    pub_qos0_tx: tokio::sync::mpsc::Sender<PublishRequestQoS0<Bytes>>,
    /// Channel that transmits outgoing PUBLISH requests as QoS 1 or 2
    pub_qos12_tx: tokio::sync::mpsc::Sender<PublishRequestQoS1QoS2<Bytes>>,
    /// Channel that transmits outgoing SUBSCRIBE/UNSUBSCRIBE requests
    sub_tx: tokio::sync::mpsc::Sender<SubscriptionRequest<Bytes>>,
}

impl Client {
    /// Sends a PUBLISH packet to the server at QoS 0.
    ///
    /// On success, the operation has been submitted to the client and a completion token is
    /// returned. Awaiting the token reports when the session releases the PUBLISH for
    /// transmission. QoS 0 has no server acknowledgement or MQTT reason code to validate.
    pub async fn publish_qos0(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        retain: bool,
        properties: PublishProperties,
    ) -> Result<PublishQoS0CompletionToken, DetachedError> {
        let (notifier, token) = completion_pair();
        self.pub_qos0_tx
            .send(PublishRequestQoS0(
                notifier,
                topic_name.into_inner().into(),
                payload,
                retain,
                properties.into(),
            ))
            .await
            .map_err(|_| DetachedError {})?;
        Ok(PublishQoS0CompletionToken(token))
    }

    /// Sends a PUBLISH packet to the server at QoS 1.
    ///
    /// On success, the operation has been submitted to the client and a completion token is
    /// returned. Awaiting the token returns the server's [`crate::packet::PubAck`]. Use
    /// [`crate::packet::PubAck::as_result`] to check its MQTT reason code.
    pub async fn publish_qos1(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        retain: bool,
        properties: PublishProperties,
    ) -> Result<PublishQoS1CompletionToken, DetachedError> {
        let (notifier, token) = completion_pair();
        self.pub_qos12_tx
            .send(PublishRequestQoS1QoS2::PublishQoS1(
                notifier,
                topic_name.into_inner().into(),
                payload,
                retain,
                properties.into(),
            ))
            .await
            .map_err(|_| DetachedError {})?;
        Ok(PublishQoS1CompletionToken(token))
    }

    /// Reserves the API for sending a PUBLISH packet at QoS 2.
    ///
    /// QoS 2 publishing is not yet implemented. Use [`Self::publish_qos0`] or
    /// [`Self::publish_qos1`] in applications.
    ///
    /// # Panics
    ///
    /// The future returned by this method always panics without submitting a PUBLISH.
    pub async fn publish_qos2(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        retain: bool,
        properties: PublishProperties,
    ) -> Result<PublishQoS2CompletionToken, DetachedError> {
        // let (notifier, token) = completion_pair();
        // self.pub_qos12_tx
        //     .send(PublishRequestQoS1QoS2::PublishQoS2(
        //         notifier,
        //         topic_name.into_inner().into(),
        //         payload,
        //         retain,
        //         properties.into(),
        //     ))
        //     .await
        //     .map_err(|_| DetachedError {})?;
        // Ok(PublishQoS2CompletionToken(token))
        unimplemented!()
    }

    /// Send a SUBSCRIBE packet to the server.
    ///
    /// On success, the operation has been submitted to the client and a completion token is
    /// returned. Awaiting the token returns the server's [`crate::packet::SubAck`]. Use
    /// [`crate::packet::SubAck::as_result`] to check its MQTT reason code. This API submits one
    /// topic filter per operation.
    ///
    /// # Panics
    ///
    /// The future returned by this method panics without submitting a SUBSCRIBE if `max_qos` is
    /// [`QoS::ExactlyOnce`], because QoS 2 receiving is not yet supported.
    pub async fn subscribe(
        &self,
        topic_filter: TopicFilter,
        max_qos: QoS,
        no_local: bool,
        retain_options: RetainOptions,
        properties: SubscribeProperties,
    ) -> Result<SubscribeCompletionToken, DetachedError> {
        if max_qos == QoS::ExactlyOnce {
            unimplemented!("QoS 2 subscriptions are not yet supported");
        }

        let (notifier, token) = completion_pair();

        let options = mqtt_proto::SubscribeOptions {
            maximum_qos: max_qos.into(),
            other_properties: mqtt_proto::SubscribeOptionsOtherProperties {
                no_local,
                retain_as_published: retain_options.retain_as_published,
                retain_handling: retain_options.retain_handling,
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
            .map_err(|_| DetachedError {})?;
        Ok(SubscribeCompletionToken(token))
    }

    /// Send an UNSUBSCRIBE packet to the server.
    ///
    /// On success, the operation has been submitted to the client and a completion token is
    /// returned. Awaiting the token returns the server's [`crate::packet::UnsubAck`]. Use
    /// [`crate::packet::UnsubAck::as_result`] to check its MQTT reason code.
    pub async fn unsubscribe(
        &self,
        topic_filter: TopicFilter,
        properties: UnsubscribeProperties,
    ) -> Result<UnsubscribeCompletionToken, DetachedError> {
        let (notifier, token) = completion_pair();
        self.sub_tx
            .send(SubscriptionRequest::Unsubscribe(
                notifier,
                topic_filter.into_inner().into(),
                properties.into(),
            ))
            .await
            .map_err(|_| DetachedError {})?;
        Ok(UnsubscribeCompletionToken(token))
    }
}

/// Receives incoming Application Messages as `Publish`es.
pub struct Receiver {
    /// Channel for receiving incoming PUBLISH packets
    rx: tokio::sync::mpsc::UnboundedReceiver<IncomingPublishAndToken<Bytes>>,
}

impl Receiver {
    /// Receives an incoming [`Publish`] and its [`ManualAcknowledgement`] control.
    ///
    /// Ignoring the acknowledgement drops its control value. For QoS 1, this attempts to accept
    /// the publish with default PUBACK properties. Bind it instead to control acknowledgement
    /// timing, properties, or outcome.
    ///
    /// Returning `None` indicates that the corresponding [`ConnectHandle`],
    /// [`EnhancedAuthHandle`], or [`Connection`] has been dropped and no more messages will be
    /// received.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ms_mqtt_client::client::Receiver;
    ///
    /// async fn receive(mut receiver: Receiver) {
    ///     while let Some((publish, _)) = receiver.recv().await {
    ///         println!("{}", String::from_utf8_lossy(&publish.payload));
    ///     }
    /// }
    /// ```
    ///
    /// To control acknowledgements explicitly, match on [`ManualAcknowledgement`]. QoS 0 requires
    /// no acknowledgement, while QoS 1 provides a token for accepting or rejecting the publish.
    /// Receiving at QoS 2 is not yet supported.
    ///
    /// ```no_run
    /// use std::error::Error;
    ///
    /// use ms_mqtt_client::client::{ManualAcknowledgement, Receiver};
    /// use ms_mqtt_client::packet::PubAckProperties;
    ///
    /// async fn receive(mut receiver: Receiver) -> Result<(), Box<dyn Error>> {
    ///     while let Some((publish, manual_ack)) = receiver.recv().await {
    ///         println!("{}", String::from_utf8_lossy(&publish.payload));
    ///
    ///         match manual_ack {
    ///             ManualAcknowledgement::QoS0 => {}
    ///             ManualAcknowledgement::QoS1(token) => {
    ///                 let completion = token.accept(PubAckProperties::default()).await?;
    ///                 completion.await?; // Observe release for transmission.
    ///             }
    ///             ManualAcknowledgement::QoS2(_) => {
    ///                 // Receiving at QoS 2 is not yet supported.
    ///             }
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    #[doc(alias = "receive")]
    pub async fn recv(&mut self) -> Option<(Publish, ManualAcknowledgement)> {
        self.rx.recv().await.map(Into::into)
    }
}

/// Handle providing MQTT CONNECT functionality.
pub struct ConnectHandle {
    session: Session<BytesMut>,
    reader_pool: BytesPool,
    writer_pool: BytesPool,
    cfg_client_id: Option<String>,
}

impl ConnectHandle {
    /// Connect to an MQTT server using standard authentication.
    ///
    /// Returns a [`ConnectResult`] indicating the status of the connection attempt,
    /// and any further handles needed to operate the connection or re-attempt.
    ///
    /// # Arguments
    /// - `connection_transport`: Configuration for the transport to use for the connection.
    /// - `clean_start`: Whether to request a new MQTT session from the server
    /// - `keep_alive`: Keep-alive configuration for the connection.
    /// - `will`: Optional Last Will and Testament to be sent on unexpected disconnect.
    /// - `username`: Optional username for authentication.
    /// - `password`: Optional password for authentication.
    /// - `properties`: Properties to include in the CONNECT packet.
    /// - `response_timeout`: Optional timeout for the MQTT CONNECT operation.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ms_mqtt_client::client::{
    ///     ClientOptions, ConnectResult, KeepAliveConfig, new_client,
    /// };
    /// use ms_mqtt_client::packet::ConnectProperties;
    /// use ms_mqtt_client::transport::{
    ///     ConnectionTransportConfig, ConnectionTransportType,
    /// };
    ///
    /// # async fn run() {
    /// let (client, connect_handle, receiver) = new_client(ClientOptions::default());
    ///
    /// let result = connect_handle.connect(
    ///     ConnectionTransportConfig {
    ///         transport_type: ConnectionTransportType::Tcp {
    ///             hostname: "localhost".into(),
    ///             port: 1883,
    ///         },
    ///         timeout: None,
    ///         proxy: None,
    ///         tcp_nodelay: false,
    ///     },
    ///     true,
    ///     KeepAliveConfig::Infinite,
    ///     None,
    ///     None,
    ///     None,
    ///     ConnectProperties::default(),
    ///     None,
    /// ).await;
    ///
    /// match result {
    ///     ConnectResult::Success(connection, connack, disconnect_handle) => {
    ///         let (connect_handle, event) = connection.run_until_disconnect().await;
    ///     }
    ///     ConnectResult::Failure(connect_handle, error) => {
    ///         eprintln!("connection failed: {error}");
    ///     }
    /// }
    /// # }
    /// ```
    #[doc(alias = "broker")]
    #[allow(clippy::too_many_arguments)] // Reducing the number of arguments creates semantic confusion
    pub async fn connect(
        mut self,
        connection_transport: ConnectionTransportConfig,
        clean_start: bool,
        keep_alive: KeepAliveConfig,
        will: Option<Will>,
        username: Option<String>,
        password: Option<Bytes>,
        properties: ConnectProperties,
        response_timeout: Option<Duration>,
    ) -> ConnectResult {
        let (mut reader, mut writer) = match self.transport_connect(connection_transport).await {
            Ok(streams) => streams,
            Err(err) => {
                return ConnectResult::Failure(self, err.into());
            }
        };

        if let Err(err) = self
            .mqtt_connect(
                &mut writer,
                clean_start,
                keep_alive.into(),
                will,
                username,
                password,
                properties,
                None,
            )
            .await
        {
            return ConnectResult::Failure(self, err);
        }

        let connack = match maybe_timeout(response_timeout, mqtt_receive(&mut reader)).await {
            Ok(Ok(Packet::ConnAck(connack))) => {
                if !connack.is_success() {
                    return ConnectResult::Failure(self, ConnectError::Rejected(connack.into()));
                }
                connack
            }
            Ok(Ok(_)) => {
                return ConnectResult::Failure(
                    self,
                    ConnectError::Protocol(ProtocolErrorRepr::UnexpectedPacket.into()),
                );
            }
            Ok(Err(err)) => return ConnectResult::Failure(self, err.into()),
            Err(_) => return ConnectResult::Failure(self, ConnectError::ResponseTimeout),
        };

        self.session
            .incoming_connack(connack.clone(), keep_alive.into());

        let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel();
        self.session.ch.disconnect_rx = Some(disconnect_rx);
        let cfg_pingresp_timeout = match keep_alive {
            KeepAliveConfig::Duration {
                ping_after,
                response_timeout,
            } => Some(response_timeout),
            KeepAliveConfig::Infinite => None,
        };
        ConnectResult::Success(
            Connection {
                session: self.session,
                reader_pool: self.reader_pool,
                writer_pool: self.writer_pool,
                reader,
                writer,
                cfg_client_id: self.cfg_client_id,
                cfg_pingresp_timeout,
            },
            connack.into(),
            DisconnectHandle(disconnect_tx),
        )
    }

    /// Connect to an MQTT server using enhanced authentication.
    ///
    /// Returns a [`ConnectEnhancedAuthResult`] indicating the status of the connection attempt,
    /// and any further handles needed to operate the connection, continue the authentication
    /// process, or re-attempt.
    ///
    /// # Arguments
    /// - `connection_transport`: Configuration for the transport to use for the connection.
    /// - `clean_start`: Whether to request a new MQTT session from the server
    /// - `keep_alive`: Keep-alive configuration for the connection.
    /// - `will`: Optional Last Will and Testament to be sent on unexpected disconnect.
    /// - `username`: Optional username for authentication.
    /// - `password`: Optional password for authentication.
    /// - `properties`: Properties to include in the CONNECT packet.
    /// - `authentication_info`: Initial authentication information for enhanced authentication.
    /// - `response_timeout`: Optional timeout for the MQTT CONNECT operation.
    ///
    /// # Example
    ///
    /// The authentication method determines how the application generates initial data and
    /// responds to each server challenge.
    ///
    /// ```no_run
    /// use bytes::Bytes;
    /// use ms_mqtt_client::client::{
    ///     ClientOptions, ConnectEnhancedAuthResult, KeepAliveConfig, new_client,
    /// };
    /// use ms_mqtt_client::packet::{
    ///     Auth, AuthProperties, AuthenticationInfo, ConnectProperties,
    /// };
    /// use ms_mqtt_client::transport::{
    ///     ConnectionTransportConfig, ConnectionTransportType,
    /// };
    ///
    /// fn respond_to_challenge(challenge: &Auth) -> Option<Bytes> {
    ///     // Generate data according to the negotiated authentication method.
    ///     todo!()
    /// }
    ///
    /// # async fn run() {
    /// let (client, connect_handle, receiver) = new_client(ClientOptions::default());
    /// let authentication = AuthenticationInfo {
    ///     method: "example-method".into(),
    ///     data: None,
    /// };
    ///
    /// let mut result = connect_handle.connect_enhanced_auth(
    ///     ConnectionTransportConfig {
    ///         transport_type: ConnectionTransportType::Tcp {
    ///             hostname: "localhost".into(),
    ///             port: 1883,
    ///         },
    ///         timeout: None,
    ///         proxy: None,
    ///         tcp_nodelay: false,
    ///     },
    ///     true,
    ///     KeepAliveConfig::Infinite,
    ///     None,
    ///     None,
    ///     None,
    ///     ConnectProperties::default(),
    ///     authentication,
    ///     None,
    /// ).await;
    ///
    /// let (connection, connack, disconnect_handle, reauth_handle) = loop {
    ///     match result {
    ///         ConnectEnhancedAuthResult::Continue(challenge, handle) => {
    ///             result = handle.continue_auth(
    ///                 respond_to_challenge(&challenge),
    ///                 AuthProperties::default(),
    ///                 None,
    ///             ).await;
    ///         }
    ///         ConnectEnhancedAuthResult::Success(
    ///             connection,
    ///             connack,
    ///             disconnect_handle,
    ///             reauth_handle,
    ///         ) => break (connection, connack, disconnect_handle, reauth_handle),
    ///         ConnectEnhancedAuthResult::Failure(connect_handle, error) => {
    ///             eprintln!("connection failed: {error}");
    ///             return;
    ///         }
    ///     }
    /// };
    ///
    /// let (connect_handle, event) = connection.run_until_disconnect().await;
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)] // Reducing the number of arguments creates semantic confusion
    pub async fn connect_enhanced_auth(
        mut self,
        connection_transport: ConnectionTransportConfig,
        clean_start: bool,
        keep_alive: KeepAliveConfig,
        will: Option<Will>,
        username: Option<String>,
        password: Option<Bytes>,
        properties: ConnectProperties,
        authentication_info: AuthenticationInfo,
        response_timeout: Option<Duration>,
    ) -> ConnectEnhancedAuthResult {
        let auth_method = authentication_info.method.clone();
        let (mut reader, mut writer) = match self.transport_connect(connection_transport).await {
            Ok(streams) => streams,
            Err(err) => return ConnectEnhancedAuthResult::Failure(self, err.into()),
        };
        if let Err(err) = self
            .mqtt_connect(
                &mut writer,
                clean_start,
                keep_alive.into(),
                will,
                username,
                password,
                properties,
                Some(authentication_info),
            )
            .await
        {
            return ConnectEnhancedAuthResult::Failure(self, err);
        }

        let packet = match maybe_timeout(response_timeout, mqtt_receive(&mut reader)).await {
            Ok(Ok(packet)) => packet,
            Ok(Err(err)) => return ConnectEnhancedAuthResult::Failure(self, err.into()),
            Err(_) => {
                return ConnectEnhancedAuthResult::Failure(self, ConnectError::ResponseTimeout);
            }
        };

        match packet {
            Packet::ConnAck(connack) => {
                self.session
                    .incoming_connack(connack.clone(), keep_alive.into());
                if connack.is_success() {
                    let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel();
                    let auth_tx = self.session.ch.auth_tx.clone();
                    self.session.ch.disconnect_rx = Some(disconnect_rx);
                    let cfg_pingresp_timeout = match keep_alive {
                        KeepAliveConfig::Duration {
                            ping_after,
                            response_timeout,
                        } => Some(response_timeout),
                        KeepAliveConfig::Infinite => None,
                    };
                    ConnectEnhancedAuthResult::Success(
                        Connection {
                            session: self.session,
                            reader_pool: self.reader_pool,
                            writer_pool: self.writer_pool,
                            reader,
                            writer,
                            cfg_client_id: self.cfg_client_id,
                            cfg_pingresp_timeout,
                        },
                        connack.into(),
                        DisconnectHandle(disconnect_tx),
                        ReauthHandle {
                            method: auth_method,
                            tx: auth_tx,
                        },
                    )
                } else {
                    ConnectEnhancedAuthResult::Failure(self, ConnectError::Rejected(connack.into()))
                }
            }

            Packet::Auth(auth) => {
                let auth_handle = EnhancedAuthHandle {
                    session: self.session,
                    reader_pool: self.reader_pool,
                    writer_pool: self.writer_pool,
                    reader,
                    writer,
                    auth_method,
                    cfg_client_id: self.cfg_client_id,
                    cfg_keep_alive: keep_alive,
                };
                ConnectEnhancedAuthResult::Continue(auth.into(), auth_handle)
            }

            _ => ConnectEnhancedAuthResult::Failure(
                self,
                ConnectError::Protocol(ProtocolErrorRepr::UnexpectedPacket.into()),
            ),
        }
    }

    async fn transport_connect(
        &self,
        transport_config: ConnectionTransportConfig,
    ) -> io::Result<(Reader<BytesPool>, Writer<BytesPool>)> {
        let ConnectionTransportConfig {
            transport_type,
            timeout,
            proxy,
            tcp_nodelay,
        } = transport_config;
        Ok(match transport_type {
            ConnectionTransportType::Tcp { hostname, port } => {
                maybe_timeout(
                    timeout,
                    crate::io::tokio_tcp::connect(
                        &hostname,
                        port,
                        proxy,
                        tcp_nodelay,
                        &self.reader_pool,
                        &self.writer_pool,
                    ),
                )
                .await??
            }

            ConnectionTransportType::Tls {
                hostname,
                port,
                tls_config,
            } => {
                maybe_timeout(
                    timeout,
                    crate::io::tokio_tls::connect(
                        &hostname,
                        port,
                        tls_config,
                        proxy,
                        tcp_nodelay,
                        &self.reader_pool,
                        &self.writer_pool,
                    ),
                )
                .await??
            }

            #[cfg(feature = "websockets")]
            ConnectionTransportType::Ws {
                request,
                tls_config,
            } => {
                maybe_timeout(
                    timeout,
                    crate::io::tokio_ws::connect(
                        request,
                        tls_config,
                        proxy,
                        tcp_nodelay,
                        &self.reader_pool,
                    ),
                )
                .await??
            }

            #[cfg(feature = "__integration")]
            ConnectionTransportType::Test {
                incoming_packets,
                outgoing_packets,
            } => crate::io::test::connect(
                incoming_packets,
                outgoing_packets,
                &self.reader_pool,
                &self.writer_pool,
            ),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn mqtt_connect(
        &self,
        writer: &mut Writer<BytesPool>,
        clean_start: bool,
        keep_alive: KeepAlive,
        will: Option<Will>,
        username: Option<String>,
        password: Option<Bytes>,
        properties: ConnectProperties,
        authentication_info: Option<AuthenticationInfo>,
    ) -> Result<(), ConnectError> {
        // Transport has been established. Send CONNECT and wait for CONNACK.

        let mut properties: mqtt_proto::ConnectOtherProperties<Bytes> = properties.into();
        properties.authentication = authentication_info.map(Into::into);

        let connect = Packet::Connect(Connect {
            username: username.as_deref().map(Into::into),
            password: password.as_deref().map(Into::into),
            will: will.map(Into::into),
            client_id: self.cfg_client_id.as_deref().map(Into::into),
            clean_start,
            keep_alive,
            other_properties: properties,
        });
        writer.write(&connect, ProtocolVersion::V5).await?;
        writer.flush().await?;
        Ok(())
    }
}

impl std::fmt::Debug for ConnectHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectHandle").finish_non_exhaustive()
    }
}

/// Handle for the intermediate step of an MQTT CONNECT with enhanced authentication.
pub struct EnhancedAuthHandle {
    session: Session<BytesMut>,
    reader_pool: BytesPool,
    writer_pool: BytesPool,
    reader: Reader<BytesPool>,
    writer: Writer<BytesPool>,
    auth_method: String,
    cfg_client_id: Option<String>,
    cfg_keep_alive: KeepAliveConfig,
}

impl EnhancedAuthHandle {
    /// Responds to an MQTT 5 enhanced-authentication challenge.
    ///
    /// Consumes this handle and returns the next state of the connection attempt.
    pub async fn continue_auth(
        mut self,
        authentication_data: Option<Bytes>,
        properties: AuthProperties,
        response_timeout: Option<Duration>,
    ) -> ConnectEnhancedAuthResult {
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
        if let Err(err) = self.writer.write(&auth, ProtocolVersion::V5).await {
            let connect_handle = ConnectHandle {
                session: self.session,
                reader_pool: self.reader_pool,
                writer_pool: self.writer_pool,
                cfg_client_id: self.cfg_client_id,
            };
            return ConnectEnhancedAuthResult::Failure(connect_handle, err.into());
        }
        if let Err(err) = self.writer.flush().await {
            let connect_handle = ConnectHandle {
                session: self.session,
                reader_pool: self.reader_pool,
                writer_pool: self.writer_pool,
                cfg_client_id: self.cfg_client_id,
            };
            return ConnectEnhancedAuthResult::Failure(connect_handle, err.into());
        }

        // Wait for next response
        let packet = match maybe_timeout(response_timeout, mqtt_receive(&mut self.reader)).await {
            Ok(Ok(packet)) => packet,
            Ok(Err(err)) => {
                let connect_handle = ConnectHandle {
                    session: self.session,
                    reader_pool: self.reader_pool,
                    writer_pool: self.writer_pool,
                    cfg_client_id: self.cfg_client_id,
                };
                return ConnectEnhancedAuthResult::Failure(connect_handle, err.into());
            }
            Err(_) => {
                let connect_handle = ConnectHandle {
                    session: self.session,
                    reader_pool: self.reader_pool,
                    writer_pool: self.writer_pool,
                    cfg_client_id: self.cfg_client_id,
                };
                return ConnectEnhancedAuthResult::Failure(
                    connect_handle,
                    ConnectError::ResponseTimeout,
                );
            }
        };

        match packet {
            Packet::ConnAck(connack) => {
                self.session
                    .incoming_connack(connack.clone(), self.cfg_keep_alive.into());

                if connack.is_success() {
                    let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel();
                    let auth_tx = self.session.ch.auth_tx.clone();
                    self.session.ch.disconnect_rx = Some(disconnect_rx);
                    let cfg_pingresp_timeout = match self.cfg_keep_alive {
                        KeepAliveConfig::Duration {
                            ping_after,
                            response_timeout,
                        } => Some(response_timeout),
                        KeepAliveConfig::Infinite => None,
                    };
                    let connection = Connection {
                        session: self.session,
                        reader_pool: self.reader_pool,
                        writer_pool: self.writer_pool,
                        reader: self.reader,
                        writer: self.writer,
                        cfg_client_id: self.cfg_client_id,
                        cfg_pingresp_timeout,
                    };
                    ConnectEnhancedAuthResult::Success(
                        connection,
                        connack.into(),
                        DisconnectHandle(disconnect_tx),
                        ReauthHandle {
                            method: self.auth_method.clone(),
                            tx: auth_tx,
                        },
                    )
                } else {
                    let connect_handle = ConnectHandle {
                        session: self.session,
                        reader_pool: self.reader_pool,
                        writer_pool: self.writer_pool,
                        cfg_client_id: self.cfg_client_id,
                    };
                    ConnectEnhancedAuthResult::Failure(
                        connect_handle,
                        ConnectError::Rejected(connack.into()),
                    )
                }
            }

            Packet::Auth(auth) => ConnectEnhancedAuthResult::Continue(auth.into(), self),

            _ => {
                let connect_handle = ConnectHandle {
                    session: self.session,
                    reader_pool: self.reader_pool,
                    writer_pool: self.writer_pool,
                    cfg_client_id: self.cfg_client_id,
                };
                ConnectEnhancedAuthResult::Failure(
                    connect_handle,
                    ConnectError::Protocol(ProtocolErrorRepr::UnexpectedPacket.into()),
                )
            }
        }
    }
}

/// Runs the MQTT client event loop, keeping the client operational.
pub struct Connection {
    session: Session<BytesMut>,
    reader_pool: BytesPool,
    writer_pool: BytesPool,
    reader: Reader<BytesPool>,
    writer: Writer<BytesPool>,
    cfg_client_id: Option<String>,
    cfg_pingresp_timeout: Option<Duration>,
}

impl Connection {
    /// Drives this connection until it is disconnected.
    ///
    /// Packets will only be sent and received while this future is running.
    /// The returned [`ConnectHandle`] can establish a later connection; reconnection is not
    /// automatic. Subscriptions generally need to be restored when the server does not resume the
    /// previous MQTT session, as indicated by [`ConnAck::session_present`].
    #[doc(alias = "reconnect")]
    #[doc(alias = "event_loop")]
    #[doc(alias = "connection_driver")]
    pub async fn run_until_disconnect(mut self) -> (ConnectHandle, DisconnectedEvent) {
        let event = match self.run_until_disconnect_inner().await {
            Ok(InnerDisconnect::Application) => DisconnectedEvent::ApplicationDisconnect,
            Ok(InnerDisconnect::Server(disconnect)) => {
                DisconnectedEvent::ServerDisconnect(disconnect)
            }
            Ok(InnerDisconnect::PingTimeout) => DisconnectedEvent::PingTimeout,
            Err(InnerConnectionError::Io(e)) => DisconnectedEvent::IoError(e),
            Err(InnerConnectionError::Protocol(e)) => DisconnectedEvent::ProtocolError(e),
        };
        let connect_handle = ConnectHandle {
            session: self.session,
            reader_pool: self.reader_pool,
            writer_pool: self.writer_pool,
            cfg_client_id: self.cfg_client_id,
        };
        // NOTE: By returning here, we implicitly drop the `reader` and `writer` stored on the
        // `Connection`, implicitly closing the underlying transport.
        (connect_handle, event)
    }

    async fn run_until_disconnect_inner(
        &mut self,
    ) -> Result<InnerDisconnect, InnerConnectionError> {
        let (reader, writer) = (&mut self.reader, &mut self.writer);
        let mut pingresp_timer: Option<Timer> = None;

        loop {
            // Check for outgoing packets from the session or incoming packets from the reader.
            let next = {
                let next_outgoing_packet_f = pin!(self.session.next_outgoing_packet());
                let read_f = pin!(mqtt_receive(reader));
                let io_f = future::select(next_outgoing_packet_f, read_f);

                // If there is a ping timer, use its remaining duration as a timeout for the I/O future.
                let timeout = pingresp_timer.as_ref().map(Timer::remaining_duration);
                match maybe_timeout(timeout, io_f).await {
                    Ok(future::Either::Left((packet, _))) => {
                        log::trace!("OUTGOING: {packet:?}");
                        future::Either::Left(packet)
                    }
                    Ok(future::Either::Right((Ok(raw_packet), _))) => {
                        log::trace!("INCOMING: {raw_packet:?}");
                        future::Either::Right(Ok(raw_packet))
                    }
                    Ok(future::Either::Right((Err(err), _))) => future::Either::Right(Err(err)),
                    Err(_) => return Ok(InnerDisconnect::PingTimeout),
                }
            };
            match next {
                // Outgoing packet from session
                future::Either::Left(packet) => {
                    let mut disconnect = false;
                    let mut op_packet = Some(packet);
                    while let Some(packet_) = op_packet {
                        if let Packet::Disconnect(disconnect_) = &packet_ {
                            disconnect = true;
                            self.session.client_disconnect(disconnect_);
                        }
                        if let Packet::PingReq(_) = &packet_
                            && let Some(timeout) = self.cfg_pingresp_timeout
                        {
                            pingresp_timer = Some(Timer::new(timeout));
                        }
                        writer.write(&packet_, ProtocolVersion::V5).await?;
                        if disconnect {
                            break;
                        }
                        op_packet = self.session.next_outgoing_packet().now_or_never();
                    }
                    writer.flush().await?;
                    // If we wrote a DISCONNECT packet, also close the connection.
                    if disconnect {
                        return Ok(InnerDisconnect::Application);
                    }
                }

                // Incoming packet from reader
                future::Either::Right(Ok(packet)) => match packet {
                    Packet::Auth(auth) => self.session.incoming_auth(auth)?,

                    Packet::SubAck(suback) => self
                        .session
                        .complete_inflight(CompletedOperation::Subscribe(suback))?,

                    Packet::UnsubAck(unsuback) => self
                        .session
                        .complete_inflight(CompletedOperation::Unsubscribe(unsuback))?,

                    Packet::PubAck(puback) => self
                        .session
                        .complete_inflight(CompletedOperation::PublishQoS1(puback))?,

                    Packet::PubRec(pubrec) => self
                        .session
                        .complete_inflight(CompletedOperation::PublishQoS2(pubrec))?,

                    Packet::Disconnect(disconnect) => {
                        self.session.server_disconnect(&disconnect);
                        return Ok(InnerDisconnect::Server(disconnect.into()));
                    }

                    Packet::Publish(publish) => self.session.incoming_publish(publish),

                    Packet::PingResp(_) => {
                        // Remove ping response timer as we have successfully received a PINGRESP.
                        pingresp_timer = None;
                    }

                    packet => {
                        let err = ProtocolError::from(ProtocolErrorRepr::UnexpectedPacket).into();
                        self.session.transport_disconnect(&err);
                        return Err(err);
                    }
                },

                future::Either::Right(Err(err)) => {
                    self.session.transport_disconnect(&err);
                    return Err(err);
                }
            }
        }
    }
}

/// One-shot handle for requesting an orderly application disconnect.
///
/// Dropping this handle does not request a disconnect.
///
/// After requesting disconnect, continue driving the associated [`Connection`] until
/// [`Connection::run_until_disconnect`] returns.
pub struct DisconnectHandle(tokio::sync::oneshot::Sender<DisconnectRequest<Bytes>>);

impl DisconnectHandle {
    /// Requests an orderly MQTT disconnect with the supplied properties.
    ///
    /// This submits the request synchronously; continue driving the associated [`Connection`]
    /// until [`Connection::run_until_disconnect`] returns
    /// [`DisconnectedEvent::ApplicationDisconnect`].
    #[doc(alias = "shutdown")]
    pub fn disconnect(self, properties: &DisconnectProperties) -> Result<(), DetachedError> {
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

        self.0.send(req).map_err(|_| DetachedError {})
    }
}

// TODO: Determine where some of these auth structures should live, and what a token vs. handle is semantically.

/// Handle for initiating MQTT 5 re-authentication on an established connection.
///
/// This handle is returned only for connections established with
/// [`ConnectHandle::connect_enhanced_auth`].
pub struct ReauthHandle {
    method: String,
    tx: tokio::sync::mpsc::Sender<ReauthRequest<Bytes>>,
}

impl ReauthHandle {
    /// Initiates MQTT 5 re-authentication on the established connection.
    ///
    /// On success, the operation has been submitted to the client and a completion token is
    /// returned. Awaiting the token returns a [`ReauthResult`].
    pub async fn reauth(
        &self,
        authentication_data: Option<Bytes>,
        properties: AuthProperties,
    ) -> Result<ReauthCompletionToken, DetachedError> {
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
            .map_err(|_| DetachedError {})?;
        Ok(ReauthCompletionToken(token))
    }
}

/// Indicates the result of an MQTT CONNECT.
pub enum ConnectResult {
    /// The connection succeeded, yielding the connection driver, server response, and orderly
    /// disconnect handle.
    Success(Connection, ConnAck, DisconnectHandle),
    /// The connection failed, yielding a reusable connection handle and the error.
    Failure(ConnectHandle, ConnectError),
}

/// Indicates the result of an MQTT CONNECT with enhanced authentication.
pub enum ConnectEnhancedAuthResult {
    /// The server sent another authentication challenge and a handle for responding to it.
    Continue(Auth, EnhancedAuthHandle),
    /// The connection succeeded, yielding the connection driver, server response, orderly
    /// disconnect handle, and reauthentication handle.
    Success(Connection, ConnAck, DisconnectHandle, ReauthHandle),
    /// The connection failed, yielding a reusable connection handle and the error.
    Failure(ConnectHandle, ConnectError),
}

/// Indicates the result of an MQTT AUTH operation on an existing connection.
#[derive(Debug)]
pub enum ReauthResult {
    /// The server sent another authentication challenge and a token for responding to it.
    Continue(Auth, ReauthToken),
    /// Reauthentication succeeded with the server's final AUTH packet.
    Success(Auth),
    /// Reauthentication failed without a server DISCONNECT packet.
    Failure, // Cannot provide Disconnect packet here because it is not guaranteed to be sent by server
}

impl From<buffered::ReauthResult<Bytes>> for ReauthResult {
    fn from(value: buffered::ReauthResult<Bytes>) -> Self {
        match value {
            buffered::ReauthResult::Continue(auth, token) => {
                Self::Continue(auth.into(), ReauthToken(token))
            }
            buffered::ReauthResult::Success(auth) => Self::Success(auth.into()),
            buffered::ReauthResult::Failure => Self::Failure,
        }
    }
}

/// Details about a client disconnect
#[derive(Debug)]
pub enum DisconnectedEvent {
    /// The application requested an orderly disconnect through [`DisconnectHandle`].
    ApplicationDisconnect,
    /// The server sent an MQTT DISCONNECT packet.
    ServerDisconnect(Disconnect),
    /// Transport I/O failed.
    IoError(io::Error),
    /// The server violated the MQTT protocol.
    ProtocolError(ProtocolError),
    /// The server did not respond to PINGREQ before the configured response timeout.
    PingTimeout,
}

/// Internal error type for propagating connection errors
#[derive(Error, Debug)]
#[error(transparent)]
pub(crate) enum InnerConnectionError {
    Io(#[from] io::Error),
    Protocol(#[from] ProtocolError),
}

impl From<InnerConnectionError> for ConnectError {
    fn from(err: InnerConnectionError) -> Self {
        match err {
            InnerConnectionError::Io(err) => Self::Io(err),
            InnerConnectionError::Protocol(err) => Self::Protocol(err),
        }
    }
}

/// Internal enum for distinguishing disconnect types
enum InnerDisconnect {
    Application,
    Server(Disconnect),
    PingTimeout,
}

/// Acknowledgement control associated with an incoming [`Publish`].
///
/// Dropping this value also drops any contained token. For QoS 1, that attempts to accept the
/// publish with default PUBACK properties.
// TODO: Rename to `ManualAcknowledgment` with the next set of breaking changes.
#[doc(alias = "ManualAcknowledgment")]
pub enum ManualAcknowledgement {
    /// The PUBLISH was delivered at QoS 0 and requires no acknowledgement.
    QoS0,
    /// Controls accepting or rejecting an incoming QoS 1 PUBLISH.
    QoS1(PubAckToken),
    /// Reserved acknowledgement control for QoS 2, which is not yet supported.
    QoS2(PubRecToken),
}

impl From<channel_data::IncomingPublishAndToken<Bytes>> for (Publish, ManualAcknowledgement) {
    fn from(inner: channel_data::IncomingPublishAndToken<Bytes>) -> Self {
        match inner {
            channel_data::IncomingPublishAndToken::QoS0(publish) => {
                (publish.into(), ManualAcknowledgement::QoS0)
            }
            channel_data::IncomingPublishAndToken::QoS1(publish, token) => (
                publish.into(),
                ManualAcknowledgement::QoS1(PubAckToken(token)),
            ),
            channel_data::IncomingPublishAndToken::QoS2(publish, token) => (
                publish.into(),
                ManualAcknowledgement::QoS2(PubRecToken(token)),
            ),
        }
    }
}

async fn maybe_timeout<F>(
    timeout: Option<Duration>,
    f: F,
) -> Result<F::Output, tokio::time::error::Elapsed>
where
    F: Future,
{
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, f).await,
        None => Ok(f.await),
    }
}

async fn mqtt_receive(
    reader: &mut Reader<BytesPool>,
) -> Result<Packet<Bytes>, InnerConnectionError> {
    let mut raw_packet = reader.read().await?;
    let packet = Packet::decode(
        raw_packet.first_byte,
        &mut raw_packet.rest,
        ProtocolVersion::V5,
    )
    .map_err(|e| ProtocolError::from(ProtocolErrorRepr::from(e)))?;
    Ok(packet)
}

mod buffered {
    use crate::buffer_pool::Shared;
    use crate::client::token::reauth::buffered::ReauthToken;
    use crate::mqtt_proto::Auth;

    /// Indicates the result of an MQTT AUTH operation on an existing connection.
    #[derive(Debug)]
    pub enum ReauthResult<S>
    where
        S: Shared,
    {
        Continue(Auth<S>, ReauthToken<S>),
        Success(Auth<S>),
        Failure, // Cannot provide Disconnect packet here because it is not guaranteed to be sent by server
    }
}
