// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::task::Poll;

use derive_where::derive_where;
use futures_util::future::FutureExt as _;
use futures_util::stream::{Peekable, Stream, StreamExt as _};
use indexmap::IndexMap;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::time::Duration;

use crate::buffer_pool::{Owned, Shared};
use crate::client::{
    InnerConnectionError,
    buffered::ReauthResult,
    channel_data::{
        AcknowledgementRequest, DisconnectRequest, IncomingPublishAndToken, PublishRequestQoS0,
        PublishRequestQoS1QoS2, ReauthRequest, SubscriptionRequest,
    },
    session::pkid::PkidPool,
    timer::Timer,
    token::acknowledgement::buffered::{PubAckToken, PubCompToken, PubRecToken, PubRelToken},
    token::completion::buffered::{
        PubRecAcceptCompletionNotifier, PubRelCompletionNotifier, PublishQoS0CompletionNotifier,
        PublishQoS1CompletionNotifier, PublishQoS2CompletionNotifier, ReauthCompletionNotifier,
        SubscribeCompletionNotifier, UnsubscribeCompletionNotifier,
    },
    token::reauth::buffered::ReauthToken,
};
use crate::error::{ProtocolError, ProtocolErrorRepr};
use crate::mqtt_proto::{
    Auth, AuthenticateReasonCode, ByteStr, ConnAck, ConnectReasonCode, Disconnect, KeepAlive,
    Packet, PacketIdentifier, PacketIdentifierDupQoS, PingReq, PubAck, PubComp, PubCompReasonCode,
    PubRec, PubRel, PubRelReasonCode, Publish, PublishOtherProperties, SessionExpiryInterval,
    SubAck, Subscribe, SubscribeTo, Topic, UnsubAck, Unsubscribe,
};

mod pkid;

/// Tracks data related to the MQTT session state
pub(crate) struct Session<O>
where
    O: Owned,
{
    /// Struct containing channels in and out of the Session
    pub(crate) ch: Channels<O::Shared>,
    /// Pool of packet identifiers for outgoing packets that can be leased
    pkid_pool: PkidPool,
    /// Queue of outgoing operations that are not yet in-flight
    /// *Technically* not part of an MQTT Session, but it makes sense to keep it here.
    outgoing_queue: VecDeque<OutgoingOperation<O::Shared>>,
    /// Connection-scoped packets generated internally in response to incoming packets
    connection_packets: VecDeque<Packet<O::Shared>>,
    /// Tracker of all inflight MQTT packets awaiting a response
    inflight: InflightTracker<O::Shared>,
    /// Tracker of MQTT packets sent to the application, awaiting a response
    in_application: InApplicationTracker<O::Shared>,
    /// Whether the session is currently connected, and if so, the CONNACK
    connected: ConnectionState<O::Shared>,
    /// Identifier for the current connection epoch
    connection_epoch: u64,
    /// Identifier for the current MQTT session generation
    #[allow(clippy::struct_field_names)]
    session_generation: u64,
    /// Whether the session is transient (i.e. non-persistent, can expire)
    transient: bool,
    /// Timer for tracking when to send the next PINGREQ (based on keep-alive)
    pingreq_timer: Option<Timer>,
    pub(crate) owned: O, // NOTE: This really shouldn't be pub(crate)
}

impl<O> Session<O>
where
    O: Owned,
{
    #[allow(clippy::too_many_arguments)] // TODO: Honestly, probably should address this
    pub fn new(
        sub_rx: Receiver<SubscriptionRequest<O::Shared>>,
        o_pub_q0_rx: Receiver<PublishRequestQoS0<O::Shared>>,
        o_pub_q12_rx: Receiver<PublishRequestQoS1QoS2<O::Shared>>,
        ack_rx: UnboundedReceiver<AcknowledgementRequest<O::Shared>>,
        auth_rx: Receiver<ReauthRequest<O::Shared>>,
        i_pub_tx: UnboundedSender<IncomingPublishAndToken<O::Shared>>,
        ack_tx: UnboundedSender<AcknowledgementRequest<O::Shared>>,
        auth_tx: Sender<ReauthRequest<O::Shared>>,
        max_pkid: PacketIdentifier,
        owned: O,
    ) -> Self {
        let ch = Channels {
            disconnect_rx: None,
            o_pub_q0_rx,
            o_pub_q12_rx: ReceiverStream(o_pub_q12_rx).peekable(),
            sub_rx: ReceiverStream(sub_rx).peekable(),
            ack_rx,
            auth_rx,
            i_pub_tx,
            ack_tx,
            auth_tx,
        };
        Self {
            ch,
            pkid_pool: PkidPool::new(max_pkid),
            outgoing_queue: Default::default(),
            connection_packets: Default::default(),
            inflight: InflightTracker::default(),
            in_application: InApplicationTracker::default(),
            connected: ConnectionState::Disconnected,
            connection_epoch: 0, // move this to the connection state?
            session_generation: 0,
            transient: false, // move this to the connection state?
            pingreq_timer: None,
            owned,
        }
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.connected, ConnectionState::Connected { .. })
    }

    /// Returns the next outgoing MQTT packet to be sent over the network
    //pub async fn next_outgoing_packet(&mut self) -> Option<Packet<O::Shared>> {
    pub async fn next_outgoing_packet(&mut self) -> Packet<O::Shared> {
        // TODO: Now that sending CONNECT is handled outside of `Session::next_outgoing_packet`,
        // it will only ever be called after `incoming_connack(ConnAck)` has been called, right?
        assert!(self.is_connected());

        // Check for replayed packets first
        let packet = if let Some(packet) = self.inflight.packets_to_replay.pop_front() {
            // TODO: Replayed PUBLISHes should be subject to server's receive-maximum.

            match packet {
                Packet::Publish(mut publish) => {
                    // For PUBLISH, we need to update the DUP flag
                    match &mut publish.packet_identifier_dup_qos {
                        PacketIdentifierDupQoS::AtLeastOnce(_, dup)
                        | PacketIdentifierDupQoS::ExactlyOnce(_, dup) => {
                            *dup = true;
                        }
                        PacketIdentifierDupQoS::AtMostOnce => {
                            // No-op
                        }
                    }
                    Packet::Publish(publish)
                }
                other => other,
            }
        } else if let Some(packet) = self.connection_packets.pop_front() {
            packet
        }
        // Otherwise get the next outgoing request
        else {
            // Get the next outgoing packet request, and turn it into a packet
            match self.next_outgoing_request().await {
                OutgoingPacketRequest::DisconnectRequest(disconnect_req) => {
                    Packet::Disconnect(disconnect_req.0)
                }

                OutgoingPacketRequest::AcknowledgementRequest(ack_req) => {
                    match ack_req {
                        // TODO: It would be preferable if the notifier was not triggered on
                        // PUBACK / PUBCOMP until they were actually sent over the network.
                        AcknowledgementRequest::PubAck(notifier, puback, epoch) => {
                            // Do not care about result - if the token was dropped, the user is no longer waiting for it.
                            let _ = notifier.complete(());
                            Packet::PubAck(puback)
                        }

                        AcknowledgementRequest::PubRecAccept(notifier, pubrec, _) => {
                            self.inflight
                                .incoming_pubrec
                                .insert(pubrec.packet_identifier, (pubrec.clone(), notifier));
                            Packet::PubRec(pubrec)
                        }

                        AcknowledgementRequest::PubRecReject(notifier, pubrec, _) => {
                            // Do not care about result - if the token was dropped, the user is no longer waiting for it.
                            let _ = notifier.complete(());
                            Packet::PubRec(pubrec)
                        }

                        AcknowledgementRequest::PubRel(notifier, pubrel, _) => {
                            self.inflight.outgoing_pubrel.insert(
                                pubrel.packet_identifier,
                                OutgoingPubRel::Application(pubrel.clone(), notifier),
                            );
                            Packet::PubRel(pubrel)
                        }

                        AcknowledgementRequest::PubComp(notifier, pubcomp, _) => {
                            // Do not care about result - if the token was dropped, the user is no longer waiting for it.
                            let _ = notifier.complete(());
                            Packet::PubComp(pubcomp)
                        }
                    }
                }

                OutgoingPacketRequest::SubscriptionRequest(sub_req, packet_identifier) => {
                    match sub_req {
                        SubscriptionRequest::Subscribe(
                            notifier,
                            topic_filter,
                            options,
                            other_properties,
                        ) => {
                            self.inflight.subscribe.insert(packet_identifier, notifier);
                            Packet::Subscribe(Subscribe {
                                packet_identifier,
                                subscribe_to: vec![SubscribeTo {
                                    topic_filter,
                                    options,
                                }],
                                other_properties,
                            })
                        }

                        SubscriptionRequest::Unsubscribe(
                            notifier,
                            topic_filter,
                            other_properties,
                        ) => {
                            self.inflight
                                .unsubscribe
                                .insert(packet_identifier, notifier);
                            Packet::Unsubscribe(Unsubscribe {
                                packet_identifier,
                                unsubscribe_from: vec![topic_filter],
                                other_properties,
                            })
                        }
                    }
                }

                OutgoingPacketRequest::PublishRequest(pub_req) => {
                    let packet = match pub_req {
                        PublishRequestWithPkid::PublishQoS0(
                            notifier,
                            topic_name,
                            payload,
                            retain,
                            other_properties,
                        ) => {
                            let publish = Publish {
                                topic_name,
                                packet_identifier_dup_qos: PacketIdentifierDupQoS::AtMostOnce,
                                retain,
                                payload,
                                other_properties,
                            };
                            // Do not care about result - if the token was dropped, the user is no longer waiting for it.
                            let _ = notifier.complete(());
                            publish
                        }

                        PublishRequestWithPkid::PublishQoS1(
                            notifier,
                            topic_name,
                            payload,
                            retain,
                            other_properties,
                            packet_identifier,
                        ) => {
                            let publish = Publish {
                                topic_name,
                                packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                                    packet_identifier,
                                    false, // NOTE: Always dup=false on initial delivery
                                ),
                                retain,
                                payload,
                                other_properties,
                            };
                            self.inflight.publish.insert(
                                packet_identifier,
                                OutgoingPublish::QoS1(publish.clone(), notifier),
                            );
                            publish
                        }

                        PublishRequestWithPkid::PublishQoS2(
                            notifier,
                            topic_name,
                            payload,
                            retain,
                            other_properties,
                            packet_identifier,
                        ) => {
                            let publish = Publish {
                                topic_name,
                                packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(
                                    packet_identifier,
                                    false, // NOTE: Always dup=false on initial delivery
                                ),
                                retain,
                                payload,
                                other_properties,
                            };
                            self.inflight.publish.insert(
                                packet_identifier,
                                OutgoingPublish::QoS2(publish.clone(), notifier),
                            );
                            publish
                        }
                    };
                    Packet::Publish(packet)
                }

                OutgoingPacketRequest::ReauthRequest(auth_req) => {
                    let (notifier, auth) = (auth_req.0, auth_req.1);
                    self.inflight.auth = Some(notifier);
                    Packet::Auth(auth)
                }

                OutgoingPacketRequest::PingReq => Packet::PingReq(PingReq),
            }
        };

        // Reset the ping timer as we are returning a packet that will be sent.
        if let Some(pingreq_timer) = self.pingreq_timer.as_mut() {
            pingreq_timer.reset();
        }

        packet
    }

    /// Returns the next outgoing MQTT packet request to be sent over the network
    async fn next_outgoing_request(&mut self) -> OutgoingPacketRequest<O::Shared> {
        // NOTE: A loop is used here because not all outgoing requests result in a packet being sent
        // e.g. acknowledgement requests with ordering requirements may not be ready
        loop {
            // First check the ordering for a pending acknowledgement that is now ready
            if self
                .in_application
                .publishes
                .first()
                .is_some_and(|(_, publish)| publish.is_ready())
            {
                let (_, publish) = self
                    .in_application
                    .publishes
                    .shift_remove_index(0)
                    .expect("already checked");
                break OutgoingPacketRequest::AcknowledgementRequest(
                    publish.into_ready().expect("already checked"),
                );
            }
            // PUBREL packets are released in the order their successful PUBRECs were received.
            else if self
                .inflight
                .outgoing_pubrel_pending
                .first()
                .is_some_and(|(_, pending)| pending.is_ready())
            {
                let (_, pending) = self
                    .inflight
                    .outgoing_pubrel_pending
                    .shift_remove_index(0)
                    .expect("already checked");
                break OutgoingPacketRequest::AcknowledgementRequest(
                    pending.into_ready().expect("already checked"),
                );
            }

            // Otherwise, poll for the next request and route acknowledgements through their
            // protocol-specific ordering state.
            let request = poll_for_outgoing_request(
                &mut self.ch,
                self.pingreq_timer.as_mut(),
                &mut self.pkid_pool,
            )
            .await;
            if let OutgoingPacketRequest::AcknowledgementRequest(ack_req) = request {
                if let Some(ack_req) = self.route_acknowledgement(ack_req) {
                    break OutgoingPacketRequest::AcknowledgementRequest(ack_req);
                }
            } else {
                break request;
            }
        }
    }

    fn route_acknowledgement(
        &mut self,
        ack_req: AcknowledgementRequest<O::Shared>,
    ) -> Option<AcknowledgementRequest<O::Shared>> {
        let valid = match &ack_req {
            AcknowledgementRequest::PubAck(_, puback, epoch) => {
                *epoch == self.connection_epoch
                    && matches!(
                        self.in_application.publishes.get(&puback.packet_identifier),
                        Some(IncomingPublishState::QoS1(PendingAcknowledgement::NotReady))
                    )
            }
            AcknowledgementRequest::PubRecAccept(_, pubrec, generation)
            | AcknowledgementRequest::PubRecReject(_, pubrec, generation) => {
                *generation == self.session_generation
                    && matches!(
                        self.in_application.publishes.get(&pubrec.packet_identifier),
                        Some(IncomingPublishState::QoS2(PendingAcknowledgement::NotReady))
                    )
            }
            AcknowledgementRequest::PubRel(_, pubrel, generation) => {
                *generation == self.session_generation
                    && matches!(
                        self.inflight
                            .outgoing_pubrel_pending
                            .get(&pubrel.packet_identifier),
                        Some(PendingPubRel::AwaitingApplication)
                    )
            }
            AcknowledgementRequest::PubComp(_, pubcomp, generation) => {
                *generation == self.session_generation
                    && self
                        .inflight
                        .incoming_pubcomp
                        .contains(&pubcomp.packet_identifier)
            }
        };

        if !valid {
            cancel_acknowledgement(ack_req, "Acknowledgement token is no longer valid");
            return None;
        }

        let packet_identifier = match &ack_req {
            AcknowledgementRequest::PubAck(_, packet, _) => packet.packet_identifier,
            AcknowledgementRequest::PubRecAccept(_, packet, _)
            | AcknowledgementRequest::PubRecReject(_, packet, _) => packet.packet_identifier,
            AcknowledgementRequest::PubRel(_, packet, _) => packet.packet_identifier,
            AcknowledgementRequest::PubComp(_, packet, _) => packet.packet_identifier,
        };

        match ack_req {
            ack_req @ (AcknowledgementRequest::PubAck(_, _, _)
            | AcknowledgementRequest::PubRecAccept(_, _, _)
            | AcknowledgementRequest::PubRecReject(_, _, _)) => {
                let pending = self
                    .in_application
                    .publishes
                    .get_mut(&packet_identifier)
                    .expect("validated above");
                pending.set_ready(ack_req);
                None
            }
            ack_req @ AcknowledgementRequest::PubRel(_, _, _) => {
                let pending = self
                    .inflight
                    .outgoing_pubrel_pending
                    .get_mut(&packet_identifier)
                    .expect("validated above");
                *pending = PendingPubRel::Ready(ack_req);
                None
            }
            ack_req @ AcknowledgementRequest::PubComp(_, _, _) => {
                self.inflight.incoming_pubcomp.remove(&packet_identifier);
                Some(ack_req)
            }
        }
    }

    // TODO: semantic fix - incoming_acknowledgement?
    /// Complete an in-flight operation with a received acknowledgement.
    /// Adjusts state as appropriate.
    pub fn complete_inflight(
        &mut self,
        operation: CompletedOperation<O::Shared>,
    ) -> Result<(), ProtocolError> {
        match operation {
            CompletedOperation::Subscribe(suback) => {
                self.pkid_pool.release_pkid(suback.packet_identifier);
                let Some(notifier) = self.inflight.subscribe.remove(&suback.packet_identifier)
                else {
                    return Err(ProtocolErrorRepr::UnexpectedPacket)?;
                };
                _ = notifier.complete(suback);
            }
            CompletedOperation::Unsubscribe(unsuback) => {
                self.pkid_pool.release_pkid(unsuback.packet_identifier);
                let Some(notifier) = self
                    .inflight
                    .unsubscribe
                    .remove(&unsuback.packet_identifier)
                else {
                    return Err(ProtocolErrorRepr::UnexpectedPacket)?;
                };
                _ = notifier.complete(unsuback);
            }
            CompletedOperation::PublishQoS1(puback) => {
                if !matches!(
                    self.inflight.publish.get(&puback.packet_identifier),
                    Some(OutgoingPublish::QoS1(_, _))
                ) {
                    return Err(ProtocolErrorRepr::UnexpectedPacket)?;
                }
                let Some(OutgoingPublish::QoS1(_, notifier)) = self
                    .inflight
                    .publish
                    .shift_remove(&puback.packet_identifier)
                else {
                    unreachable!("PUBLISH variant was validated above");
                };
                self.pkid_pool.release_pkid(puback.packet_identifier);
                _ = notifier.complete(puback);
            }
            CompletedOperation::PublishQoS2(pubrec) => {
                let packet_identifier = pubrec.packet_identifier;
                if matches!(
                    self.inflight.publish.get(&packet_identifier),
                    Some(OutgoingPublish::QoS1(_, _))
                ) {
                    return Err(ProtocolErrorRepr::UnexpectedPacket)?;
                }
                let Some(publish) = self.inflight.publish.shift_remove(&packet_identifier) else {
                    if self
                        .inflight
                        .outgoing_pubrel_pending
                        .contains_key(&packet_identifier)
                    {
                        return Ok(());
                    }

                    if let Some(pubrel) = self
                        .inflight
                        .outgoing_pubrel
                        .get(&packet_identifier)
                        .map(OutgoingPubRel::packet)
                    {
                        self.connection_packets
                            .push_back(Packet::PubRel(pubrel.clone()));
                        return Ok(());
                    }

                    let recovery = PubRel {
                        packet_identifier,
                        reason_code: PubRelReasonCode::PacketIdentifierNotFound,
                        other_properties: Default::default(),
                    };
                    self.inflight.outgoing_pubrel.insert(
                        packet_identifier,
                        OutgoingPubRel::Recovery(recovery.clone()),
                    );
                    self.connection_packets.push_back(Packet::PubRel(recovery));
                    return Ok(());
                };
                let OutgoingPublish::QoS2(_, notifier) = publish else {
                    unreachable!("PUBLISH variant was validated above");
                };

                let token = if pubrec.reason_code.is_success() {
                    self.inflight
                        .outgoing_pubrel_pending
                        .insert(packet_identifier, PendingPubRel::AwaitingApplication);
                    Some(PubRelToken::new(
                        packet_identifier,
                        self.session_generation,
                        self.ch.ack_tx.clone(),
                    ))
                } else {
                    self.pkid_pool.release_pkid(packet_identifier);
                    None
                };
                _ = notifier.complete((pubrec, token));
            }
            CompletedOperation::PubRec(pubrel) => {
                let packet_identifier = pubrel.packet_identifier;
                if let Some((_, notifier)) =
                    self.inflight.incoming_pubrec.remove(&packet_identifier)
                {
                    self.inflight.incoming_pubcomp.insert(packet_identifier);
                    let token = PubCompToken::new(
                        packet_identifier,
                        self.session_generation,
                        self.ch.ack_tx.clone(),
                    );
                    _ = notifier.complete((pubrel, token));
                } else if !self.inflight.incoming_pubcomp.contains(&packet_identifier) {
                    self.connection_packets.push_back(Packet::PubComp(PubComp {
                        packet_identifier,
                        reason_code: PubCompReasonCode::PacketIdentifierNotFound,
                        other_properties: Default::default(),
                    }));
                }
            }
            CompletedOperation::PubRel(pubcomp) => {
                let Some(pubrel) = self
                    .inflight
                    .outgoing_pubrel
                    .shift_remove(&pubcomp.packet_identifier)
                else {
                    return Err(ProtocolErrorRepr::UnexpectedPacket)?;
                };
                match pubrel {
                    OutgoingPubRel::Application(_, notifier) => {
                        self.pkid_pool.release_pkid(pubcomp.packet_identifier);
                        _ = notifier.complete(pubcomp);
                    }
                    OutgoingPubRel::Recovery(_) => {}
                }
            }
        }
        Ok(())
    }

    pub fn incoming_connack(&mut self, connack: ConnAck<O::Shared>, client_keep_alive: KeepAlive) {
        if let ConnectReasonCode::Success { session_present } = connack.reason_code {
            if !session_present {
                // Previous session, if any, is not present on the server.
                self.session_expired();
            }

            self.connection_epoch += 1;

            if matches!(
                connack.other_properties.session_expiry_interval,
                Some(SessionExpiryInterval::Duration(0))
            ) && !self.transient
            {
                // We asked for a persistent session but the server overrode it to transient.
                self.transient = true;
            }

            let keep_alive = if let Some(keep_alive) = connack.other_properties.server_keep_alive {
                keep_alive
            } else {
                client_keep_alive
            };
            match keep_alive {
                KeepAlive::Duration(duration) => {
                    self.pingreq_timer =
                        Some(Timer::new(Duration::from_secs(u64::from(duration.get()))));
                }
                KeepAlive::Infinite => {
                    // If there is no client or server specified keep alive, there is no requirement
                    // to ping, although the client may still choose to do so.
                    self.pingreq_timer = None;
                }
            }

            self.connected = ConnectionState::Connected { connack };
        }
    }

    /// Trigger a disconnect and adjust state based on the information in the outgoing `Disconnect` packet
    pub fn client_disconnect(&mut self, disconnect: &Disconnect<O::Shared>) {
        log::info!("client disconnected by request {disconnect:?}");

        self.disconnected();

        // If the disconnect overrides the session to be transient
        if let Some(SessionExpiryInterval::Duration(0)) =
            disconnect.other_properties.session_expiry_interval
        {
            self.transient = true;
        }

        if self.transient {
            self.session_expired();
        }
    }

    /// Trigger a disconnect and adjust state based on the information in the incoming `Disconnect` packet
    pub fn server_disconnect(&mut self, disconnect: &Disconnect<O::Shared>) {
        log::error!("client disconnected due to server {disconnect:?}");

        self.disconnected();

        // NOTE: Server disconnect cannot override session expiry interval of client.

        if self.transient {
            self.session_expired();
        }
    }

    /// Trigger a disconnect and adjust state based on the error from the underlying transport
    pub fn transport_disconnect(&mut self, err: &InnerConnectionError) {
        log::error!("client disconnected due to transport error {err}");

        self.disconnected();

        if self.transient {
            self.session_expired();
        }
    }

    /// An incoming PUBLISH packet has been received from the server
    pub fn incoming_publish(&mut self, publish: Publish<O::Shared>) {
        let incoming = match publish.packet_identifier_dup_qos {
            PacketIdentifierDupQoS::AtMostOnce => Some(IncomingPublishAndToken::QoS0(publish)),
            PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, _) => {
                let r = self.in_application.publishes.insert(
                    packet_identifier,
                    IncomingPublishState::QoS1(PendingAcknowledgement::NotReady),
                );
                // TODO: How to handle if the pkid already exists? What should the error
                // story / experience be precisely?
                assert!(
                    r.is_none(),
                    "TODO: Handle the case where pkid already exists"
                );
                Some(IncomingPublishAndToken::QoS1(
                    publish,
                    PubAckToken::new(
                        packet_identifier,
                        self.connection_epoch,
                        self.ch.ack_tx.clone(),
                    ),
                ))
            }
            PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, _) => {
                if let Some((pubrec, _)) = self.inflight.incoming_pubrec.get(&packet_identifier) {
                    self.connection_packets
                        .push_back(Packet::PubRec(pubrec.clone()));
                    None
                } else if self
                    .in_application
                    .publishes
                    .contains_key(&packet_identifier)
                    || self.inflight.incoming_pubcomp.contains(&packet_identifier)
                {
                    None
                } else {
                    self.in_application.publishes.insert(
                        packet_identifier,
                        IncomingPublishState::QoS2(PendingAcknowledgement::NotReady),
                    );
                    Some(IncomingPublishAndToken::QoS2(
                        publish,
                        PubRecToken::new(
                            packet_identifier,
                            self.session_generation,
                            self.ch.ack_tx.clone(),
                        ),
                    ))
                }
            }
        };

        // Ignore error from sending to incoming PUBLISH receiver.
        // If there is an error, it's because the application dropped the incoming PUBLISH receiver,
        // in which case `incoming` will be dropped here and will auto-ack it via dropping the ack token.
        if let Some(incoming) = incoming {
            _ = self.ch.i_pub_tx.send(incoming);
        }
    }

    /// An incoming AUTH packet has been received from the server
    pub fn incoming_auth(&mut self, auth: Auth<O::Shared>) -> Result<(), ProtocolError> {
        match auth.reason_code {
            // TODO: Validate authentication method from CONNACK
            AuthenticateReasonCode::Success => {
                let Some(notifier) = self.inflight.auth.take() else {
                    return Err(ProtocolErrorRepr::UnexpectedPacket)?;
                };
                _ = notifier.complete(ReauthResult::Success(auth));
            }
            AuthenticateReasonCode::ContinueAuthentication => {
                //pass on, do not stop tracking
                let Some(notifier) = self.inflight.auth.take() else {
                    return Err(ProtocolErrorRepr::UnexpectedPacket)?;
                };
                let token = ReauthToken {
                    method: auth
                        .authentication
                        .as_ref()
                        .expect("Authentication Method must be present for reason code 0x18")
                        .method
                        .clone(),
                    tx: self.ch.auth_tx.clone(),
                };
                _ = notifier.complete(ReauthResult::Continue(auth, token));
            }
            AuthenticateReasonCode::ReAuthenticate => {
                // AuthenticateReasonCode::ReAuthenticate (0x19) is not possible to be sent by the server
                return Err(ProtocolErrorRepr::UnexpectedPacket)?;
            }
        }
        Ok(())
    }

    /// The connection has been closed for any reason.
    fn disconnected(&mut self) {
        // NOTE: When we cancel CompletionNotifiers here, we don't care about the Result because
        // if it fails, that just means the user no longer has the corresponding CompletionToken

        self.connected = ConnectionState::Disconnected;
        self.pingreq_timer = None;
        self.connection_packets.clear();
        // Remove and cancel all in-flight SUBSCRIBEs
        for (pkid, notifier) in self.inflight.subscribe.drain() {
            let _ = notifier.cancel("Client disconnected");
            self.pkid_pool.release_pkid(pkid);
        }
        // Remove and cancel all in-flight UNSUBSCRIBEs
        for (pkid, notifier) in self.inflight.unsubscribe.drain() {
            let _ = notifier.cancel("Client disconnected");
            self.pkid_pool.release_pkid(pkid);
        }
        // Remove and cancel any in-flight AUTH
        self.inflight
            .auth
            .take()
            .map(|n| n.cancel("Client disconnected"));

        // Incoming QoS 1 acknowledgement controls are connection-scoped. QoS 2 controls belong
        // to the MQTT session and remain valid if the server resumes that session.
        let mut retained = IndexMap::new();
        for (packet_identifier, publish) in std::mem::take(&mut self.in_application.publishes) {
            match publish {
                IncomingPublishState::QoS1(pending) => {
                    pending.cancel("Client disconnected");
                }
                publish @ IncomingPublishState::QoS2(_) => {
                    retained.insert(packet_identifier, publish);
                }
            }
        }
        self.in_application.publishes = retained;

        // Build list of packets to replay
        self.inflight.packets_to_replay.clear();
        for pubrel in self
            .inflight
            .outgoing_pubrel
            .values()
            .map(OutgoingPubRel::packet)
        {
            self.inflight
                .packets_to_replay
                .push_back(Packet::PubRel(pubrel.clone()));
        }
        for publish in self.inflight.publish.values().map(OutgoingPublish::packet) {
            let mut publish = publish.clone();
            if let PacketIdentifierDupQoS::AtLeastOnce(_, dup)
            | PacketIdentifierDupQoS::ExactlyOnce(_, dup) =
                &mut publish.packet_identifier_dup_qos
            {
                *dup = true;
            }
            self.inflight
                .packets_to_replay
                .push_back(Packet::Publish(publish));
        }
    }

    /// Perform state changes when the session is known to be expired on the server:
    ///
    /// 1. The connection closed, and it had originally been established with session expiry interval == 0
    /// 2. The client closed the connect via a DISCONNECT with session expiry interval == 0
    /// 3. A new connection was established and the CONNACK says session present == false
    fn session_expired(&mut self) {
        self.session_generation += 1;

        // Remove and cancel all in-flight PUBLISHes
        for (pkid, publish) in self.inflight.publish.drain(..) {
            publish.cancel("MQTT session expired");
            self.pkid_pool.release_pkid(pkid);
        }
        // Remove and cancel accepted inbound PUBLISHes awaiting PUBREL.
        for (_, (_, notifier)) in self.inflight.incoming_pubrec.drain() {
            let _ = notifier.cancel("MQTT session expired");
        }
        self.inflight.incoming_pubcomp.clear();

        // Release every outbound packet identifier that has progressed beyond PUBREC.
        for (pkid, pending) in self.inflight.outgoing_pubrel_pending.drain(..) {
            pending.cancel("MQTT session expired");
            self.pkid_pool.release_pkid(pkid);
        }
        for (pkid, pubrel) in self.inflight.outgoing_pubrel.drain(..) {
            if let OutgoingPubRel::Application(_, notifier) = pubrel {
                let _ = notifier.cancel("MQTT session expired");
                self.pkid_pool.release_pkid(pkid);
            }
        }

        for (_, publish) in self.in_application.publishes.drain(..) {
            publish.cancel("MQTT session expired");
        }

        self.connection_packets.clear();
        self.inflight.packets_to_replay.clear();

        // NOTE: No need to clear subscribe/unsubscribe/auth here because those are cleared on
        // any disconnect. So any session expiry that happens, either due to disconnect or on the
        // reconnect after the disconnect has already been handled.

        // NOTE: connection_epoch is NOT reset here, since that would allow for old tokens to become valid again.
    }
}

enum ConnectionState<S>
where
    S: Shared,
{
    Disconnected,
    Connected { connack: ConnAck<S> },
}

/// A desired operation initiated by the client
enum OutgoingOperation<S>
where
    S: Shared,
{
    Subscribe(PacketIdentifier, SubscribeCompletionNotifier<S>),
    Unsubscribe(PacketIdentifier, UnsubscribeCompletionNotifier<S>),
    PublishQoS1(Publish<S>, PublishQoS1CompletionNotifier<S>),
}

enum OutgoingPublish<S>
where
    S: Shared,
{
    QoS1(Publish<S>, PublishQoS1CompletionNotifier<S>),
    QoS2(Publish<S>, PublishQoS2CompletionNotifier<S>),
}

impl<S> OutgoingPublish<S>
where
    S: Shared,
{
    fn packet(&self) -> &Publish<S> {
        match self {
            Self::QoS1(publish, _) | Self::QoS2(publish, _) => publish,
        }
    }

    fn cancel(self, reason: &str) {
        match self {
            Self::QoS1(_, notifier) => {
                let _ = notifier.cancel(reason);
            }
            Self::QoS2(_, notifier) => {
                let _ = notifier.cancel(reason);
            }
        }
    }
}

/// A response to an operation initiated by the client
pub enum CompletedOperation<S>
where
    S: Shared,
{
    PublishQoS1(PubAck<S>),
    PublishQoS2(PubRec<S>),
    PubRec(PubRel<S>),
    PubRel(PubComp<S>),
    Subscribe(SubAck<S>),
    Unsubscribe(UnsubAck<S>),
}

/// Organizational struct containing channels on which the `Session` receives input
pub(crate) struct Channels<S>
where
    S: Shared,
{
    // --- Channels used to receive in the Session ---
    /// Channel for receiving outgoing CONNECT and DISCONNECT requests
    pub(crate) disconnect_rx: Option<tokio::sync::oneshot::Receiver<DisconnectRequest<S>>>,
    /// Channel for receiving outgoing PUBLISH requests (QoS 0)
    o_pub_q0_rx: Receiver<PublishRequestQoS0<S>>,
    /// Channel for receiving outgoing PUBLISH requests (QoS 1, 2)
    o_pub_q12_rx: Peekable<ReceiverStream<PublishRequestQoS1QoS2<S>>>,
    /// Channel for receiving outgoing SUBSCRIBE and UNSUBSCRIBE requests
    sub_rx: Peekable<ReceiverStream<SubscriptionRequest<S>>>,
    /// Channel for receiving outgoing PUBACK, PUBREC, PUBREL and PUBCOMP requests
    ack_rx: UnboundedReceiver<AcknowledgementRequest<S>>,
    /// Channel for receiving outgoing AUTH requests
    auth_rx: Receiver<ReauthRequest<S>>,
    /// Channel for sending incoming PUBLISHes and associated acknowledgement tokens
    i_pub_tx: UnboundedSender<IncomingPublishAndToken<S>>,

    // --- Channels stored here to be cloned, and should not be used directly ---
    // TODO: Is this really the correct place for these?
    /// Channel for sending outgoing PUBACK, PUBREC, PUBREL and PUBCOMP requests
    ack_tx: UnboundedSender<AcknowledgementRequest<S>>,
    /// Channel for sending outgoing AUTH requests
    pub(crate) auth_tx: Sender<ReauthRequest<S>>, // TODO: ideally this would not be pub crate
}

enum OutgoingPacketRequest<S>
where
    S: Shared,
{
    DisconnectRequest(DisconnectRequest<S>),
    AcknowledgementRequest(AcknowledgementRequest<S>),
    SubscriptionRequest(SubscriptionRequest<S>, PacketIdentifier),
    PublishRequest(PublishRequestWithPkid<S>),
    ReauthRequest(ReauthRequest<S>),
    PingReq,
}

/// This represents a `PublishRequest` that has been assigned a packet identifier if it needed one.
/// So its definition is identical to `PublishRequest`, but with an additional `PacketIdentifier` field
/// for the QoS 1 and QoS 2 variants.
enum PublishRequestWithPkid<S>
where
    S: Shared,
{
    PublishQoS0(
        PublishQoS0CompletionNotifier,
        Topic<ByteStr<S>>,
        S,
        bool,
        PublishOtherProperties<S>,
    ),
    PublishQoS1(
        PublishQoS1CompletionNotifier<S>,
        Topic<ByteStr<S>>,
        S,
        bool,
        PublishOtherProperties<S>,
        PacketIdentifier,
    ),
    PublishQoS2(
        PublishQoS2CompletionNotifier<S>,
        Topic<ByteStr<S>>,
        S,
        bool,
        PublishOtherProperties<S>,
        PacketIdentifier,
    ),
}

/// Poll for the next outgoing packet request.
/// Priority order: Disconnects, Acknowledgements, Subscriptions, Publishes, Pings
fn poll_for_outgoing_request<S>(
    ch: &mut Channels<S>,
    mut pingreq_timer: Option<&mut Timer>,
    pkid_pool: &mut PkidPool,
) -> impl Future<Output = OutgoingPacketRequest<S>>
where
    S: Shared,
{
    futures_util::future::poll_fn(move |cx| {
        // Disconnects get top priority, since they indicate the user wants to close the connection now.
        if let Some(disconnect_rx) = &mut ch.disconnect_rx
            && let Poll::Ready(disconnect_req) = Pin::new(disconnect_rx).poll(cx)
        {
            drop(ch.disconnect_rx.take());
            if let Ok(disconnect_req) = disconnect_req {
                return Poll::Ready(OutgoingPacketRequest::DisconnectRequest(disconnect_req));
            }
            // ... else: User dropped the disconnect_tx, so there's nothing more to do.
        }

        // Next priority are acknowledgements, as they can free up packet ids
        if let Poll::Ready(Some(ack_req)) = ch.ack_rx.poll_recv(cx) {
            return Poll::Ready(OutgoingPacketRequest::AcknowledgementRequest(ack_req));
        }

        // Next priority are auth requests as they are important for maintaining connection
        // TODO: Ideally, no polling for reauth if one is already in progress
        if let Poll::Ready(Some(auth_req)) = ch.auth_rx.poll_recv(cx) {
            return Poll::Ready(OutgoingPacketRequest::ReauthRequest(auth_req));
        }

        // Next priority are subscription requests
        if let Poll::Ready(Some(_)) = Pin::new(&mut ch.sub_rx).peek().poll_unpin(cx)
            && let Some(pkid) = pkid_pool.lease_next_pkid()
        {
            let Poll::Ready(Some(sub_req)) = ch.sub_rx.poll_next_unpin(cx) else {
                unreachable!("peek() confirmed the stream has an element");
            };
            return Poll::Ready(OutgoingPacketRequest::SubscriptionRequest(sub_req, pkid));
        }

        // Next priority are QoS 0 publishes (which do not need packet ids)
        if let Poll::Ready(Some(pub_req)) = ch.o_pub_q0_rx.poll_recv(cx) {
            let PublishRequestQoS0(notifier, topic, payload, retain, properties) = pub_req;
            return Poll::Ready(OutgoingPacketRequest::PublishRequest(
                PublishRequestWithPkid::PublishQoS0(notifier, topic, payload, retain, properties),
            ));
        }

        // Next priority are QoS 1 and QoS 2 publishes (which do need packet ids)
        if let Poll::Ready(Some(publish)) = Pin::new(&mut ch.o_pub_q12_rx).peek().poll_unpin(cx)
            && let Some(pkid) = pkid_pool.lease_next_pkid()
        {
            let Poll::Ready(Some(publish)) = ch.o_pub_q12_rx.poll_next_unpin(cx) else {
                unreachable!("peek() confirmed the stream has an element");
            };
            return Poll::Ready(OutgoingPacketRequest::PublishRequest(match publish {
                PublishRequestQoS1QoS2::PublishQoS1(
                    notifier,
                    topic,
                    payload,
                    retain,
                    properties,
                ) => PublishRequestWithPkid::PublishQoS1(
                    notifier, topic, payload, retain, properties, pkid,
                ),
                PublishRequestQoS1QoS2::PublishQoS2(
                    notifier,
                    topic,
                    payload,
                    retain,
                    properties,
                ) => PublishRequestWithPkid::PublishQoS2(
                    notifier, topic, payload, retain, properties, pkid,
                ),
            }));
        }

        // Finally, if no other packets are ready to send, check if enought time has elapsed for a ping
        if let Some(ref mut pingreq_timer) = pingreq_timer
            && let Poll::Ready(()) = Pin::new(&mut *pingreq_timer).poll(cx)
        {
            return Poll::Ready(OutgoingPacketRequest::PingReq);
        }

        Poll::Pending
    })
}

/// Contains data related to in-flight operations pending a response
#[derive_where(Default)]
struct InflightTracker<S>
where
    S: Shared,
{
    // NOTE: We use IndexMap to preserve insertion order for packet types where ordering matters
    // e.g. Publishes are redelivered in the order they were originally sent.
    // It is more performative to use a HashMap in cases where we do not care.

    // --- Operation tracking ---
    // None of these hashmaps should ever use the same key at the same time, although this is not
    // enforced for simplicity.
    /// All inflight QoS 1 and QoS 2 PUBLISH operations, in original send order
    publish: IndexMap<PacketIdentifier, OutgoingPublish<S>>,
    /// All inflight SUBSCRIBE operations
    subscribe: HashMap<PacketIdentifier, SubscribeCompletionNotifier<S>>,
    /// All inflight UNSUBSCRIBE operations
    unsubscribe: HashMap<PacketIdentifier, UnsubscribeCompletionNotifier<S>>,

    // --- Acknowledgement tracking ---
    // None of these hashmaps should ever use the same key at the same time, although this is not
    // enforced for simplicity.
    /// Successful outbound PUBRECs awaiting application confirmation, in receive order
    outgoing_pubrel_pending: IndexMap<PacketIdentifier, PendingPubRel<S>>,
    /// Outbound PUBREL packets awaiting PUBCOMP
    outgoing_pubrel: IndexMap<PacketIdentifier, OutgoingPubRel<S>>,
    /// Accepted inbound QoS 2 PUBLISHes awaiting PUBREL
    incoming_pubrec: HashMap<PacketIdentifier, (PubRec<S>, PubRecAcceptCompletionNotifier<S>)>,
    /// Inbound PUBREL packets awaiting application confirmation
    incoming_pubcomp: HashSet<PacketIdentifier>,

    packets_to_replay: VecDeque<Packet<S>>,

    // --- Other ----
    /// Inflight AUTH operation, if any.
    auth: Option<ReauthCompletionNotifier<S>>,
}

#[derive_where(Default)]
struct InApplicationTracker<S>
where
    S: Shared,
{
    publishes: IndexMap<PacketIdentifier, IncomingPublishState<S>>,
}

enum IncomingPublishState<S>
where
    S: Shared,
{
    QoS1(PendingAcknowledgement<S>),
    QoS2(PendingAcknowledgement<S>),
}

impl<S> IncomingPublishState<S>
where
    S: Shared,
{
    fn is_ready(&self) -> bool {
        matches!(
            self,
            Self::QoS1(PendingAcknowledgement::Ready(_))
                | Self::QoS2(PendingAcknowledgement::Ready(_))
        )
    }

    fn set_ready(&mut self, ack_req: AcknowledgementRequest<S>) {
        let pending = match self {
            Self::QoS1(pending) | Self::QoS2(pending) => pending,
        };
        debug_assert!(matches!(pending, PendingAcknowledgement::NotReady));
        *pending = PendingAcknowledgement::Ready(ack_req);
    }

    fn into_ready(self) -> Option<AcknowledgementRequest<S>> {
        match self {
            Self::QoS1(PendingAcknowledgement::Ready(ack_req))
            | Self::QoS2(PendingAcknowledgement::Ready(ack_req)) => Some(ack_req),
            Self::QoS1(PendingAcknowledgement::NotReady)
            | Self::QoS2(PendingAcknowledgement::NotReady) => None,
        }
    }

    fn cancel(self, reason: &str) {
        match self {
            Self::QoS1(pending) | Self::QoS2(pending) => pending.cancel(reason),
        }
    }
}

enum PendingAcknowledgement<S>
where
    S: Shared,
{
    NotReady,
    Ready(AcknowledgementRequest<S>),
}

impl<S> PendingAcknowledgement<S>
where
    S: Shared,
{
    fn cancel(self, reason: &str) {
        if let Self::Ready(ack_req) = self {
            cancel_acknowledgement(ack_req, reason);
        }
    }
}

enum PendingPubRel<S>
where
    S: Shared,
{
    AwaitingApplication,
    Ready(AcknowledgementRequest<S>),
}

impl<S> PendingPubRel<S>
where
    S: Shared,
{
    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    fn into_ready(self) -> Option<AcknowledgementRequest<S>> {
        match self {
            Self::Ready(ack_req) => Some(ack_req),
            Self::AwaitingApplication => None,
        }
    }

    fn cancel(self, reason: &str) {
        if let Self::Ready(ack_req) = self {
            cancel_acknowledgement(ack_req, reason);
        }
    }
}

enum OutgoingPubRel<S>
where
    S: Shared,
{
    Application(PubRel<S>, PubRelCompletionNotifier<S>),
    Recovery(PubRel<S>),
}

impl<S> OutgoingPubRel<S>
where
    S: Shared,
{
    fn packet(&self) -> &PubRel<S> {
        match self {
            Self::Application(pubrel, _) | Self::Recovery(pubrel) => pubrel,
        }
    }
}

fn cancel_acknowledgement<S>(ack_req: AcknowledgementRequest<S>, reason: &str)
where
    S: Shared,
{
    match ack_req {
        AcknowledgementRequest::PubAck(notifier, _, _)
        | AcknowledgementRequest::PubRecReject(notifier, _, _)
        | AcknowledgementRequest::PubComp(notifier, _, _) => {
            let _ = notifier.cancel(reason);
        }
        AcknowledgementRequest::PubRecAccept(notifier, _, _) => {
            let _ = notifier.cancel(reason);
        }
        AcknowledgementRequest::PubRel(notifier, _, _) => {
            let _ = notifier.cancel(reason);
        }
    }
}

struct ReceiverStream<T>(Receiver<T>);

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tokio::sync::mpsc::{channel, unbounded_channel};

    use super::{OutgoingPacketRequest, Session};
    use crate::client::channel_data::AcknowledgementRequest;
    use crate::client::token::completion::buffered::completion_pair;
    use crate::mqtt_proto::{PacketIdentifier, PubComp, PubCompReasonCode};

    #[tokio::test]
    async fn pubcomp_bypasses_publish_acknowledgement_ordering() {
        let (_sub_tx, sub_rx) = channel(1);
        let (_publish_qos0_tx, publish_qos0_rx) = channel(1);
        let (_publish_qos12_tx, publish_qos12_rx) = channel(1);
        let (ack_tx, ack_rx) = unbounded_channel();
        let (auth_tx, auth_rx) = channel(1);
        let (incoming_publish_tx, _incoming_publish_rx) = unbounded_channel();
        let mut session = Session::new(
            sub_rx,
            publish_qos0_rx,
            publish_qos12_rx,
            ack_rx,
            auth_rx,
            incoming_publish_tx,
            ack_tx.clone(),
            auth_tx,
            PacketIdentifier::new(u16::MAX).unwrap(),
            BytesMut::new(),
        );
        let pubcomp = PubComp {
            packet_identifier: PacketIdentifier::new(1).unwrap(),
            reason_code: PubCompReasonCode::Success,
            other_properties: Default::default(),
        };
        let (notifier, _completion_token) = completion_pair();

        ack_tx
            .send(AcknowledgementRequest::PubComp(notifier, pubcomp.clone()))
            .unwrap();

        let OutgoingPacketRequest::AcknowledgementRequest(AcknowledgementRequest::PubComp(
            _,
            outgoing_pubcomp,
        )) = session.next_outgoing_request().await
        else {
            panic!("expected PUBCOMP acknowledgement request");
        };
        assert_eq!(outgoing_pubcomp, pubcomp);
        assert!(session.in_application.publishes.is_empty());
    }
}
