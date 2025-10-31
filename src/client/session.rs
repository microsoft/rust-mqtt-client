// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::task::Poll;

use derive_where::derive_where;
use futures_util::future::FutureExt as _;
use futures_util::stream::{Peekable, Stream, StreamExt as _};
use indexmap::IndexMap;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender};
use tokio::time::Duration;

use crate::buffer_pool::{Owned, Shared};
use crate::client::token::ReauthCompletionNotifier;
use crate::client::{
    channel_data::{
        AcknowledgementRequest, DisconnectRequest, IncomingPublish, PublishRequest, ReauthRequest,
        ReauthResponse, ReauthToken, SubscriptionRequest,
    },
    session::pkid::PkidPool,
    session::timer::Timer,
    token::{
        AckHandle, CompletionNotifier, PubAckToken, PubCompToken, PubRecAcceptCompletionNotifier,
        PubRelCompletionNotifier, PubRelToken, PublishQoS0CompletionNotifier,
        PublishQoS1CompletionNotifier, PublishQoS2CompletionNotifier, SubscribeCompletionNotifier,
        UnsubscribeCompletionNotifier,
    },
};
use crate::mqtt_proto::{
    Auth, AuthenticateReasonCode, ByteStr, ConnAck, ConnectReasonCode, Disconnect, Packet,
    PacketIdentifier, PacketIdentifierDupQoS, PingReq, PubAck, PubComp, PubRec, PubRel, Publish,
    PublishOtherProperties, RetainHandling, SessionExpiryInterval, SubAck, Subscribe,
    SubscribeOptions, SubscribeOptionsOtherProperties, SubscribeTo, Topic, UnsubAck, Unsubscribe,
};

mod pkid;
mod timer;

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
    /// Tracker of all inflight MQTT packets awaiting a response
    inflight: InflightTracker<O::Shared>,
    /// Tracker of MQTT packets sent to the application, awaiting a response
    in_application: InApplicationTracker<O::Shared>,
    /// Whether the session is currently connected, and if so, the CONNACK
    connected: ConnectionState<O::Shared>,
    /// Identifier for the current connection epoch
    connection_epoch: u64,
    transient: bool,
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
        o_pub_rx: Receiver<PublishRequest<O::Shared>>,
        ack_rx: Receiver<AcknowledgementRequest<O::Shared>>,
        auth_rx: Receiver<ReauthRequest<O::Shared>>,
        i_pub_tx: UnboundedSender<IncomingPublish<O::Shared>>,
        ack_tx: Sender<AcknowledgementRequest<O::Shared>>,
        auth_tx: Sender<ReauthRequest<O::Shared>>,
        max_pkid: PacketIdentifier,
        owned: O,
    ) -> Self {
        let ch = Channels {
            disconnect_rx: None,
            o_pub_rx: ReceiverStream(o_pub_rx).peekable(),
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
            inflight: InflightTracker::default(),
            in_application: InApplicationTracker::default(),
            connected: ConnectionState::Disconnected,
            connection_epoch: 0, // move this to the connection state?
            transient: false,    // move this to the connection state?
            pingreq_timer: None,
            owned,
        }
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.connected, ConnectionState::Connected { .. })
    }

    /// Returns the next outgoing MQTT packet to be sent over the network
    #[allow(clippy::unused_self)]
    pub async fn next_outgoing_packet(&mut self) -> Option<Packet<O::Shared>> {
        // TODO: Now that sending CONNECT is handled outside of `Session::next_outgoing_packet`,
        // it will only ever be called after `incoming_connack(ConnAck)` has been called, right?
        assert!(self.is_connected());

        // NOTE: A loop is used here because not all outgoing requests result in a packet being sent
        // e.g. acknowledgement requests need to be ordered
        let packet = loop {
            // Check for replayed packets first
            if let Some(packet) = self.inflight.packets_to_replay.pop_front() {
                // TODO: Replayed PUBLISHes should be subject to server's receive-maximum.
                // TODO: Need to set dup flag on replayed PUBLISHes.
                break packet;
            }
            // Check if a the next pending acknowledgement in order is ready
            else if let Some((_, PendingAcknowledgement::Ready(ack_req))) =
                self.in_application.publishes.first()
            {
                if let (_, PendingAcknowledgement::Ready(ack_req)) = self
                    .in_application
                    .publishes
                    .shift_remove_index(0)
                    .expect("Already checked")
                {
                    match ack_req {
                        // TODO: Reject PUBACK if epoch does not match current connection epoch
                        // TODO: It would be preferable if the notifier was not triggered on
                        // PUBACK / PUBCOMP until they were actually sent over the network.
                        AcknowledgementRequest::PubAck(notifier, puback, epoch) => {
                            // Do not care about result - if the token was dropped, the user is no longer waiting for it.
                            let _ = notifier.complete(());
                            break Packet::PubAck(puback);
                        }

                        AcknowledgementRequest::PubRecAccept(notifier, pubrec) => {
                            self.inflight
                                .pubrec
                                .insert(pubrec.packet_identifier, (pubrec.clone(), notifier));
                            break Packet::PubRec(pubrec);
                        }

                        AcknowledgementRequest::PubRecReject(notifier, pubrec) => {
                            // Do not care about result - if the token was dropped, the user is no longer waiting for it.
                            let _ = notifier.complete(());
                            break Packet::PubRec(pubrec);
                        }

                        AcknowledgementRequest::PubRel(notifier, pubrel) => {
                            self.inflight
                                .pubrel
                                .insert(pubrel.packet_identifier, (pubrel.clone(), notifier));
                            break Packet::PubRel(pubrel);
                        }

                        AcknowledgementRequest::PubComp(notifier, pubcomp) => {
                            // Do not care about result - if the token was dropped, the user is no longer waiting for it.
                            let _ = notifier.complete(());
                            break Packet::PubComp(pubcomp);
                        }
                    }
                }
            }
            // Otherwise poll for the next outgoing request
            else {
                // Get the next outgoing packet request, and turn it into a packet
                #[allow(unreachable_code)] // TODO: Remove when todo!()s are resolved
                match self.next_outgoing_request().await {
                    OutgoingPacketRequest::DisconnectRequest(disconnect_req) => {
                        break Packet::Disconnect(Disconnect {
                            reason_code: todo!(),
                            other_properties: todo!(),
                        });
                    }

                    OutgoingPacketRequest::AcknowledgementRequest(ack_req) => {
                        let pkid = match &ack_req {
                            AcknowledgementRequest::PubAck(_, puback, _) => {
                                puback.packet_identifier
                            }
                            AcknowledgementRequest::PubRecAccept(_, pubrec)
                            | AcknowledgementRequest::PubRecReject(_, pubrec) => {
                                pubrec.packet_identifier
                            }
                            AcknowledgementRequest::PubRel(_, pubrel) => pubrel.packet_identifier,
                            AcknowledgementRequest::PubComp(_, pubcomp) => {
                                pubcomp.packet_identifier
                            }
                        };
                        let pending = self
                            .in_application
                            .publishes
                            .get_mut(&pkid)
                            .expect("TODO: error handling");
                        *pending = PendingAcknowledgement::Ready(ack_req);
                        //continue; Commented because clippy doesn't like it, but I think it's clearer
                    }
                    OutgoingPacketRequest::SubscriptionRequest(sub_req, packet_identifier) => {
                        break match sub_req {
                            SubscriptionRequest::Subscribe(
                                notifier,
                                topic_filter,
                                qos,
                                other_properties,
                            ) => {
                                self.inflight.subscribe.insert(packet_identifier, notifier);
                                Packet::Subscribe(Subscribe {
                                    packet_identifier,
                                    subscribe_to: vec![SubscribeTo {
                                        topic_filter,
                                        options: SubscribeOptions {
                                            maximum_qos: qos,
                                            // TODO: Get from sub_req
                                            other_properties: SubscribeOptionsOtherProperties {
                                                no_local: false,
                                                retain_as_published: false,
                                                retain_handling: RetainHandling::Send,
                                            },
                                        },
                                    }],
                                    other_properties,
                                })
                            }

                            SubscriptionRequest::Unsubscribe(notifier, ..) => {
                                self.inflight
                                    .unsubscribe
                                    .insert(packet_identifier, notifier);
                                Packet::Unsubscribe(Unsubscribe {
                                    packet_identifier,
                                    unsubscribe_from: todo!(),
                                    other_properties: todo!(),
                                })
                            }
                        };
                    }

                    OutgoingPacketRequest::PublishRequest(pub_req) => {
                        let packet = match pub_req {
                            PublishRequestWithPkid::PublishQoS0(
                                notifier,
                                topic_name,
                                payload,
                                other_properties,
                            ) => {
                                let publish = Publish {
                                    topic_name,
                                    packet_identifier_dup_qos: PacketIdentifierDupQoS::AtMostOnce,
                                    retain: false, // TODO: Get from pub_req
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
                                other_properties,
                                packet_identifier,
                            ) => {
                                let publish = Publish {
                                    topic_name,
                                    packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                                        packet_identifier,
                                        false, // TODO: Get from pub_req
                                    ),
                                    retain: false, // TODO: Get from pub_req
                                    payload,
                                    other_properties,
                                };
                                self.inflight
                                    .publish_qos1
                                    .insert(packet_identifier, (publish.clone(), notifier));
                                publish
                            }

                            PublishRequestWithPkid::PublishQoS2(
                                notifier,
                                topic_name,
                                payload,
                                other_properties,
                                packet_identifier,
                            ) => {
                                let publish = Publish {
                                    topic_name,
                                    packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(
                                        packet_identifier,
                                        false, // TODO: Get from pub_req
                                    ),
                                    retain: false, // TODO: Get from pub_req
                                    payload,
                                    other_properties,
                                };
                                self.inflight
                                    .publish_qos2
                                    .insert(packet_identifier, (publish.clone(), notifier));
                                publish
                            }
                        };
                        break Packet::Publish(packet);
                    }

                    OutgoingPacketRequest::ReauthRequest(auth_req) => {
                        let (notifier, auth) = (auth_req.0, auth_req.1);
                        self.inflight.auth = Some(notifier);
                        break Packet::Auth(auth);
                    }

                    OutgoingPacketRequest::PingReq => break Packet::PingReq(PingReq),
                }
            }
        };

        // Reset the ping timer as we are returning a packet that will be sent.
        if let Some(pingreq_timer) = self.pingreq_timer.as_mut() {
            pingreq_timer.reset();
        }

        Some(packet)
    }

    async fn next_outgoing_request(&mut self) -> OutgoingPacketRequest<O::Shared> {
        poll_for_outgoing_request(
            &mut self.ch,
            self.pingreq_timer.as_mut(),
            &mut self.pkid_pool,
        )
        .await
    }

    // TODO: semantic fix - incoming_acknowledgement?
    /// Complete an in-flight operation with a received acknowledgement.
    /// Adjusts state as appropriate.
    pub fn complete_inflight(&mut self, operation: CompletedOperation<O::Shared>) {
        match operation {
            CompletedOperation::Subscribe(suback) => {
                self.pkid_pool.release_pkid(suback.packet_identifier);
                let notifier = self
                    .inflight
                    .subscribe
                    .remove(&suback.packet_identifier)
                    .expect("TODO: error handling");
                _ = notifier.complete(suback);
            }
            CompletedOperation::Unsubscribe(unsuback) => {
                self.pkid_pool.release_pkid(unsuback.packet_identifier);
                let notifier = self
                    .inflight
                    .unsubscribe
                    .remove(&unsuback.packet_identifier)
                    .expect("TODO: error handling");
                _ = notifier.complete(unsuback);
            }
            CompletedOperation::PublishQoS1(puback) => {
                self.pkid_pool.release_pkid(puback.packet_identifier);
                let (_, notifier) = self
                    .inflight
                    .publish_qos1
                    .shift_remove(&puback.packet_identifier)
                    .expect("TODO: error handling");
                _ = notifier.complete(puback);
            }
            CompletedOperation::PublishQoS2(pubrec) => {
                let token = if pubrec.reason_code.is_success() {
                    // Pubrec accept token
                    Some(PubRelToken::new(
                        pubrec.packet_identifier,
                        self.ch.ack_tx.clone(),
                    ))
                } else {
                    // Release pkid because there will be no pubrel/pubcomp exchange
                    self.pkid_pool.release_pkid(pubrec.packet_identifier);
                    // No token
                    None
                };
                let (_, notifier) = self
                    .inflight
                    .publish_qos2
                    .shift_remove(&pubrec.packet_identifier)
                    .expect("TODO: error handling");

                _ = notifier.complete((pubrec, token));
            }
            CompletedOperation::PubRec(pubrel) => {
                let (_, notifier) = self
                    .inflight
                    .pubrec
                    .remove(&pubrel.packet_identifier)
                    .expect("TODO: error handling");
                let token = PubCompToken::new(pubrel.packet_identifier, self.ch.ack_tx.clone());
                _ = notifier.complete((pubrel, token));
            }
            CompletedOperation::PubRel(pubcomp) => {
                self.pkid_pool.release_pkid(pubcomp.packet_identifier);
                let (_, notifier) = self
                    .inflight
                    .pubrel
                    .shift_remove(&pubcomp.packet_identifier)
                    .expect("TODO: error handling");
                _ = notifier.complete(pubcomp);
            }
        }
    }

    pub fn incoming_connack(&mut self, connack: ConnAck<O::Shared>) {
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

            self.connected = ConnectionState::Connected { connack };

            // TODO: Get PINGREQ duration from connect properties
            self.pingreq_timer = Some(Timer::new(Duration::from_secs(5)));
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
    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    #[allow(clippy::unused_self)]
    pub fn server_disconnect(&mut self, disconnect: &Disconnect<O::Shared>) {
        log::error!("client disconnected due to server {disconnect:?}");

        self.disconnected();

        // NOTE: Server disconnect cannot override session expiry interval of client.

        if self.transient {
            self.session_expired();
        }
    }

    /// Trigger a disconnect and adjust state based on the error from the underlying transport
    pub fn transport_disconnect(&mut self, err: &std::io::Error) {
        log::error!("client disconnected due to tranport error {err}");

        self.disconnected();

        if self.transient {
            self.session_expired();
        }
    }

    /// An incoming PUBLISH packet has been received from the server
    pub fn incoming_publish(&mut self, publish: Publish<O::Shared>) {
        let ack_handle = match publish.packet_identifier_dup_qos {
            PacketIdentifierDupQoS::AtMostOnce => AckHandle::QoS0,
            PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, _) => {
                self.in_application
                    .publishes
                    .insert(packet_identifier, PendingAcknowledgement::NotReady);
                AckHandle::QoS1(PubAckToken::new(
                    packet_identifier,
                    self.connection_epoch,
                    self.ch.ack_tx.clone(),
                ))
            }
            PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, _) => {
                self.in_application
                    .publishes
                    .insert(packet_identifier, PendingAcknowledgement::NotReady);
                todo!()
            }
        };
        // TODO: Register ack tracking if QoS 1 or 2
        self.ch
            .i_pub_tx
            .send((publish, ack_handle))
            .expect("TODO: error handling");
    }

    /// An incoming AUTH packet has been received from the server
    pub fn incoming_auth(&mut self, auth: Auth<O::Shared>) {
        match auth.reason_code {
            // TODO: Validate authentication method from CONNACK
            AuthenticateReasonCode::Success => {
                let notifier = self.inflight.auth.take().expect("TODO: error handling");
                _ = notifier.complete(ReauthResponse::Success(auth));
            }
            AuthenticateReasonCode::ContinueAuthentication => {
                //pass on, do not stop tracking
                let notifier = self.inflight.auth.take().expect("TODO: error handling");
                let token = ReauthToken {
                    method: auth
                        .authentication
                        .as_ref()
                        .expect("Authentication Method must be present for reason code 0x18")
                        .method
                        .clone(),
                    tx: self.ch.auth_tx.clone(),
                };
                _ = notifier.complete(ReauthResponse::Continue(auth, token));
            }
            AuthenticateReasonCode::ReAuthenticate => unreachable!(
                "AuthenticateReasonCode::ReAuthenticate (0x19) is not possible to be sent by the server"
            ),
        }
    }

    /// The connection has been closed for any reason.
    fn disconnected(&mut self) {
        // NOTE: When we cancel CompletionNotifiers here, we don't care about the Result because
        // if it fails, that just means the user no longer has the corresponding CompletionToken

        self.connected = ConnectionState::Disconnected;
        self.pingreq_timer = None;
        // Remove and cancel all in-flight SUBSCRIBEs
        for (pkid, notifier) in self.inflight.subscribe.drain() {
            let _ = notifier.cancel();
            self.pkid_pool.release_pkid(pkid);
        }
        // Remove and cancel all in-flight UNSUBSCRIBEs
        for (pkid, notifier) in self.inflight.unsubscribe.drain() {
            let _ = notifier.cancel();
            self.pkid_pool.release_pkid(pkid);
        }

        // Build list of packets to replay
        self.inflight.packets_to_replay.clear();
        for (pubrel, _) in self.inflight.pubrel.values() {
            self.inflight
                .packets_to_replay
                .push_back(Packet::PubRel(pubrel.clone()));
        }
        for publish in self
            .inflight
            .publish_qos1
            .values()
            .map(|(publish, _)| publish)
            .chain(
                self.inflight
                    .publish_qos2
                    .values()
                    .map(|(publish, _)| publish),
            )
        {
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

        // Remove and cancel any in-flight AUTH
        self.inflight.auth.take().map(CompletionNotifier::cancel);
    }

    /// Perform state changes when the session is known to be expired on the server:
    ///
    /// 1. The connection closed, and it had originally been established with session expiry interval == 0
    /// 2. The client closed the connect via a DISCONNECT with session expiry interval == 0
    /// 3. A new connection was established and the CONNACK says session present == false
    fn session_expired(&mut self) {
        // Remove and cancel all in-flight QoS 1 PUBLISHes
        for (pkid, (_, notifier)) in self.inflight.publish_qos1.drain(..) {
            let _ = notifier.cancel();
            self.pkid_pool.release_pkid(pkid);
        }
        // Remove and cancel all in-flight QoS 2 PUBLISHes
        for (pkid, (_, notifier)) in self.inflight.publish_qos2.drain(..) {
            let _ = notifier.cancel();
            self.pkid_pool.release_pkid(pkid);
        }
        // Remove and cancel all in-flight PUBREC
        for (pkid, (_, notifier)) in self.inflight.pubrec.drain() {
            let _ = notifier.cancel();
        }
        // Remove and cancel all in-flight PUBREL
        for (pkid, (_, notifier)) in self.inflight.pubrel.drain(..) {
            let _ = notifier.cancel();
        }
        self.inflight.packets_to_replay.clear();

        // NOTE: No need to clear subscribe/unsubscribe/auth here because those are cleared on
        // any disconnect. So any session expiry that happens, either due to disconnect or on the
        // reconnect after the disconnect has already been handled.

        // NOTE: PUBREC/PUBREL do not release their PKIDs because those are not leased from the PKID pool.

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
    /// Channel for receiving outgoing PUBLISH requests
    o_pub_rx: Peekable<ReceiverStream<PublishRequest<S>>>,
    /// Channel for receiving outgoing SUBSCRIBE and UNSUBSCRIBE requests
    sub_rx: Peekable<ReceiverStream<SubscriptionRequest<S>>>,
    /// Channel for receving outgoing PUBACK, PUBREC, PUBREL and PUBCOMP requests
    ack_rx: Receiver<AcknowledgementRequest<S>>,
    /// Channel for receiving outgoing AUTH requests
    auth_rx: Receiver<ReauthRequest<S>>,
    /// Channel for sending incoming PUBLISH requests
    i_pub_tx: UnboundedSender<IncomingPublish<S>>,

    // --- Channels stored here to be cloned, and should not be used directly ---
    // TODO: Is this really the correct place for these?
    /// Channel for sending outgoing PUBACK, PUBREC, PUBREL and PUBCOMP requests
    ack_tx: Sender<AcknowledgementRequest<S>>,
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
        PublishOtherProperties<S>,
    ),
    PublishQoS1(
        PublishQoS1CompletionNotifier<S>,
        Topic<ByteStr<S>>,
        S,
        PublishOtherProperties<S>,
        PacketIdentifier,
    ),
    PublishQoS2(
        PublishQoS2CompletionNotifier<S>,
        Topic<ByteStr<S>>,
        S,
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

        // TODO: Add support for ordered acking ready notification here

        if let Poll::Ready(Some(ack_req)) = ch.ack_rx.poll_recv(cx) {
            return Poll::Ready(OutgoingPacketRequest::AcknowledgementRequest(ack_req));
        }

        // TODO: Ideally, no polling for reauth if one is already in progress
        if let Poll::Ready(Some(auth_req)) = ch.auth_rx.poll_recv(cx) {
            return Poll::Ready(OutgoingPacketRequest::ReauthRequest(auth_req));
        }

        if let Poll::Ready(Some(_)) = Pin::new(&mut ch.sub_rx).peek().poll_unpin(cx)
            && let Some(pkid) = pkid_pool.lease_next_pkid()
        {
            let Poll::Ready(Some(sub_req)) = ch.sub_rx.poll_next_unpin(cx) else {
                unreachable!("peek() confirmed the stream has an element");
            };
            return Poll::Ready(OutgoingPacketRequest::SubscriptionRequest(sub_req, pkid));
        }

        if let Poll::Ready(Some(publish)) = Pin::new(&mut ch.o_pub_rx).peek().poll_unpin(cx) {
            if matches!(publish, PublishRequest::PublishQoS0(..)) {
                let Poll::Ready(Some(PublishRequest::PublishQoS0(
                    notifier,
                    topic,
                    payload,
                    properties,
                ))) = ch.o_pub_rx.poll_next_unpin(cx)
                else {
                    unreachable!("peek() confirmed the stream has an element");
                };
                return Poll::Ready(OutgoingPacketRequest::PublishRequest(
                    PublishRequestWithPkid::PublishQoS0(notifier, topic, payload, properties),
                ));
            } else if let Some(pkid) = pkid_pool.lease_next_pkid() {
                let Poll::Ready(Some(publish)) = ch.o_pub_rx.poll_next_unpin(cx) else {
                    unreachable!("peek() confirmed the stream has an element");
                };
                return Poll::Ready(OutgoingPacketRequest::PublishRequest(match publish {
                    PublishRequest::PublishQoS0(..) => unreachable!("handled above"),
                    PublishRequest::PublishQoS1(notifier, topic, payload, properties) => {
                        PublishRequestWithPkid::PublishQoS1(
                            notifier, topic, payload, properties, pkid,
                        )
                    }
                    PublishRequest::PublishQoS2(notifier, topic, payload, properties) => {
                        PublishRequestWithPkid::PublishQoS2(
                            notifier, topic, payload, properties, pkid,
                        )
                    }
                }));
            }
        }

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
    /// All inflight QoS 1 PUBLISH operations
    publish_qos1: IndexMap<PacketIdentifier, (Publish<S>, PublishQoS1CompletionNotifier<S>)>,
    /// All inflight QoS 2 PUBLISH operations
    publish_qos2: IndexMap<PacketIdentifier, (Publish<S>, PublishQoS2CompletionNotifier<S>)>,
    /// All inflight SUBSCRIBE operations
    subscribe: HashMap<PacketIdentifier, SubscribeCompletionNotifier<S>>,
    /// All inflight UNSUBSCRIBE operations
    unsubscribe: HashMap<PacketIdentifier, UnsubscribeCompletionNotifier<S>>,

    // --- Acknowledgement tracking ---
    // None of these hashmaps should ever use the same key at the same time, although this is not
    // enforced for simplicity.
    /// All inflight PUBREC operations
    pubrec: HashMap<PacketIdentifier, (PubRec<S>, PubRecAcceptCompletionNotifier<S>)>,
    /// All inflight PUBREL operations
    pubrel: IndexMap<PacketIdentifier, (PubRel<S>, PubRelCompletionNotifier<S>)>,

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
    publishes: IndexMap<PacketIdentifier, PendingAcknowledgement<S>>,
}

enum PendingAcknowledgement<S>
where
    S: Shared,
{
    NotReady,
    Ready(AcknowledgementRequest<S>),
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
