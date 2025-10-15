// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{Duration, Sleep};

use crate::buffer_pool::Shared;
use crate::client::{
    channel_data::{
        AcknowledgementRequest, DisconnectRequest, IncomingPublish, PublishRequest,
        SubscriptionRequest,
    },
    session::pkid::PkidPool,
};
use crate::mqtt_proto::{
    ConnAck, ConnectReasonCode, Disconnect, Packet, PacketIdentifier, PacketIdentifierDupQoS,
    PingReq, PubAck, PubComp, PubRec, PubRel, Publish, SessionExpiryInterval, SubAck, Subscribe,
    UnsubAck, Unsubscribe,
};
use crate::token::{
    PubRecCompletionNotifier, PubRelCompletionNotifier, PublishQoS1CompletionNotifier,
    PublishQoS2CompletionNotifier, SubscribeCompletionNotifier, UnsubscribeCompletionNotifier,
};

mod pkid;

/// Tracks data related to the MQTT session state
pub(crate) struct Session<S: Shared> {
    /// Struct containing channels in and out of the Session
    pub(crate) ch: Channels,
    /// Pool of packet identifiers for outgoing packets that can be leased
    pkid_pool: PkidPool,
    /// Queue of outgoing operations that are not yet in-flight
    /// *Technically* not part of an MQTT Session, but it makes sense to keep it here.
    outgoing_queue: VecDeque<OutgoingOperation<S>>,
    /// Tracker of all inflight MQTT packets awaiting a response
    inflight: InflightTracker<S>,
    //ack_order: AckOrderer, // TODO
    connected: bool,
    transient: bool,
    pingreq: Option<PingReqTimer>,
}

impl<S: Shared> Session<S> {
    pub fn new(
        sub_rx: Receiver<SubscriptionRequest>,
        o_pub_rx: Receiver<PublishRequest>,
        ack_rx: Receiver<AcknowledgementRequest>,
        i_pub_tx: Sender<IncomingPublish>, // TODO: correct type
        max_pkid: PacketIdentifier,
    ) -> Self {
        let ch = Channels {
            disconnect_rx: None,
            o_pub_rx,
            sub_rx,
            ack_rx,
            i_pub_tx,
        };
        Self {
            ch,
            pkid_pool: PkidPool::new(max_pkid),
            outgoing_queue: Default::default(),
            inflight: InflightTracker::default(),
            connected: false,
            transient: false,
            pingreq: None,
        }
    }

    /// Returns the next outgoing MQTT packet to be sent over the network
    #[allow(clippy::unused_self)]
    pub async fn next_outgoing_packet(&mut self) -> Option<Packet<S>> {
        // TODO: Now that sending CONNECT is handled outside of `Session::next_outgoing_packet`,
        // it will only ever be called after `complete_inflight(ConnAck)` has been called, right?
        assert!(self.connected);

        #[allow(unreachable_code)] // TODO: Remove when todo!()s are resolved
        let packet = match poll_connected_channels(&mut self.ch, self.pingreq.as_mut()).await {
            ConnectedChannelsOutgoingPacket::DisconnectRequest(disconnect_req) => {
                Packet::Disconnect(Disconnect {
                    reason_code: todo!(),
                    other_properties: todo!(),
                })
            }

            ConnectedChannelsOutgoingPacket::AcknowledgementRequest(ack_req) => match ack_req {
                AcknowledgementRequest::PubAck(..) => Packet::PubAck(PubAck {
                    packet_identifier: todo!(),
                    reason_code: todo!(),
                    other_properties: todo!(),
                }),
                AcknowledgementRequest::PubComp(..) => Packet::PubComp(PubComp {
                    packet_identifier: todo!(),
                    reason_code: todo!(),
                    other_properties: todo!(),
                }),

                AcknowledgementRequest::PubRec(..) => Packet::PubRec(PubRec {
                    packet_identifier: todo!(),
                    reason_code: todo!(),
                    other_properties: todo!(),
                }),

                AcknowledgementRequest::PubRel(..) => Packet::PubRel(PubRel {
                    packet_identifier: todo!(),
                    reason_code: todo!(),
                    other_properties: todo!(),
                }),
            },

            ConnectedChannelsOutgoingPacket::SubscriptionRequest(sub_req) => {
                let packet_identifier = todo!();
                match sub_req {
                    SubscriptionRequest::Subscribe(notifier, ..) => {
                        self.inflight.subscribe.insert(packet_identifier, notifier);
                        Packet::Subscribe(Subscribe {
                            packet_identifier,
                            subscribe_to: todo!(),
                            other_properties: todo!(),
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
                }
            }

            ConnectedChannelsOutgoingPacket::PublishRequest(publish) => {
                let packet = match publish {
                    PublishRequest::PublishQoS0(..) => Publish {
                        topic_name: todo!(),
                        packet_identifier_dup_qos: PacketIdentifierDupQoS::AtMostOnce,
                        retain: todo!(),
                        payload: todo!(),
                        other_properties: todo!(),
                    },

                    PublishRequest::PublishQoS1(..) => {
                        // TODO: Push to outgoing messages queue if packet ID can't be assigned.
                        let packet_identifier = todo!();
                        Publish {
                            topic_name: todo!(),
                            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                                packet_identifier,
                                todo!(),
                            ),
                            retain: todo!(),
                            payload: todo!(),
                            other_properties: todo!(),
                        }
                    }

                    PublishRequest::PublishQoS2(..) => {
                        // TODO: Push to outgoing messages queue if packet ID can't be assigned.
                        let packet_identifier = todo!();
                        Publish {
                            topic_name: todo!(),
                            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(
                                packet_identifier,
                                todo!(),
                            ),
                            retain: todo!(),
                            payload: todo!(),
                            other_properties: todo!(),
                        }
                    }
                };
                Packet::Publish(packet)
            }

            ConnectedChannelsOutgoingPacket::PingReq => Packet::PingReq(PingReq),
        };
        Some(packet)
    }

    /// Complete an in-flight operation with a received acknowledgement.
    /// Adjusts state as appropriate.
    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    pub fn complete_inflight(&mut self, operation: CompletedOperation<S>) {
        match operation {
            CompletedOperation::Connect(connack) => {
                if let ConnectReasonCode::Success { session_present } = connack.reason_code {
                    self.connected = true;

                    if !session_present {
                        // Previous session, if any, is not present on the server.
                        self.session_expired();
                    }

                    if matches!(
                        connack.other_properties.session_expiry_interval,
                        Some(SessionExpiryInterval::Duration(0))
                    ) && !self.transient
                    {
                        // We asked for a persistent session but the server overrode it to transient.
                        self.transient = true;
                    }

                    // TODO: Get PINGREQ duration from connect properties
                    self.pingreq = Some(PingReqTimer::new(Duration::from_secs(5)));
                }
            }
            CompletedOperation::Subscribe(suback) => {
                // TODO: Convert suback to user-facing type and complete notifier
            }
            CompletedOperation::Unsubscribe(unsuback) => {
                // TODO: Convert unsuback to user-facing type and complete notifier
            }
            CompletedOperation::PublishQoS1(puback) => {
                // TODO: Convert puback to user-facing type and complete notifier
            }
            CompletedOperation::PublishQoS2(pubrec) => {
                // TODO: Convert pubrec to user-facing type, create pubrel infrastructure
                // and complete notifier
            }
        }
    }

    /// Trigger a disconnect and adjust state based on the information in the outgoing `Disconnect` packet
    pub fn client_disconnect(&mut self, disconnect: &Disconnect<S>) {
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
    pub fn server_disconnect(&mut self, disconnect: &Disconnect<S>) {
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

    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    #[allow(clippy::unused_self)]
    pub fn incoming_publish(&mut self, publish: Publish<S>) {
        unimplemented!()
    }

    /// The connection has been closed for any reason.
    fn disconnected(&mut self) {
        // NOTE: When we cancel CompletionNotifiers here, we don't care about the Result because
        // if it fails, that just means the user no longer has the corresponding CompletionToken

        self.connected = false;
        self.pingreq = None;
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
    }

    /// Perform state changes when the session is known to be expired on the server:
    ///
    /// 1. The connection closed, and it had originally been established with session expiry interval == 0
    /// 2. The client closed the connect via a DISCONNECT with session expiry interval == 0
    /// 3. A new connection was established and the CONNACK says session present == false
    fn session_expired(&mut self) {
        // Remove and cancel all in-flight QoS 1 PUBLISHes
        for (pkid, (_, notifier)) in self.inflight.publish_qos1.drain() {
            let _ = notifier.cancel();
            self.pkid_pool.release_pkid(pkid);
        }
        // Remove and cancel all in-flight QoS 2 PUBLISHes
        for (pkid, (_, notifier)) in self.inflight.publish_qos2.drain() {
            let _ = notifier.cancel();
            self.pkid_pool.release_pkid(pkid);
        }

        // TODO: PUBREL, PUBREC, PUBCOMP
    }
}

/// A desired operation initiated by the client
enum OutgoingOperation<S>
where
    S: Shared,
{
    Subscribe(PacketIdentifier, SubscribeCompletionNotifier),
    Unsubscribe(PacketIdentifier, UnsubscribeCompletionNotifier),
    PublishQoS1(Publish<S>, PublishQoS1CompletionNotifier),
}

/// A response to an operation initiated by the client
pub enum CompletedOperation<S>
where
    S: Shared,
{
    Connect(ConnAck<S>),
    PublishQoS1(PubAck<S>),
    PublishQoS2(PubRec<S>),
    Subscribe(SubAck<S>),
    Unsubscribe(UnsubAck<S>),
    // TODO: pubrec, pubrel
}

/// Organizational struct containing channels on which the `Session` receives input
pub(crate) struct Channels {
    /// Channel for receiving outgoing CONNECT and DISCONNECT requests
    pub(crate) disconnect_rx: Option<tokio::sync::oneshot::Receiver<DisconnectRequest>>,
    /// Channel for receiving outgoing PUBLISH requests
    o_pub_rx: Receiver<PublishRequest>,
    /// Channel for receiving outgoing SUBSCRIBE and UNSUBSCRIBE requests
    sub_rx: Receiver<SubscriptionRequest>,
    /// Channel for receving outgoing PUBACK, PUBREC, PUBREL and PUBCOMP requests
    ack_rx: Receiver<AcknowledgementRequest>,
    /// Channel for sending incoming PUBLISH requests
    i_pub_tx: Sender<IncomingPublish>,
}

enum ConnectedChannelsOutgoingPacket {
    DisconnectRequest(DisconnectRequest),
    AcknowledgementRequest(AcknowledgementRequest),
    SubscriptionRequest(SubscriptionRequest),
    PublishRequest(PublishRequest),
    PingReq,
}

// Poll for outgoing ACKs, then for outgoing SUBSCRIBEs, then for outgoing PUBLISHes.
// If any of them yields an item, reset the PINGREQ timer, else poll for outgoing PINGREQs.
fn poll_connected_channels(
    ch: &mut Channels,
    mut pingreq: Option<&mut PingReqTimer>,
) -> impl Future<Output = ConnectedChannelsOutgoingPacket> {
    futures_util::future::poll_fn(move |cx| {
        if let Some(disconnect_rx) = &mut ch.disconnect_rx
            && let Poll::Ready(disconnect_req) = Pin::new(disconnect_rx).poll(cx)
        {
            drop(ch.disconnect_rx.take());
            if let Ok(disconnect_req) = disconnect_req {
                return Poll::Ready(ConnectedChannelsOutgoingPacket::DisconnectRequest(
                    disconnect_req,
                ));
            }
            // ... else: User dropped the disconnect_tx, so there's nothing more to do.
        }

        if let Poll::Ready(Some(ack_req)) = ch.ack_rx.poll_recv(cx) {
            if let Some(ref mut pingreq) = pingreq {
                pingreq.reset();
            }
            return Poll::Ready(ConnectedChannelsOutgoingPacket::AcknowledgementRequest(
                ack_req,
            ));
        }

        if let Poll::Ready(Some(sub_req)) = ch.sub_rx.poll_recv(cx) {
            if let Some(ref mut pingreq) = pingreq {
                pingreq.reset();
            }
            return Poll::Ready(ConnectedChannelsOutgoingPacket::SubscriptionRequest(
                sub_req,
            ));
        }

        if let Poll::Ready(Some(publish)) = ch.o_pub_rx.poll_recv(cx) {
            if let Some(ref mut pingreq) = pingreq {
                pingreq.reset();
            }
            return Poll::Ready(ConnectedChannelsOutgoingPacket::PublishRequest(publish));
        }

        if let Some(ref mut pingreq) = pingreq
            && let Poll::Ready(()) = Pin::new(&mut *pingreq).poll(cx)
        {
            pingreq.reset();
            return Poll::Ready(ConnectedChannelsOutgoingPacket::PingReq);
        }

        Poll::Pending
    })
}

/// Contains data related to in-flight operations pending a response
struct InflightTracker<S>
where
    S: Shared,
{
    // --- Operation tracking ---
    // None of these hashmaps should ever use the same key at the same time, although this is not
    // enforced for simplicity.
    /// All inflight QoS 1 PUBLISH operations
    publish_qos1: HashMap<PacketIdentifier, (Publish<S>, PublishQoS1CompletionNotifier)>,
    /// All inflight QoS 2 PUBLISH operations
    publish_qos2: HashMap<PacketIdentifier, (Publish<S>, PublishQoS2CompletionNotifier)>,
    /// All inflight SUBSCRIBE operations
    subscribe: HashMap<PacketIdentifier, SubscribeCompletionNotifier>,
    /// All inflight UNSUBSCRIBE operations
    unsubscribe: HashMap<PacketIdentifier, UnsubscribeCompletionNotifier>,

    // --- Acknowledgement tracking ---
    // None of these hashmaps should ever use the same key at the same time, although this is not
    // enforced for simplicity.
    /// All inflight PUBREC operations
    pubrec: HashMap<PacketIdentifier, (PubRec<S>, PubRecCompletionNotifier)>,
    /// All inflight PUBREL operations
    pubrel: HashMap<PacketIdentifier, (PubRel<S>, PubRelCompletionNotifier)>,
}

impl<S> Default for InflightTracker<S>
where
    S: Shared,
{
    fn default() -> Self {
        Self {
            publish_qos1: HashMap::new(),
            publish_qos2: HashMap::new(),
            subscribe: HashMap::new(),
            unsubscribe: HashMap::new(),
            pubrec: HashMap::new(),
            pubrel: HashMap::new(),
        }
    }
}

struct PingReqTimer {
    inner: Pin<Box<Sleep>>,
    duration: Duration,
}

impl PingReqTimer {
    fn new(duration: Duration) -> Self {
        Self {
            inner: Box::pin(tokio::time::sleep(duration)),
            duration,
        }
    }

    fn reset(&mut self) {
        self.inner
            .as_mut()
            .reset(tokio::time::Instant::now() + self.duration);
    }
}

impl Future for PingReqTimer {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}
