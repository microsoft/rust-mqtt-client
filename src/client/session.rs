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
        AcknowledgementRequest, ConnectionRequest, IncomingPublish, PublishRequest,
        SubscriptionRequest,
    },
    session::pkid::PkidPool,
};
use crate::mqtt_proto::{
    ConnAck, Connect, ConnectSessionExpiryInterval, Disconnect, KeepAlive, Packet,
    PacketIdentifier, PacketIdentifierDupQoS, PingReq, PubAck, PubComp, PubRec, PubRel, Publish,
    SessionExpiryInterval, SubAck, Subscribe, UnsubAck, Unsubscribe,
};
use crate::token::{
    ConnectCompletionNotifier, PubRecCompletionNotifier, PubRelCompletionNotifier,
    PublishQoS1CompletionNotifier, PublishQoS2CompletionNotifier, SubscribeCompletionNotifier,
    UnsubscribeCompletionNotifier,
};

mod pkid;

/// Tracks data related to the MQTT session state
pub struct Session<S: Shared> {
    /// Struct containing channels in and out of the Session
    ch: Channels,
    /// Pool of packet identifiers for outgoing packets that can be leased
    pkid_pool: PkidPool,
    /// Queue of outgoing operations that are not yet in-flight
    /// *Technically* not part of an MQTT Session, but it makes sense to keep it here.
    outgoing_queue: VecDeque<OutgoingOperation<S>>,
    /// Tracker of all inflight MQTT packets awaiting a response
    inflight: InflightTracker<S>,
    //ack_order: AckOrderer, // TODO
    connected: bool,
}

pub(crate) enum ConnectionTransportConfig {
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

impl<S: Shared> Session<S> {
    pub fn new(
        conn_rx: Receiver<ConnectionRequest>,
        sub_rx: Receiver<SubscriptionRequest>,
        o_pub_rx: Receiver<PublishRequest>,
        ack_rx: Receiver<AcknowledgementRequest>,
        i_pub_tx: Sender<IncomingPublish>, // TODO: correct type
        max_pkid: PacketIdentifier,
    ) -> Self {
        let ch = Channels {
            conn_rx,
            o_pub_rx,
            sub_rx,
            ack_rx,
            i_pub_tx,
            pingreq: None,
        };
        Self {
            ch,
            pkid_pool: PkidPool::new(max_pkid),
            outgoing_queue: Default::default(),
            inflight: InflightTracker::default(),
            connected: false,
        }
    }

    /// Returns parameters for establishing a new connection.
    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    #[allow(clippy::unused_self)]
    pub async fn connection_transport_config(&mut self) -> ConnectionTransportConfig {
        unimplemented!()
    }

    /// Returns the next outgoing MQTT packet to be sent over the network
    #[allow(clippy::unused_self)]
    pub async fn next_outgoing_packet(&mut self) -> Option<Packet<S>> {
        match (self.connected, &mut self.inflight.connect) {
            // If we're currently disconnected, then only poll `conn_rx` and generate a `Connect`
            (false, inflight_connect @ None) => {
                let (notifier, _properties) = loop {
                    match self.ch.conn_rx.recv().await? {
                        ConnectionRequest::Connect(notifier, properties) => {
                            break (notifier, properties);
                        }
                        // TODO: Just ignore it? Or return an error?
                        // Or split conn_rx into separate channels for Connect and Disconnect requests?
                        ConnectionRequest::Disconnect(..) => (),
                    }
                };
                *inflight_connect = Some(notifier);
                // TODO: Get values from properties
                Some(Packet::Connect(Connect {
                    username: None,
                    password: None,
                    will: None,
                    client_id: None,
                    clean_start: true,
                    keep_alive: KeepAlive::Infinite,
                    session_expiry_interval: ConnectSessionExpiryInterval(
                        SessionExpiryInterval::Infinite,
                    ),
                    other_properties: Default::default(),
                }))
            }

            // If we're currently disconnected and waiting for CONNACK, then yield `Pending`
            (false, Some(_)) => std::future::pending().await,

            // If we're currently connected, then poll self.ch
            (true, _) => {
                #[allow(unreachable_code)] // TODO: Remove when todo!()s are resolved
                let packet = match poll_connected_channels(&mut self.ch).await {
                    ConnectedChannelsOutgoingPacket::AcknowledgementRequest(ack_req) => {
                        match ack_req {
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
                        }
                    }

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
        }
    }

    /// Complete an in-flight operation with a received acknowledgement.
    /// Adjusts state as appropriate.
    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    pub fn complete_inflight(&mut self, operation: CompletedOperation<S>) {
        match operation {
            CompletedOperation::Connect(connack) => {
                if let Some(notifier) = self.inflight.connect.take() {
                    #[allow(clippy::collapsible_if)] // TODO: remove
                    if connack.is_success() {
                        self.connected = true;
                        // TODO: Get PINGREQ duration from connect properties
                        self.ch.pingreq = Some(PingReqTimer::new(Duration::from_secs(5)));
                    }
                    // TODO: Convert connack to user-facing type and complete notifier
                    //let connack = connack.into()
                    //notifier.complete(connack);
                } else {
                    todo!("treat as protocol error: unexpected CONNACK");
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
        // NOTE: When we cancel CompletionNotifiers here, we don't care about the Result because
        // if it fails, that just means the user no longer has the corresponding CompletionToken

        // Set connection state
        self.connected = false;
        self.ch.pingreq = None;
        // Remove and cancel any in-flight CONNECT
        if let Some(notifier) = self.inflight.connect.take() {
            let _ = notifier.cancel();
        }
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

        // If session is ended, additional state changes must be taken
        if let Some(SessionExpiryInterval::Duration(0)) =
            disconnect.other_properties.session_expiry_interval
        {
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

    /// Trigger a disconnect and adjust state based on the information in the incoming `Disconnect` packet
    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    #[allow(clippy::unused_self)]
    pub fn server_disconnect(&mut self, disconnect: Disconnect<S>) {
        // NOTE: When we cancel CompletionNotifiers here, we don't care about the Result because
        // if it fails, that just means the user no longer has the corresponding CompletionToken

        // Set connection state
        self.connected = false;
        self.ch.pingreq = None;
        // Remove and cancel any in-flight CONNECT
        // This shouldn't happen, since DISCONNECT requires an existing connection to be issued.
        if let Some(notifier) = self.inflight.connect.take() {
            let _ = notifier.cancel();
            log::warn!("Received DISCONNECT while CONNECT packet in-flight");
        }
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

        // If session is ended, additional state changes must be taken
        if let Some(SessionExpiryInterval::Duration(0)) =
            disconnect.other_properties.session_expiry_interval
        {
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

    /// Trigger a disconnect and adjust state based on the error from the underlying transport
    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    #[allow(clippy::unused_self)]
    pub fn transport_disconnect(&mut self, err: std::io::Error) {
        unimplemented!()
    }

    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    #[allow(clippy::unused_self)]
    pub fn incoming_publish(&mut self, publish: Publish<S>) {
        unimplemented!()
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
struct Channels {
    /// Channel for receiving outgoing CONNECT and DISCONNECT requests
    conn_rx: Receiver<ConnectionRequest>,
    /// Channel for receiving outgoing PUBLISH requests
    o_pub_rx: Receiver<PublishRequest>,
    /// Channel for receiving outgoing SUBSCRIBE and UNSUBSCRIBE requests
    sub_rx: Receiver<SubscriptionRequest>,
    /// Channel for receving outgoing PUBACK, PUBREC, PUBREL and PUBCOMP requests
    ack_rx: Receiver<AcknowledgementRequest>,
    /// Channel for sending incoming PUBLISH requests
    i_pub_tx: Sender<IncomingPublish>,
    pingreq: Option<PingReqTimer>,
}

enum ConnectedChannelsOutgoingPacket {
    AcknowledgementRequest(AcknowledgementRequest),
    SubscriptionRequest(SubscriptionRequest),
    PublishRequest(PublishRequest),
    PingReq,
}

fn poll_connected_channels(
    ch: &mut Channels,
) -> impl Future<Output = ConnectedChannelsOutgoingPacket> {
    futures_util::future::poll_fn(|cx| {
        // Poll for outgoing ACKs, then for outgoing SUBSCRIBEs, then for outgoing PUBLISHes.
        // If any of them yields an item, reset the PINGREQ timer, else poll for outgoing PINGREQs.

        if let Poll::Ready(Some(ack_req)) = ch.ack_rx.poll_recv(cx) {
            if let Some(pingreq) = &mut ch.pingreq {
                pingreq.reset();
            }
            return Poll::Ready(ConnectedChannelsOutgoingPacket::AcknowledgementRequest(
                ack_req,
            ));
        }

        if let Poll::Ready(Some(sub_req)) = ch.sub_rx.poll_recv(cx) {
            if let Some(pingreq) = &mut ch.pingreq {
                pingreq.reset();
            }
            return Poll::Ready(ConnectedChannelsOutgoingPacket::SubscriptionRequest(
                sub_req,
            ));
        }

        if let Poll::Ready(Some(publish)) = ch.o_pub_rx.poll_recv(cx) {
            if let Some(pingreq) = &mut ch.pingreq {
                pingreq.reset();
            }
            return Poll::Ready(ConnectedChannelsOutgoingPacket::PublishRequest(publish));
        }

        if let Some(pingreq) = &mut ch.pingreq
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
    /// Inflight CONNECT operation
    connect: Option<ConnectCompletionNotifier>,

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
            connect: None,
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
