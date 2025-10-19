// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender};
use tokio::time::{Duration, Sleep};

use crate::buffer_pool::{Owned, Shared};
use crate::client::{
    AckHandle,
    channel_data::{
        AcknowledgementRequest, DisconnectRequest, IncomingPublish, PublishRequest,
        SubscriptionRequest,
    },
    session::pkid::PkidPool,
    token::{
        PubAckToken, PubRecCompletionNotifier, PubRelCompletionNotifier,
        PublishQoS1CompletionNotifier, PublishQoS2CompletionNotifier, SubscribeCompletionNotifier,
        UnsubscribeCompletionNotifier,
    },
};
use crate::mqtt_proto::{
    ConnAck, ConnectReasonCode, Disconnect, Packet, PacketIdentifier, PacketIdentifierDupQoS,
    PingReq, PubAck, PubComp, PubRec, PubRel, Publish, RetainHandling, SessionExpiryInterval,
    SubAck, Subscribe, SubscribeOptions, SubscribeOptionsOtherProperties, SubscribeTo, UnsubAck,
    Unsubscribe,
};
use crate::packet::IntoBuffered;

mod pkid;

/// Tracks data related to the MQTT session state
pub(crate) struct Session<O>
where
    O: Owned,
{
    /// Struct containing channels in and out of the Session
    pub(crate) ch: Channels,
    /// Pool of packet identifiers for outgoing packets that can be leased
    pkid_pool: PkidPool,
    /// Queue of outgoing operations that are not yet in-flight
    /// *Technically* not part of an MQTT Session, but it makes sense to keep it here.
    outgoing_queue: VecDeque<OutgoingOperation<O::Shared>>,
    /// Tracker of all inflight MQTT packets awaiting a response
    inflight: InflightTracker<O::Shared>,
    /// Whether the session is currently connected
    connected: bool,
    /// Identifier for the current connection epoch
    connection_epoch: u64,
    transient: bool,
    pingreq: Option<PingReqTimer>,
    owned: O,
}

impl<O> Session<O>
where
    O: Owned,
{
    pub fn new(
        sub_rx: Receiver<SubscriptionRequest>,
        o_pub_rx: Receiver<PublishRequest>,
        ack_rx: Receiver<AcknowledgementRequest>,
        i_pub_tx: UnboundedSender<IncomingPublish>,
        ack_tx: Sender<AcknowledgementRequest>,
        max_pkid: PacketIdentifier,
        owned: O,
    ) -> Self {
        let ch = Channels {
            disconnect_rx: None,
            o_pub_rx,
            sub_rx,
            ack_rx,
            i_pub_tx,
            ack_tx,
        };
        Self {
            ch,
            pkid_pool: PkidPool::new(max_pkid),
            outgoing_queue: Default::default(),
            inflight: InflightTracker::default(),
            connected: false,
            connection_epoch: 0,
            transient: false,
            pingreq: None,
            owned,
        }
    }

    // Need to retrieve packets from some combination of:
    // - Connected channels
    // - Ping timer
    // - Ack orderer

    // Should ConnectedChannelsOutgoingPacket be renamed to OutgoingPacket ?
    // Oh, it should be NEXT REQUEST type shouldn't it?

    // Moving the ping reset is tricky - you ideally trigger it after the write
    // This is kind of the same as QoS0 publish completion notify.
    //    This also applies to QoS1 PUBACK / QoS2 PUBCOMP / QoS2 PUBREC (rejected only)
    // We also will likely need notifiers sent to the client to fail the op if write fails,
    // thus, next_outgoing_packet() should probably also return a notifier?
    //
    // OR there should be a way to call back in somehow and trigger the notifier internally to the session
    // - this might be preferable because in no other case does the notifier escape the session...
    // - would it really be so bad though? After all, the failure is coming from the write stack...
    // - bigger issue is that in sub/unsub/qos1pub/qos2pub we want to keep inflight unless failure
    //
    // How about a "current operation" field that is set when next_outgoing_packet is called,
    // that can be called back into to fail (or complete) when necessary?
    // 
    // Perhaps also the completion notifier could be clonable - but that would still require bespoke
    // cleanup logic for failure condition.
    //
    // Techincally this all applies to inflight tracked ops too - inflight op needs to be cleaned up
    // if write fails. Or added after, but then we might risk a race condition...
    // Actually, not a race condition if we make sure we do it BEFORE reading a new packet!!!!
    // So return some kind of "indicate completion" struct?

    // Is a write failure fatal?

    // My best idea is probably to just set it as "pending" when calling next_outgoing_packet, and then
    // also have to call "complete_pending_write" or something
    // Or, perhaps don't set pending at all, and merely pass the whole thing - nothing gets tracked until
    // a call to "report_write_success" or "report_write_failure" - maybe report_packet_write is sufficient (failure may not be needed)

    // PUBACK in order of PUBLISH
    // PUBREC in order of PUBLISH
    // PUBREL in order of PUBREC
    // PUBCOMP in any order, it doesn't matter

    /// Returns the next outgoing MQTT packet to be sent over the network
    #[allow(clippy::unused_self)]
    pub async fn next_outgoing_packet(&mut self) -> Option<Packet<O::Shared>> {
        // TODO: Now that sending CONNECT is handled outside of `Session::next_outgoing_packet`,
        // it will only ever be called after `complete_inflight(ConnAck)` has been called, right?
        assert!(self.connected);

        #[allow(unreachable_code)] // TODO: Remove when todo!()s are resolved
        let packet = match self.next_outgoing_request().await {
            OutgoingPacketRequest::DisconnectRequest(disconnect_req) => {
                Packet::Disconnect(Disconnect {
                    reason_code: todo!(),
                    other_properties: todo!(),
                })
            }

            OutgoingPacketRequest::AcknowledgementRequest(ack_req) => match ack_req {
                // TODO: Reject PUBACK if epoch does not match current connection epoch
                // TODO: It would be preferable if the notifier was not triggered on
                // PUBACK / PUBCOMP until they were actually sent over the network.
                AcknowledgementRequest::PubAck(notifier, puback, epoch) => {
                    let puback = Packet::PubAck(PubAck {
                        packet_identifier: puback.packet_identifier,
                        reason_code: puback.reason.into(),
                        other_properties: puback
                            .properties
                            .into_buffered(&mut self.owned)
                            .expect("TODO: error handling"),
                    });
                    // Do not care about result - if the token was dropped, the user is no longer waiting for it.
                    let _ = notifier.complete(());
                    puback
                }
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

            OutgoingPacketRequest::SubscriptionRequest(sub_req) => {
                // TODO: Make this async instead of failing so that we can wake up when a packet ID becomes available.
                let packet_identifier = self.pkid_pool.lease_next_pkid().unwrap();
                match sub_req {
                    SubscriptionRequest::Subscribe(
                        notifier,
                        topic_filter,
                        qos,
                        subscribe_properties,
                    ) => {
                        self.inflight.subscribe.insert(packet_identifier, notifier);
                        Packet::Subscribe(Subscribe {
                            packet_identifier,
                            subscribe_to: vec![SubscribeTo {
                                topic_filter: topic_filter
                                    .into_inner()
                                    .to_shared(&mut self.owned)
                                    .expect("TODO: error handling"),
                                options: SubscribeOptions {
                                    maximum_qos: qos.into(),
                                    // TODO: Get from subscribe_properties
                                    other_properties: SubscribeOptionsOtherProperties {
                                        no_local: false,
                                        retain_as_published: false,
                                        retain_handling: RetainHandling::Send,
                                    },
                                },
                            }],
                            other_properties: subscribe_properties
                                .into_buffered(&mut self.owned)
                                .expect("TODO: error handling"),
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

            OutgoingPacketRequest::PublishRequest(publish) => {
                let packet = match publish {
                    PublishRequest::PublishQoS0(notifier, topic_name, payload, properties) => {
                        Publish {
                            topic_name: topic_name
                                .into_inner()
                                .to_shared(&mut self.owned)
                                .expect("TODO: error handling"),
                            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtMostOnce,
                            retain: false, // TODO: Get from properties
                            payload: payload
                                .into_buffered(&mut self.owned)
                                .expect("TODO: error handling"),
                            other_properties: properties
                                .into_buffered(&mut self.owned)
                                .expect("TODO: error handling"),
                        }
                        // TODO: Trigger notifier somehow
                    }

                    PublishRequest::PublishQoS1(notifier, topic_name, payload, properties) => {
                        // TODO: Push to outgoing messages queue if packet ID can't be assigned.

                        // TODO: Make this async instead of failing so that we can wake up when a packet ID becomes available.
                        let packet_identifier = self.pkid_pool.lease_next_pkid().unwrap();
                        let publish = Publish {
                            topic_name: topic_name
                                .into_inner()
                                .to_shared(&mut self.owned)
                                .expect("TODO: error handling"),
                            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                                packet_identifier,
                                false, // TODO: Get from properties
                            ),
                            retain: false, // TODO: Get from properties
                            payload: payload
                                .into_buffered(&mut self.owned)
                                .expect("TODO: error handling"),
                            other_properties: properties
                                .into_buffered(&mut self.owned)
                                .expect("TODO: error handling"),
                        };
                        self.inflight
                            .publish_qos1
                            .insert(packet_identifier, (publish.clone(), notifier));
                        publish
                    }

                    PublishRequest::PublishQoS2(notifier, topic_name, payload, properties) => {
                        // TODO: Push to outgoing messages queue if packet ID can't be assigned.

                        // TODO: Make this async instead of failing so that we can wake up when a packet ID becomes available.
                        let packet_identifier = self.pkid_pool.lease_next_pkid().unwrap();
                        let publish = Publish {
                            topic_name: topic_name
                                .into_inner()
                                .to_shared(&mut self.owned)
                                .expect("TODO: error handling"),
                            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(
                                packet_identifier,
                                false, // TODO: Get from properties
                            ),
                            retain: false, // TODO: Get from properties
                            payload: payload
                                .into_buffered(&mut self.owned)
                                .expect("TODO: error handling"),
                            other_properties: properties
                                .into_buffered(&mut self.owned)
                                .expect("TODO: error handling"),
                        };
                        self.inflight
                            .publish_qos2
                            .insert(packet_identifier, (publish.clone(), notifier));
                        publish
                    }
                };
                Packet::Publish(packet)
            }

            OutgoingPacketRequest::PingReq => Packet::PingReq(PingReq),
        };
        Some(packet)
    }

    async fn next_outgoing_request(&mut self) -> OutgoingPacketRequest {
        poll_for_outgoing_request(&mut self.ch, self.pingreq.as_mut()).await

        // TODO: validation of request based on connection configuration
        // TODO: ack ordering
    }


    // /// Returns the next outgoing MQTT packet to be sent over the network
    // #[allow(clippy::unused_self)]
    // pub async fn next_outgoing_packet(&mut self) -> Option<Packet<O::Shared>> {
    //     // TODO: Now that sending CONNECT is handled outside of `Session::next_outgoing_packet`,
    //     // it will only ever be called after `complete_inflight(ConnAck)` has been called, right?
    //     assert!(self.connected);

    //     #[allow(unreachable_code)] // TODO: Remove when todo!()s are resolved
    //     let packet = match poll_connected_channels(&mut self.ch, self.pingreq.as_mut()).await {
    //         ConnectedChannelsOutgoingPacket::DisconnectRequest(disconnect_req) => {
    //             Packet::Disconnect(Disconnect {
    //                 reason_code: todo!(),
    //                 other_properties: todo!(),
    //             })
    //         }

    //         ConnectedChannelsOutgoingPacket::AcknowledgementRequest(ack_req) => match ack_req {
    //             // TODO: Reject PUBACK if epoch does not match current connection epoch
    //             // TODO: It would be preferable if the notifier was not triggered on
    //             // PUBACK / PUBCOMP until they were actually sent over the network.
    //             AcknowledgementRequest::PubAck(notifier, puback, epoch) => {
    //                 let puback = Packet::PubAck(PubAck {
    //                     packet_identifier: puback.packet_identifier,
    //                     reason_code: puback.reason.into(),
    //                     other_properties: puback
    //                         .properties
    //                         .into_buffered(&mut self.owned)
    //                         .expect("TODO: error handling"),
    //                 });
    //                 // Do not care about result - if the token was dropped, the user is no longer waiting for it.
    //                 let _ = notifier.complete(());
    //                 puback
    //             }
    //             AcknowledgementRequest::PubComp(..) => Packet::PubComp(PubComp {
    //                 packet_identifier: todo!(),
    //                 reason_code: todo!(),
    //                 other_properties: todo!(),
    //             }),

    //             AcknowledgementRequest::PubRec(..) => Packet::PubRec(PubRec {
    //                 packet_identifier: todo!(),
    //                 reason_code: todo!(),
    //                 other_properties: todo!(),
    //             }),

    //             AcknowledgementRequest::PubRel(..) => Packet::PubRel(PubRel {
    //                 packet_identifier: todo!(),
    //                 reason_code: todo!(),
    //                 other_properties: todo!(),
    //             }),
    //         },

    //         ConnectedChannelsOutgoingPacket::SubscriptionRequest(sub_req) => {
    //             // TODO: Make this async instead of failing so that we can wake up when a packet ID becomes available.
    //             let packet_identifier = self.pkid_pool.lease_next_pkid().unwrap();
    //             match sub_req {
    //                 SubscriptionRequest::Subscribe(
    //                     notifier,
    //                     topic_filter,
    //                     qos,
    //                     subscribe_properties,
    //                 ) => {
    //                     self.inflight.subscribe.insert(packet_identifier, notifier);
    //                     Packet::Subscribe(Subscribe {
    //                         packet_identifier,
    //                         subscribe_to: vec![SubscribeTo {
    //                             topic_filter: topic_filter
    //                                 .into_inner()
    //                                 .to_shared(&mut self.owned)
    //                                 .expect("TODO: error handling"),
    //                             options: SubscribeOptions {
    //                                 maximum_qos: qos.into(),
    //                                 // TODO: Get from subscribe_properties
    //                                 other_properties: SubscribeOptionsOtherProperties {
    //                                     no_local: false,
    //                                     retain_as_published: false,
    //                                     retain_handling: RetainHandling::Send,
    //                                 },
    //                             },
    //                         }],
    //                         other_properties: subscribe_properties
    //                             .into_buffered(&mut self.owned)
    //                             .expect("TODO: error handling"),
    //                     })
    //                 }

    //                 SubscriptionRequest::Unsubscribe(notifier, ..) => {
    //                     self.inflight
    //                         .unsubscribe
    //                         .insert(packet_identifier, notifier);
    //                     Packet::Unsubscribe(Unsubscribe {
    //                         packet_identifier,
    //                         unsubscribe_from: todo!(),
    //                         other_properties: todo!(),
    //                     })
    //                 }
    //             }
    //         }

    //         ConnectedChannelsOutgoingPacket::PublishRequest(publish) => {
    //             let packet = match publish {
    //                 PublishRequest::PublishQoS0(notifier, topic_name, payload, properties) => {
    //                     Publish {
    //                         topic_name: topic_name
    //                             .into_inner()
    //                             .to_shared(&mut self.owned)
    //                             .expect("TODO: error handling"),
    //                         packet_identifier_dup_qos: PacketIdentifierDupQoS::AtMostOnce,
    //                         retain: false, // TODO: Get from properties
    //                         payload: payload
    //                             .into_buffered(&mut self.owned)
    //                             .expect("TODO: error handling"),
    //                         other_properties: properties
    //                             .into_buffered(&mut self.owned)
    //                             .expect("TODO: error handling"),
    //                     }
    //                 }

    //                 PublishRequest::PublishQoS1(notifier, topic_name, payload, properties) => {
    //                     // TODO: Push to outgoing messages queue if packet ID can't be assigned.

    //                     // TODO: Make this async instead of failing so that we can wake up when a packet ID becomes available.
    //                     let packet_identifier = self.pkid_pool.lease_next_pkid().unwrap();
    //                     let publish = Publish {
    //                         topic_name: topic_name
    //                             .into_inner()
    //                             .to_shared(&mut self.owned)
    //                             .expect("TODO: error handling"),
    //                         packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
    //                             packet_identifier,
    //                             false, // TODO: Get from properties
    //                         ),
    //                         retain: false, // TODO: Get from properties
    //                         payload: payload
    //                             .into_buffered(&mut self.owned)
    //                             .expect("TODO: error handling"),
    //                         other_properties: properties
    //                             .into_buffered(&mut self.owned)
    //                             .expect("TODO: error handling"),
    //                     };
    //                     self.inflight
    //                         .publish_qos1
    //                         .insert(packet_identifier, (publish.clone(), notifier));
    //                     publish
    //                 }

    //                 PublishRequest::PublishQoS2(notifier, topic_name, payload, properties) => {
    //                     // TODO: Push to outgoing messages queue if packet ID can't be assigned.

    //                     // TODO: Make this async instead of failing so that we can wake up when a packet ID becomes available.
    //                     let packet_identifier = self.pkid_pool.lease_next_pkid().unwrap();
    //                     let publish = Publish {
    //                         topic_name: topic_name
    //                             .into_inner()
    //                             .to_shared(&mut self.owned)
    //                             .expect("TODO: error handling"),
    //                         packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(
    //                             packet_identifier,
    //                             false, // TODO: Get from properties
    //                         ),
    //                         retain: false, // TODO: Get from properties
    //                         payload: payload
    //                             .into_buffered(&mut self.owned)
    //                             .expect("TODO: error handling"),
    //                         other_properties: properties
    //                             .into_buffered(&mut self.owned)
    //                             .expect("TODO: error handling"),
    //                     };
    //                     self.inflight
    //                         .publish_qos2
    //                         .insert(packet_identifier, (publish.clone(), notifier));
    //                     publish
    //                 }
    //             };
    //             Packet::Publish(packet)
    //         }

    //         ConnectedChannelsOutgoingPacket::PingReq => Packet::PingReq(PingReq),
    //     };
    //     Some(packet)
    // }

    /// Complete an in-flight operation with a received acknowledgement.
    /// Adjusts state as appropriate.
    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    pub fn complete_inflight(&mut self, operation: CompletedOperation<O::Shared>) {
        match operation {
            CompletedOperation::Connect(connack) => {
                if let ConnectReasonCode::Success { session_present } = connack.reason_code {
                    self.connected = true;

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

                    // TODO: Get PINGREQ duration from connect properties
                    self.pingreq = Some(PingReqTimer::new(Duration::from_secs(5)));
                }
            }
            CompletedOperation::Subscribe(suback) => {
                let notifier = self
                    .inflight
                    .subscribe
                    .remove(&suback.packet_identifier)
                    .expect("TODO: error handling");
                _ = notifier.complete(suback.into());
            }
            CompletedOperation::Unsubscribe(unsuback) => {
                let notifier = self
                    .inflight
                    .unsubscribe
                    .remove(&unsuback.packet_identifier)
                    .expect("TODO: error handling");
                _ = notifier.complete(unsuback.into());
            }
            CompletedOperation::PublishQoS1(puback) => {
                let (_, notifier) = self
                    .inflight
                    .publish_qos1
                    .remove(&puback.packet_identifier)
                    .expect("TODO: error handling");
                _ = notifier.complete(puback.into());
            }
            CompletedOperation::PublishQoS2(pubrec) => {
                // TODO: Convert pubrec to user-facing type, create pubrel infrastructure
                // and complete notifier
            }
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
                AckHandle::QoS1(PubAckToken::new(
                    packet_identifier,
                    self.connection_epoch,
                    self.ch.ack_tx.clone(),
                ))
            }
            PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, _) => {
                todo!()
            }
        };
        // TODO: Register ack tracking if QoS 1 or 2
        self.ch
            .i_pub_tx
            .send((publish.into(), ack_handle))
            .expect("TODO: error handling");
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

        // NOTE: connection_epoch is NOT reset here, since that would allow for old tokens to become valid again.
        // If session had a different lifespan, this would work differently (and may need to at some point).

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
    i_pub_tx: UnboundedSender<IncomingPublish>,
    /// Channel for sending outgoing ACK requests.
    /// Stored here just to be cloned, should NOT be used directly.
    ack_tx: Sender<AcknowledgementRequest>, // TODO: Is this really the correct place for this?
}

enum OutgoingPacketRequest {
    DisconnectRequest(DisconnectRequest),
    AcknowledgementRequest(AcknowledgementRequest),
    SubscriptionRequest(SubscriptionRequest),
    PublishRequest(PublishRequest),
    PingReq,
}

// enum PollOutgoing {
//     DisconnectRequest(DisconnectRequest),
//     AcknowledgementRequest(AcknowledgementRequest),
//     AckonwledgementReady(AcknowledgementRequest),
//     SubscriptionRequest(SubscriptionRequest),
//     PublishRequest(PublishRequest),
//     PingReq,
// }



// TODO: remove
enum ConnectedChannelsOutgoingPacket {
    DisconnectRequest(DisconnectRequest),
    AcknowledgementRequest(AcknowledgementRequest),
    SubscriptionRequest(SubscriptionRequest),
    PublishRequest(PublishRequest),
    PingReq,
}


// TODO: probably wrap this in a method taking self that handles the loop over this
// TODO: Pingreq reset should probably happen out there, rather than in here.
//      However... on the other hand, that's incosistent with how we might want to handle the ack tracker.
//      Ultimately, is there a loop over this or nah?

// Further complexity here setmes from the idea that AcknowledgementRequest spans all QoS,
// and there are different behaviors for different packets within that, e.g. PUBCOMP isn't ordered.

// Poll for outgoing ACKs, then for outgoing SUBSCRIBEs, then for outgoing PUBLISHes.
// If any of them yields an item, reset the PINGREQ timer, else poll for outgoing PINGREQs.
fn poll_for_outgoing_request(
    ch: &mut Channels,
    mut pingreq: Option<&mut PingReqTimer>,
) -> impl Future<Output = OutgoingPacketRequest> {
    futures_util::future::poll_fn(move |cx| {
        // Disconnects get top priority, since they indicate the user wants to close the connection now.
        if let Some(disconnect_rx) = &mut ch.disconnect_rx
            && let Poll::Ready(disconnect_req) = Pin::new(disconnect_rx).poll(cx)
        {
            drop(ch.disconnect_rx.take());
            if let Ok(disconnect_req) = disconnect_req {
                return Poll::Ready(OutgoingPacketRequest::DisconnectRequest(
                    disconnect_req,
                ));
            }
            // ... else: User dropped the disconnect_tx, so there's nothing more to do.
        }

        // 

        if let Poll::Ready(Some(ack_req)) = ch.ack_rx.poll_recv(cx) {
            // TODO: insert into some kind of tracker and continue

            if let Some(ref mut pingreq) = pingreq {
                pingreq.reset();
            }
            return Poll::Ready(OutgoingPacketRequest::AcknowledgementRequest(
                ack_req,
            ));
        }

        if let Poll::Ready(Some(sub_req)) = ch.sub_rx.poll_recv(cx) {
            if let Some(ref mut pingreq) = pingreq {
                pingreq.reset();
            }
            return Poll::Ready(OutgoingPacketRequest::SubscriptionRequest(
                sub_req,
            ));
        }

        if let Poll::Ready(Some(publish)) = ch.o_pub_rx.poll_recv(cx) {
            if let Some(ref mut pingreq) = pingreq {
                pingreq.reset();
            }
            return Poll::Ready(OutgoingPacketRequest::PublishRequest(publish));
        }

        if let Some(ref mut pingreq) = pingreq
            && let Poll::Ready(()) = Pin::new(&mut *pingreq).poll(cx)
        {
            pingreq.reset();
            return Poll::Ready(OutgoingPacketRequest::PingReq);
        }

        Poll::Pending
    })
}










// // Poll for outgoing ACKs, then for outgoing SUBSCRIBEs, then for outgoing PUBLISHes.
// // If any of them yields an item, reset the PINGREQ timer, else poll for outgoing PINGREQs.
// fn poll_connected_channels(
//     ch: &mut Channels,
//     mut pingreq: Option<&mut PingReqTimer>,
// ) -> impl Future<Output = ConnectedChannelsOutgoingPacket> {
//     futures_util::future::poll_fn(move |cx| {
//         if let Some(disconnect_rx) = &mut ch.disconnect_rx
//             && let Poll::Ready(disconnect_req) = Pin::new(disconnect_rx).poll(cx)
//         {
//             drop(ch.disconnect_rx.take());
//             if let Ok(disconnect_req) = disconnect_req {
//                 return Poll::Ready(ConnectedChannelsOutgoingPacket::DisconnectRequest(
//                     disconnect_req,
//                 ));
//             }
//             // ... else: User dropped the disconnect_tx, so there's nothing more to do.
//         }

//         if let Poll::Ready(Some(ack_req)) = ch.ack_rx.poll_recv(cx) {
//             if let Some(ref mut pingreq) = pingreq {
//                 pingreq.reset();
//             }
//             return Poll::Ready(ConnectedChannelsOutgoingPacket::AcknowledgementRequest(
//                 ack_req,
//             ));
//         }

//         if let Poll::Ready(Some(sub_req)) = ch.sub_rx.poll_recv(cx) {
//             if let Some(ref mut pingreq) = pingreq {
//                 pingreq.reset();
//             }
//             return Poll::Ready(ConnectedChannelsOutgoingPacket::SubscriptionRequest(
//                 sub_req,
//             ));
//         }

//         if let Poll::Ready(Some(publish)) = ch.o_pub_rx.poll_recv(cx) {
//             if let Some(ref mut pingreq) = pingreq {
//                 pingreq.reset();
//             }
//             return Poll::Ready(ConnectedChannelsOutgoingPacket::PublishRequest(publish));
//         }

//         if let Some(ref mut pingreq) = pingreq
//             && let Poll::Ready(()) = Pin::new(&mut *pingreq).poll(cx)
//         {
//             pingreq.reset();
//             return Poll::Ready(ConnectedChannelsOutgoingPacket::PingReq);
//         }

//         Poll::Pending
//     })
// }

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
