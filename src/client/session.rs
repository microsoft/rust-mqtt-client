// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::{HashMap, VecDeque};

use tokio::sync::mpsc::{Receiver, Sender};

use crate::buffer_pool::Shared;
use crate::client::{
    channel_data::{
        AcknowledgementRequest, ConnectionRequest, IncomingPublish, PublishRequest,
        SubscriptionRequest,
    },
    session::pkid::PkidPool,
};
use crate::mqtt_proto::{
    ConnAck, Disconnect, Packet, PacketIdentifier, PubAck, PubRec, PubRel, Publish,
    SessionExpiryInterval, SubAck, UnsubAck,
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
        };
        Self {
            ch,
            pkid_pool: PkidPool::new(max_pkid),
            outgoing_queue: Default::default(),
            inflight: InflightTracker::default(),
            connected: false,
        }
    }

    /// Returns the next outgoing MQTT packet to be sent over the network
    #[allow(clippy::unused_self)]
    pub async fn next_outgoing_packet(&mut self) -> Option<Packet<S>> {
        unimplemented!()
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
                    }
                    // TODO: Convert connack to user-facing type and complete notifier
                    //let connack = connack.into()
                    //notifier.complete(connack);
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

    /// Trigger a disconnect and adjust state based on the information in the `Disconnect` packet
    pub fn transition_disconnected(&mut self, disconnect: &Disconnect<S>) {
        // NOTE: When we cancel CompletionNotifiers here, we don't care about the Result because
        // if it fails, that just means the user no longer has the corresponding CompletionToken

        // Set connection state
        self.connected = false;
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
