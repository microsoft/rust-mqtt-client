// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::VecDeque;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::buffer_pool::Shared;
use crate::client::{
    channel_data::{
        AcknowledgementRequest, ConnectionRequest, IncomingPublish, PublishRequest,
        SubscriptionRequest,
    },
    session::{inflight::InflightTracker, pkid::PkidPool},
};
use crate::mqtt_proto::{
    ConnAck, Connect, Disconnect, Packet, PacketIdentifier, PubAck, Publish, SubAck, UnsubAck,
};
use crate::token::{
    ConnectCompletionNotifier, PublishQoS1CompletionNotifier, SubscribeCompletionNotifier,
    UnsubscribeCompletionNotifier,
};

mod inflight;
mod pkid;

pub struct Session<S: Shared> {
    /// Struct containing channels in and out of the Session
    ch: Channels,
    /// Pool of packet identifiers for outgoing packets that can be leased
    pkid_pool: PkidPool,
    /// Queue of outgoing operations that are not yet in-flight
    /// *Technically* not part of an MQTT Session, but it makes sense to keep it here.
    outgoing_queue: VecDeque<OutgoingOperation<S>>,
    /// Tracker of all inflight MQTT packets awaiting a response
    inflight_tracker: InflightTracker<S>,
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
            inflight_tracker: InflightTracker::new(),
            connected: false,
        }
    }

    /// Returns the next outgoing MQTT packet to be sent over the network
    #[allow(clippy::unused_self)]
    pub async fn next_outgoing_packet(&mut self) -> Option<Packet<S>> {
        unimplemented!()
    }

    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    #[allow(clippy::unused_self)]
    pub fn incoming_publish(&mut self, publish: Publish<S>) {
        unimplemented!()
    }

    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    #[allow(clippy::unused_self)]
    pub fn transition_connected(&mut self, connack: ConnAck<S>) {
        unimplemented!()
    }

    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    #[allow(clippy::unused_self)]
    pub fn transition_disconnected(&mut self, disconnect: Disconnect<S>) {
        unimplemented!()
    }

    #[allow(clippy::needless_pass_by_value)] //TODO: Remove
    #[allow(clippy::unused_self)]
    pub fn complete_inflight(&mut self, operation: CompletedOperation<S>) {
        unimplemented!()
    }
}

enum OutgoingOperation<S>
where
    S: Shared,
{
    Connect(Connect<S>, ConnectCompletionNotifier),
    Subscribe(PacketIdentifier, SubscribeCompletionNotifier),
    Unsubscribe(PacketIdentifier, UnsubscribeCompletionNotifier),
    PublishQoS1(Publish<S>, PublishQoS1CompletionNotifier),
}

pub enum CompletedOperation<S>
where
    S: Shared,
{
    Connect(ConnAck<S>),
    PublishQoS1(PubAck<S>),
    Subscribe(SubAck<S>),
    Unsubscribe(UnsubAck<S>),
    // TODO: QoS 2 publish, pubrec, pubrel
}

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
