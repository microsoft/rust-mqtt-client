// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::VecDeque;


use crate::client::pkid::PkidPool;
use crate::buffer_pool::Shared;
use crate::mqtt_proto::{Packet, PacketIdentifier, Disconnect, Connect, Publish, ConnAck, PubAck, SubAck, UnsubAck};
use crate::token::{ConnectCompletionNotifier, PublishQoS1CompletionNotifier, SubscribeCompletionNotifier, UnsubscribeCompletionNotifier};


// TODO: rename to `Session`?
pub struct SessionManager <S:Shared> {
    //client_rx: tokio::sync::mpsc::Receiver<Request>,
    
    /// Pool of packet identifiers for outgoing packets that can be leased
    pkid_pool: PkidPool,
    /// Queue of outgoing operations that are not yet in-flight
    /// *Technically* not part of an MQTT Session, but it makes sense to keep it here.
    outgoing_queue: VecDeque<OutgoingOperation<S>>,
    //inflight_tracker: InflightTracker<S>,
    //ack_order: AckOrderer,
    connected: bool,
}

impl <S:Shared> SessionManager <S> {
    /// Returns the next outgoing MQTT packet to be sent over the network
    pub async fn next_outgoing_packet(&mut self) -> Option<Packet<S>> {
        unimplemented!()
    }

    pub fn incoming_publish(&mut self, publish: Publish<S>) {
        unimplemented!()
    }

    pub fn transition_connected(&mut self, connack: ConnAck<S>) {
        unimplemented!()
    }

    pub fn transition_disconnected(&mut self, disconnect: Disconnect<S>) {
        unimplemented!()
    }

    pub fn complete_inflight(&mut self, operation: CompletedOperation<S>) {
        unimplemented!()
    }
}

enum OutgoingOperation <S>
where S: Shared
{
    Connect(Connect<S>, ConnectCompletionNotifier),
    Subscribe(PacketIdentifier, SubscribeCompletionNotifier),
    Unsubscribe(PacketIdentifier, UnsubscribeCompletionNotifier),
    PublishQoS1(Publish<S>, PublishQoS1CompletionNotifier),
}

pub enum CompletedOperation <S>
where S: Shared
{
    Connect(ConnAck<S>),
    PublishQoS1(PubAck<S>),
    Subscribe(SubAck<S>),
    Unsubscribe(UnsubAck<S>),
    // TODO: QoS 2 publish, pubrec, pubrel
}


// struct OutgoingPacketManager {
//     // receivers for packets from client
// }