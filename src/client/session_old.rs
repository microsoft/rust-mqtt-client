// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Session state management for MQTT client

use std::num::NonZeroU16;

use crate::buffer_pool::Shared;
use crate::client::pkid::PkidPool;
use crate::mqtt_proto::{PacketIdentifier, PubAck, SubAck, UnsubAck, ConnAck, Publish};
use crate::token::{ConnectCompletionNotifier, PublishQoS1CompletionNotifier, SubscribeCompletionNotifier, UnsubscribeCompletionNotifier};

pub struct SessionOptions {
    pub max_inflight: NonZeroU16,
}

// AXIOMS
// - Packets MUST be assembled outside of the SessionState because not all packets go into the SessionState
//      - in fact, really, only publishes are stored...
// - This means Packet IDS must be leased outside of the SessionState


pub struct SessionState {
    pkid_pool: PkidPool,
    // offline queue?
}

impl SessionState {
    pub fn new(options: SessionOptions) -> Self {
        // TODO: Is the max pkid always equal to max_inflight? What about offline queuing? Perhaps this is not true.
        // Also, if we do want this, consider using a conversion trait to avoid the expect.
        let max_pkid = PacketIdentifier::new(options.max_inflight.get()).expect("non-zero");
        Self {
            pkid_pool: PkidPool::new(max_pkid),
        }
    }

    // TODO: If this creates the packet, there need to be distinct functions for each type of operation
    // - if we follow this model, registration could fail for violating one of the rules of the session, e.g. max size
    pub fn register_inflight<S: Shared>(&mut self, operation: InflightOperation<S>) {
        unimplemented!()
    }

    pub fn complete_inflight<S:Shared>(&mut self, operation: CompletedOperation<S>) {
        match operation {
            CompletedOperation::Connect(connack) => {
                // TODO: ?
            }
            CompletedOperation::PublishQoS1(puback) => {
                // TODO: complete the inflight operation
                self.pkid_pool.release_pkid(puback.packet_identifier);
            }
            CompletedOperation::Subscribe(suback) => {
                // TODO: complete the inflight operation
                self.pkid_pool.release_pkid(suback.packet_identifier);
            }
            CompletedOperation::Unsubscribe(unsuback) => {
                // TODO: complete the inflight operation
                self.pkid_pool.release_pkid(unsuback.packet_identifier);
            }
        }
    }

    // What is API experience for:
    // - deleting on disconnect?
    // - redelivery on reconnect?
    // - deleting a packet that is invalid (e.g. too big)
    // And how does the event loop handle 


    // Maybe it really isn't so bad if registering the operation gives you the packet...
    // a bit weird for QoS 0 publish, but otherwise... fine?
    // pub fn register_publish(), etc.?

    // What if leasing a PKID from a SessionState gives you some kind of wrapper?

    // OR: what if registering an inflight operation is what GIVES you the pkid? only publish needs the packet...
    //      - OTOH, publish is a big deal lmao


    // TODO: edge case - how does timing of inflight vs pkid lease work?

    // semantics of leasing vs releasing, cancellation, etc. who is responsible for what?

    // OH QOS 2 ENFORCES THE USE OF THE NOTIFIER TO BE OUTSIDE THE STATE BECAUSE OF THE TOKEN THAT MUST BE PROVIDED
    // - or could the completion token be created inside... very weird, but it could work. probably not good tho
}

pub enum InflightOperation <S>
where S: Shared
{
    Connect(ConnectCompletionNotifier),
    Subscribe(PacketIdentifier, SubscribeCompletionNotifier),
    Unsubscribe(PacketIdentifier, UnsubscribeCompletionNotifier),
    PublishQoS1(Publish<S>, PublishQoS1CompletionNotifier),
}


// TODO: should these be the user facing, or the internal types?
// Consider that there isn't really any "user facing" types for a lot of these things - really just the acks and incoming publishes.
// the rest come in as a Request enum...
pub enum CompletedOperation <S>
where S: Shared
{
    Connect(ConnAck<S>),
    PublishQoS1(PubAck<S>),
    Subscribe(SubAck<S>),
    Unsubscribe(UnsubAck<S>),
    // TODO: QoS 2 publish, pubrec, pubrel
}

// - add inflight packet
// - update connection state
// 