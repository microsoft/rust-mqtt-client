// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::{HashMap, HashSet};

use thiserror::Error;

use crate::buffer_pool::Shared;
use crate::mqtt_proto::{PacketIdentifier, PacketIdentifierDupQoS, Publish};
use crate::token::{
    PubRecCompletionNotifier, PubRelCompletionNotifier, PubRelToken, PublishQoS1CompletionNotifier,
    PublishQoS2CompletionNotifier, SubscribeCompletionNotifier, UnsubscribeCompletionNotifier,
};

// #[derive(Error, Debug)]
// pub struct OccupiedError(#[from] OccupiedError);

#[derive(Error, Debug)]
#[error("Packet Identifier {0} is already in-flight")]
pub struct PkidError(PacketIdentifier);

// pub enum InsertionError {
//     PkidOccupied(PacketIdentifier),
//     Invalid,
//     // Other variants as needed
// }

// pub enum InflightPublish<S>
// where S: Shared
// {
//     QoS1(Publish<S>, CompletionNotifier<PubAck>),
//     QoS2(Publish<S>, CompletionNotifier<(PubRec, Option<PubRelToken>)>),
//     // Other packet types as needed
// }

#[derive(Debug, Error)]
#[error("{reason}")]
pub struct InsertionError<P, C> {
    pub packet: P,
    pub completion: C,
    pub reason: String,
}

/// Tracks inflight messages and their associated values
#[derive(Debug, Default)]
pub struct InflightTracker<S>
where
    S: Shared,
{
    // --- Operation tracking (shares PKID pool) ---
    // Superset of all inflight packet identifiers for PUBLISH, SUBSCRIBE, and UNSUBSCRIBE
    inflight_pkids: HashSet<PacketIdentifier>,
    // All inflight QoS 1 PUBLISH operations
    publish_qos1: HashMap<PacketIdentifier, (Publish<S>, PublishQoS1CompletionNotifier)>,
    // All inflight QoS 2 PUBLISH operations
    publish_qos2: HashMap<PacketIdentifier, (Publish<S>, PublishQoS2CompletionNotifier)>,
    // All inflight SUBSCRIBE operations
    subscribe: HashMap<PacketIdentifier, SubscribeCompletionNotifier>,
    // All inflight UNSUBSCRIBE operations
    unsubscribe: HashMap<PacketIdentifier, UnsubscribeCompletionNotifier>,

    // --- Acknowledgement tracking (does not share PKID pool) ---
    // All inflight PUBREC operations
    pubrec: HashMap<PacketIdentifier, PubRecCompletionNotifier>,
    // All inflight PUBREL operations
    pubrel: HashMap<PacketIdentifier, PubRelCompletionNotifier>,
}

// TODO: how does the pkid get freed up again? Outside of this module?
// i.e. when we do mass deletes, probably needs to be hooked in here somewhow...

// TODO: how do we interact with this? Insert, fetch for redelivery, complete, delete...

// TODO: if the key is shared, could it be flattened somewhat?
// - we could use an enum over the types of completion notifiers, but that would make clearing slowerly likely
// - consider that sub and unsub are simply not going to happen all that often...

impl<S> InflightTracker<S>
where
    S: Shared,
{
    pub fn add_publish_qos1(
        &mut self,
        publish: Publish<S>,
        completion: PublishQoS1CompletionNotifier,
    ) -> Result<(), InsertionError<Publish<S>, PublishQoS1CompletionNotifier>> {
        if let PacketIdentifierDupQoS::AtLeastOnce(pkid, _) = publish.packet_identifier_dup_qos {
            if self.inflight_pkids.contains(&pkid) {
                return Err(InsertionError {
                    packet: publish,
                    completion,
                    reason: format!("Packet Identifier {pkid} is already in-flight"),
                });
            }
            self.inflight_pkids.insert(pkid);
            self.publish_qos1.insert(pkid, (publish, completion));
            return Ok(());
        } else {
            return Err(InsertionError {
                packet: publish,
                completion,
                reason: "Publish packet is not QoS 1".to_string(),
            });
        }
    }

    pub fn add_publish_qos2(
        &mut self,
        publish: Publish<S>,
        completion: PublishQoS2CompletionNotifier,
    ) -> Result<(), InsertionError<Publish<S>, PublishQoS2CompletionNotifier>> {
        if let PacketIdentifierDupQoS::ExactlyOnce(pkid, _) = publish.packet_identifier_dup_qos {
            if self.inflight_pkids.contains(&pkid) {
                return Err(InsertionError {
                    packet: publish,
                    completion,
                    reason: format!("Packet Identifier {pkid} is already in-flight"),
                });
            }
            self.inflight_pkids.insert(pkid);
            self.publish_qos2.insert(pkid, (publish, completion));
            return Ok(());
        } else {
            return Err(InsertionError {
                packet: publish,
                completion,
                reason: "Publish packet is not QoS 2".to_string(),
            });
        }
    }
}
