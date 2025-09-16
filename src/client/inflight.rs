// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::{HashMap, HashSet};

use thiserror::Error;

use crate::buffer_pool::Shared;
use crate::mqtt_proto::{PacketIdentifier, PacketIdentifierDupQoS, Publish};
use crate::packet::{PubAck, PubRec, SubAck, UnsubAck};
use crate::token::{CompletionTransmitter, PubRelToken};

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
//     QoS1(Publish<S>, CompletionTransmitter<PubAck>),
//     QoS2(Publish<S>, CompletionTransmitter<(PubRec, Option<PubRelToken>)>),
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
    // Superset of all inflight packet identifiers for PUBLISH, SUBSCRIBE, and UNSUBSCRIBE
    inflight_pkids: HashSet<PacketIdentifier>,
    // All inflight QoS 1 PUBLISH operations
    publish_qos1: HashMap<PacketIdentifier, (Publish<S>, CompletionTransmitter<PubAck>)>,
    // All inflight QoS 2 PUBLISH operations
    publish_qos2: HashMap<
        PacketIdentifier,
        (
            Publish<S>,
            CompletionTransmitter<(PubRec, Option<PubRelToken>)>,
        ),
    >,
    // All inflight SUBSCRIBE operations
    subscribe: HashMap<PacketIdentifier, CompletionTransmitter<SubAck>>,
    // All inflight UNSUBSCRIBE operations
    unsubscribe: HashMap<PacketIdentifier, CompletionTransmitter<UnsubAck>>,
    // All inflight PUBREC operations
    pubrec: HashMap<PacketIdentifier, CompletionTransmitter<(PubRel, PubCompToken)>>,
}

impl<S> InflightTracker<S>
where
    S: Shared,
{
    pub fn add_publish_qos1(
        &mut self,
        publish: Publish<S>,
        completion: CompletionTransmitter<PubAck>,
    ) -> Result<(), InsertionError<Publish<S>, CompletionTransmitter<PubAck>>> {
        if let PacketIdentifierDupQoS::AtLeastOnce(pkid, _) = publish.packet_identifier_dup_qos {
            if self.inflight_pkids.contains(&pkid) {
                return Err(InsertionError {
                    packet: publish,
                    completion,
                    reason: format!("Packet Identifier {pkid} is already in-flight")
                });
            }
            self.inflight_pkids.insert(pkid);
            self.publish_qos1.insert(pkid, (publish, completion));
            return Ok(());
        } else {
            return Err(InsertionError {
                packet: publish,
                completion,
                reason: "Publish packet is not QoS 1".to_string()
            });
        }
    }

    pub fn add_publish_qos2(
        &mut self,
        publish: Publish<S>,
        completion: CompletionTransmitter<(PubRec, Option<PubRelToken>)>,
    ) -> Result<(), InsertionError<Publish<S>, CompletionTransmitter<(PubRec, Option<PubRelToken>)>>> {
        if let PacketIdentifierDupQoS::ExactlyOnce(pkid, _) = publish.packet_identifier_dup_qos {
            if self.inflight_pkids.contains(&pkid) {
                return Err(InsertionError {
                    packet: publish,
                    completion,
                    reason: format!("Packet Identifier {pkid} is already in-flight")
                });
            }
            self.inflight_pkids.insert(pkid);
            self.publish_qos2.insert(pkid, (publish, completion));
            return Ok(());
        } else {
            return Err(InsertionError {
                packet: publish,
                completion,
                reason: "Publish packet is not QoS 2".to_string()
            });
        }
    }

}
