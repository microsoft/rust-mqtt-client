// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Types that define requests for an MQTT operation

use bytes::Bytes;

use crate::client::AckHandle;
use crate::client::token::{
    PubAckCompletionNotifier, PubCompCompletionNotifier, PubRecAcceptCompletionNotifier,
    PubRecRejectCompletionNotifier, PubRelCompletionNotifier, PublishQoS0CompletionNotifier,
    PublishQoS1CompletionNotifier, PublishQoS2CompletionNotifier, SubscribeCompletionNotifier,
    UnsubscribeCompletionNotifier,
};
use crate::packet::{
    Auth, DisconnectProperties, PubAck, PubComp, PubRec, PubRel, Publish, PublishProperties, QoS,
    SubscribeProperties, UnsubscribeProperties,
};
use crate::topic::{TopicFilter, TopicName};

// TODO: I don't love the "Request" naming, because it implies a "Response" structure which doens't exist.
// It also isn't symmetrical with the IncomingPublish type.
// Revisit naming.

pub struct DisconnectRequest(pub(crate) DisconnectProperties);

/// Request to send a PUBLISH packet.
#[allow(clippy::redundant_field_names)]
pub enum PublishRequest {
    PublishQoS0(
        PublishQoS0CompletionNotifier,
        TopicName,
        Bytes,
        PublishProperties,
    ),
    PublishQoS1(
        PublishQoS1CompletionNotifier,
        TopicName,
        Bytes,
        PublishProperties,
    ),
    PublishQoS2(
        PublishQoS2CompletionNotifier,
        TopicName,
        Bytes,
        PublishProperties,
    ),
}

/// Request to send a subscription-related packet
pub enum SubscriptionRequest {
    // NOTE: A PUBLISH *is* a control packet, but it is not included here as it has a dedicated
    // channel and enum to allow for prioritization.
    Subscribe(
        SubscribeCompletionNotifier,
        TopicFilter,
        QoS,
        SubscribeProperties,
    ),
    Unsubscribe(
        UnsubscribeCompletionNotifier,
        TopicFilter,
        UnsubscribeProperties,
    ),
}

/// Request to send an acknowledgement packet
#[allow(clippy::enum_variant_names)]
pub enum AcknowledgementRequest {
    // NOTE: Use the user facing packet here because why bother with the composite parts when we
    // have an appropriate structure?
    PubAck(PubAckCompletionNotifier, PubAck, u64),
    PubRecAccept(PubRecAcceptCompletionNotifier, PubRec),
    PubRecReject(PubRecRejectCompletionNotifier, PubRec),
    PubRel(PubRelCompletionNotifier, PubRel),
    PubComp(PubCompCompletionNotifier, PubComp),
}

/// Request to send an AUTH packet
// NOTE: Similar to AcknowledgementRequest, we use the user-facing packet type here.
pub struct AuthRequest(pub Auth);

/// Incoming Publish + Acknowledgement infrastructure
pub type IncomingPublish = (Publish, AckHandle);
