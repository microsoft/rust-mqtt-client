// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Types that define requests for an MQTT operation

use bytes::Bytes;

use crate::client::AckHandle;
use crate::packet::{
    ConnectProperties, DisconnectProperties, PubAck, PubComp, PubRec, PubRel, Publish,
    PublishProperties, QoS, SubscribeProperties, UnsubscribeProperties,
};
use crate::token::{
    ConnectCompletionNotifier, DisconnectCompletionNotifier, PubAckCompletionNotifier,
    PubCompCompletionNotifier, PubRecCompletionNotifier, PubRelCompletionNotifier,
    PublishQoS0CompletionNotifier, PublishQoS1CompletionNotifier, PublishQoS2CompletionNotifier,
    SubscribeCompletionNotifier, UnsubscribeCompletionNotifier,
};
use crate::topic::{TopicFilter, TopicName};

// TODO: I don't love the "Request" naming, because it implies a "Response" structure which doens't exist.
// It also isn't symmetrical with the IncomingPublish type.
// Revisit naming.

/// Request to send a connection-related packet
pub enum ConnectionRequest {
    Connect(ConnectCompletionNotifier, ConnectProperties),
    Disconnect(DisconnectCompletionNotifier, DisconnectProperties),
}

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

// NOTE: Use the user facing packet here because why bother with the composite parts when we
// have an appropriate structure?
/// Request to send an acknowledgement packet
#[allow(clippy::enum_variant_names)]
pub enum AcknowledgementRequest {
    PubAck(PubAckCompletionNotifier, PubAck),
    PubRec(PubRecCompletionNotifier, PubRec),
    PubRel(PubRelCompletionNotifier, PubRel),
    PubComp(PubCompCompletionNotifier, PubComp),
}

/// Incoming Publish + Acknowledgement infrastructure
pub type IncomingPublish = (Publish, AckHandle);
