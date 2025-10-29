// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Types that define requests for an MQTT operation

use crate::buffer_pool::{Shared, SharedImpl};
use crate::client::token::{
    AckHandle, PubAckCompletionNotifier, PubCompCompletionNotifier, PubRecAcceptCompletionNotifier,
    PubRecRejectCompletionNotifier, PubRelCompletionNotifier, PublishQoS0CompletionNotifier,
    PublishQoS1CompletionNotifier, PublishQoS2CompletionNotifier, ReauthCompletionNotifier,
    SubscribeCompletionNotifier, UnsubscribeCompletionNotifier,
};
use crate::mqtt_proto::{
    Auth, ByteStr, Filter, PubAck, PubComp, PubRec, PubRel, Publish, PublishOtherProperties,
    SessionExpiryInterval, SubscribeOtherProperties, Topic, UnsubscribeOtherProperties,
};
use crate::packet::{DisconnectProperties, QoS};

// TODO: I don't love the "Request" naming, because it implies a "Response" structure which doens't exist.
// It also isn't symmetrical with the IncomingPublish type.
// Revisit naming.

pub struct DisconnectRequest<S>
where
    S: Shared,
{
    pub session_expiry_interval: Option<SessionExpiryInterval>,
    pub reason_string: Option<ByteStr<S>>,
    pub user_properties: Vec<(ByteStr<S>, ByteStr<S>)>,
    pub server_reference: Option<ByteStr<S>>,
}

impl DisconnectRequest<SharedImpl> {
    pub fn new(properties: &DisconnectProperties) -> Self {
        let DisconnectProperties {
            session_expiry_interval,
            reason_string,
            user_properties,
            server_reference,
        } = properties;
        Self {
            session_expiry_interval: *session_expiry_interval,
            reason_string: reason_string.as_deref().map(Into::into),
            user_properties: user_properties
                .iter()
                .map(|(key, value)| (key.as_str().into(), value.as_str().into()))
                .collect(),
            server_reference: server_reference.as_deref().map(Into::into),
        }
    }
}

/// Request to send a PUBLISH packet.
#[allow(clippy::redundant_field_names)]
pub enum PublishRequest<S>
where
    S: Shared,
{
    PublishQoS0(
        PublishQoS0CompletionNotifier,
        Topic<ByteStr<S>>,
        S,
        PublishOtherProperties<S>,
    ),
    PublishQoS1(
        PublishQoS1CompletionNotifier,
        Topic<ByteStr<S>>,
        S,
        PublishOtherProperties<S>,
    ),
    PublishQoS2(
        PublishQoS2CompletionNotifier<S>,
        Topic<ByteStr<S>>,
        S,
        PublishOtherProperties<S>,
    ),
}

/// Request to send a subscription-related packet
pub enum SubscriptionRequest<S>
where
    S: Shared,
{
    // NOTE: A PUBLISH *is* a control packet, but it is not included here as it has a dedicated
    // channel and enum to allow for prioritization.
    Subscribe(
        SubscribeCompletionNotifier,
        Filter<ByteStr<S>>,
        QoS,
        SubscribeOtherProperties<S>,
    ),
    Unsubscribe(
        UnsubscribeCompletionNotifier,
        Filter<ByteStr<S>>,
        UnsubscribeOtherProperties<S>,
    ),
}

/// Request to send an acknowledgement packet
#[allow(clippy::enum_variant_names)]
pub enum AcknowledgementRequest<S>
where
    S: Shared,
{
    // NOTE: Use the user facing packet here because why bother with the composite parts when we
    // have an appropriate structure?
    PubAck(PubAckCompletionNotifier, PubAck<S>, u64),
    PubRecAccept(PubRecAcceptCompletionNotifier<S>, PubRec<S>),
    PubRecReject(PubRecRejectCompletionNotifier, PubRec<S>),
    PubRel(PubRelCompletionNotifier, PubRel<S>),
    PubComp(PubCompCompletionNotifier, PubComp<S>),
}

/// Request to send an AUTH packet
pub struct ReauthRequest<S>(pub ReauthCompletionNotifier<S>, pub Auth<S>)
where
    S: Shared;

/// Incoming Publish + Acknowledgement infrastructure
pub type IncomingPublish<S> = (Publish<S>, AckHandle<S>);
