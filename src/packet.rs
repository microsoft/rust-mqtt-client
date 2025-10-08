// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! MQTT packet types and associated properties and reason codes.

// TODO: Remove when possible.
#![allow(clippy::derivable_impls)]

// TODO: This may not be necessary in it's entirety - this is a straight port of API proposal stubs.
// Remove items as necessary.

use std::num::{NonZeroU16, NonZeroU32};

use bytes::Bytes;

use crate::error::OperationFailure;
use crate::{buffer_pool, mqtt_proto};
pub use crate::mqtt_proto::{PacketIdentifier, PacketIdentifierDupQoS};  // TODO: repalce instead of re-export
use crate::topic::TopicName;

/// Trait for converting a packet to a buffer-backed internal variant.
// TODO: Should this be here, or will it make more sense to implement as functions in some kind of
// conversion module? Depends on where and how the boundary between public and internal types works
trait IntoBuffered<O>
where
    O: buffer_pool::Owned
{
    type BufferBacked;
    fn into_buffered(self, owned: &mut O) -> Result<Self::BufferBacked, buffer_pool::Error>;
}


#[derive(Debug, PartialEq, Eq, Clone)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadFormatIndicator {
    Unspecified = 0,
    UTF8 = 1,
}


#[derive(Debug, Clone)]
pub struct ConnectProperties {}
impl Default for ConnectProperties {
    fn default() -> Self {
        ConnectProperties {}
    }
}

#[derive(Debug, Clone)]
pub struct ConnAck {
    pub reason: ConnAckReason,
    pub properties: ConnectProperties,
}

impl ConnAck {
    pub fn is_success(&self) -> bool {
        unimplemented!()
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        unimplemented!()
    }
}

/////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Publish {
    pub payload: Bytes,
    pub qos: PacketIdentifierDupQoS,    // TODO: Represent this better (DeliveryQoS enum with DeliveryInfo inside?)
    pub retain: bool,
    pub topic_name: TopicName,
    pub properties: PublishProperties,
}

impl <S> From<mqtt_proto::Publish<S>> for Publish
where
    S: buffer_pool::Shared
{
    fn from(value: mqtt_proto::Publish<S>) -> Publish {
        Publish {
            payload: Bytes::copy_from_slice(value.payload.as_ref()),
            qos: value.packet_identifier_dup_qos,
            retain: value.retain,
            topic_name: value.topic_name.to_owned().into(),
            properties: value.other_properties.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PublishProperties {
    pub payload_format_indicator: PayloadFormatIndicator,
    pub message_expiry_interval: Option<u32>,
    pub topic_alias: Option<NonZeroU16>,
    pub response_topic: Option<TopicName>,
    pub correlation_data: Option<Bytes>,
    pub user_properties: Vec<(String, String)>,
    pub subscription_identifiers: Vec<NonZeroU32>,
    pub content_type: Option<String>,
}

// TODO: can we derive this?
impl Default for PublishProperties {
    fn default() -> Self {
        PublishProperties {
            payload_format_indicator: PayloadFormatIndicator::Unspecified,
            message_expiry_interval: None,
            topic_alias: None,
            response_topic: None,
            correlation_data: None,
            user_properties: Vec::new(),
            subscription_identifiers: Vec::new(),
            content_type: None,
        }
    }
}

impl <S> From<mqtt_proto::PublishOtherProperties<S>> for PublishProperties
where
    S: buffer_pool::Shared
{
    fn from(value: mqtt_proto::PublishOtherProperties<S>) -> PublishProperties {
        let payload_format_indicator = 
            if value.payload_is_utf8 { PayloadFormatIndicator::UTF8 } else { PayloadFormatIndicator::Unspecified };
        PublishProperties {
            payload_format_indicator,
            message_expiry_interval: value.message_expiry_interval,
            topic_alias: value.topic_alias,
            response_topic: value.response_topic.map(|s| s.to_owned().into()),
            correlation_data: value.correlation_data.map(|s| Bytes::copy_from_slice(s.as_ref())),
            user_properties: value.user_properties.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            subscription_identifiers: value.subscription_identifiers,
            content_type: value.content_type.map(|s| s.to_string()),
        }
    }
}

impl <O> IntoBuffered<O> for PublishProperties
where
    O: buffer_pool::Owned
{
    type BufferBacked = mqtt_proto::PublishOtherProperties<O::Shared>;

    fn into_buffered(self, owned: &mut O) -> Result<Self::BufferBacked, buffer_pool::Error> {
        Ok(mqtt_proto::PublishOtherProperties {
            payload_is_utf8: matches!(self.payload_format_indicator, PayloadFormatIndicator::UTF8),
            message_expiry_interval: self.message_expiry_interval,
            topic_alias: self.topic_alias,
            response_topic: self.response_topic.map(|t|t.into_inner().to_shared(owned)).transpose()?,
            correlation_data: self.correlation_data.map(|b| mqtt_proto::BinaryData::new(owned, b)).transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
            subscription_identifiers: self.subscription_identifiers,
            content_type: self.content_type.map(|s| mqtt_proto::ByteStr::new(owned, s)).transpose()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PubAck {
    pub packet_identifier: PacketIdentifier,
    pub reason: PubAckReason,
    pub properties: PubAckProperties,
}

impl PubAck {
    pub fn is_success(&self) -> bool {
        unimplemented!()
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        unimplemented!()
    }
}

impl <S> From<mqtt_proto::PubAck<S>> for PubAck
where
    S: buffer_pool::Shared
{
    fn from(value: mqtt_proto::PubAck<S>) -> PubAck {
        PubAck {
            packet_identifier: value.packet_identifier,
            reason: value.reason_code.into(),
            properties: value.other_properties.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubAckReason {
    // Ok
    Success = 0x00,
    NoMatchingSubscribers = 0x10,
    // Errors
    UnspecifiedError = 0x80,
    ImplementationSpecificError = 0x83,
    NotAuthorized = 0x87,
    TopicNameInvalid = 0x90,
    PacketIdentifierInUse = 0x91,
    QuotaExceeded = 0x97,
    PayloadFormatInvalid = 0x99,
}

impl From<mqtt_proto::PubAckReasonCode> for PubAckReason
{
    fn from(value: mqtt_proto::PubAckReasonCode) -> PubAckReason {
        match value {
            mqtt_proto::PubAckReasonCode::Success => PubAckReason::Success,
            mqtt_proto::PubAckReasonCode::NoMatchingSubscribers => PubAckReason::NoMatchingSubscribers,
            mqtt_proto::PubAckReasonCode::UnspecifiedError => PubAckReason::UnspecifiedError,
            mqtt_proto::PubAckReasonCode::ImplementationSpecificError => PubAckReason::ImplementationSpecificError,
            mqtt_proto::PubAckReasonCode::NotAuthorized => PubAckReason::NotAuthorized,
            mqtt_proto::PubAckReasonCode::TopicNameInvalid => PubAckReason::TopicNameInvalid,
            mqtt_proto::PubAckReasonCode::PacketIdentifierInUse => PubAckReason::PacketIdentifierInUse,
            mqtt_proto::PubAckReasonCode::QuotaExceeded => PubAckReason::QuotaExceeded,
            mqtt_proto::PubAckReasonCode::PayloadFormatInvalid => PubAckReason::PayloadFormatInvalid,
        }
    }
}

impl From<PubAckReason> for mqtt_proto::PubAckReasonCode {
    fn from(value: PubAckReason) -> mqtt_proto::PubAckReasonCode {
        match value {
            PubAckReason::Success => mqtt_proto::PubAckReasonCode::Success,
            PubAckReason::NoMatchingSubscribers => mqtt_proto::PubAckReasonCode::NoMatchingSubscribers,
            PubAckReason::UnspecifiedError => mqtt_proto::PubAckReasonCode::UnspecifiedError,
            PubAckReason::ImplementationSpecificError => mqtt_proto::PubAckReasonCode::ImplementationSpecificError,
            PubAckReason::NotAuthorized => mqtt_proto::PubAckReasonCode::NotAuthorized,
            PubAckReason::TopicNameInvalid => mqtt_proto::PubAckReasonCode::TopicNameInvalid,
            PubAckReason::PacketIdentifierInUse => mqtt_proto::PubAckReasonCode::PacketIdentifierInUse,
            PubAckReason::QuotaExceeded => mqtt_proto::PubAckReasonCode::QuotaExceeded,
            PubAckReason::PayloadFormatInvalid => mqtt_proto::PubAckReasonCode::PayloadFormatInvalid,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl <S> From<mqtt_proto::PubAckOtherProperties<S>> for PubAckProperties
where
    S: buffer_pool::Shared
{
    fn from(value: mqtt_proto::PubAckOtherProperties<S>) -> PubAckProperties {
        PubAckProperties {
            reason_string: value.reason_string.map(|s| s.to_string()),
            user_properties: value.user_properties.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        }
    }
}

impl <O> IntoBuffered<O> for PubAckProperties
where
    O: buffer_pool::Owned
{
    type BufferBacked = mqtt_proto::PubAckOtherProperties<O::Shared>;

    fn into_buffered(self, owned: &mut O) -> Result<Self::BufferBacked, buffer_pool::Error> {
        Ok(mqtt_proto::PubAckOtherProperties {
            reason_string: self.reason_string.map(|s| mqtt_proto::ByteStr::new(owned, s)).transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PubRec {
    pub packet_identifier: PacketIdentifier,
    pub reason: PubRecReason,
    pub properties: PubRecProperties,
}

impl PubRec {
    pub fn is_success(&self) -> bool {
        unimplemented!()
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        unimplemented!()
    }
}

impl <S> From<mqtt_proto::PubRec<S>> for PubRec
where
    S: buffer_pool::Shared
{
    fn from(value: mqtt_proto::PubRec<S>) -> PubRec {
        PubRec {
            packet_identifier: value.packet_identifier,
            reason: value.reason_code.into(),
            properties: value.other_properties.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubRecReason {
    // Ok
    Success = 0x00,
    NoMatchingSubscribers = 0x10,
    // Errors
    UnspecifiedError = 0x80,
    ImplementationSpecificError = 0x83,
    NotAuthorized = 0x87,
    TopicNameInvalid = 0x90,
    PacketIdentifierInUse = 0x91,
    QuotaExceeded = 0x97,
    PayloadFormatInvalid = 0x99,
}

impl From<mqtt_proto::PubRecReasonCode> for PubRecReason
{
    fn from(value: mqtt_proto::PubRecReasonCode) -> PubRecReason {
        match value {
            mqtt_proto::PubRecReasonCode::Success => PubRecReason::Success,
            mqtt_proto::PubRecReasonCode::NoMatchingSubscribers => PubRecReason::NoMatchingSubscribers,
            mqtt_proto::PubRecReasonCode::UnspecifiedError => PubRecReason::UnspecifiedError,
            mqtt_proto::PubRecReasonCode::ImplementationSpecificError => PubRecReason::ImplementationSpecificError,
            mqtt_proto::PubRecReasonCode::NotAuthorized => PubRecReason::NotAuthorized,
            mqtt_proto::PubRecReasonCode::TopicNameInvalid => PubRecReason::TopicNameInvalid,
            mqtt_proto::PubRecReasonCode::PacketIdentifierInUse => PubRecReason::PacketIdentifierInUse,
            mqtt_proto::PubRecReasonCode::QuotaExceeded => PubRecReason::QuotaExceeded,
            mqtt_proto::PubRecReasonCode::PayloadFormatInvalid => PubRecReason::PayloadFormatInvalid,
        }
    }
}

impl From<PubRecReason> for mqtt_proto::PubRecReasonCode {
    fn from(value: PubRecReason) -> mqtt_proto::PubRecReasonCode {
        match value {
            PubRecReason::Success => mqtt_proto::PubRecReasonCode::Success,
            PubRecReason::NoMatchingSubscribers => mqtt_proto::PubRecReasonCode::NoMatchingSubscribers,
            PubRecReason::UnspecifiedError => mqtt_proto::PubRecReasonCode::UnspecifiedError,
            PubRecReason::ImplementationSpecificError => mqtt_proto::PubRecReasonCode::ImplementationSpecificError,
            PubRecReason::NotAuthorized => mqtt_proto::PubRecReasonCode::NotAuthorized,
            PubRecReason::TopicNameInvalid => mqtt_proto::PubRecReasonCode::TopicNameInvalid,
            PubRecReason::PacketIdentifierInUse => mqtt_proto::PubRecReasonCode::PacketIdentifierInUse,
            PubRecReason::QuotaExceeded => mqtt_proto::PubRecReasonCode::QuotaExceeded,
            PubRecReason::PayloadFormatInvalid => mqtt_proto::PubRecReasonCode::PayloadFormatInvalid,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PubRecProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl <S> From<mqtt_proto::PubRecOtherProperties<S>> for PubRecProperties
where
    S: buffer_pool::Shared
{
    fn from(value: mqtt_proto::PubRecOtherProperties<S>) -> PubRecProperties {
        PubRecProperties {
            reason_string: value.reason_string.map(|s| s.to_string()),
            user_properties: value.user_properties.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        }
    }
}

impl <O> IntoBuffered<O> for PubRecProperties
where
    O: buffer_pool::Owned
{
    type BufferBacked = mqtt_proto::PubRecOtherProperties<O::Shared>;

    fn into_buffered(self, owned: &mut O) -> Result<Self::BufferBacked, buffer_pool::Error> {
        Ok(mqtt_proto::PubRecOtherProperties {
            reason_string: self.reason_string.map(|s| mqtt_proto::ByteStr::new(owned, s)).transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
        })
    }
}

// NOTE: strict subset of PubAckReason/PubRecReason
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubRejectReason {
    UnspecifiedError = 0x80,
    ImplementationSpecificError = 0x83,
    NotAuthorized = 0x87,
    TopicNameInvalid = 0x90,
    PacketIdentifierInUse = 0x91,
    QuotaExceeded = 0x97,
    PayloadFormatInvalid = 0x99,
}

impl From<PubRejectReason> for mqtt_proto::PubAckReasonCode {
    fn from(value: PubRejectReason) -> mqtt_proto::PubAckReasonCode {
        match value {
            PubRejectReason::UnspecifiedError => mqtt_proto::PubAckReasonCode::UnspecifiedError,
            PubRejectReason::ImplementationSpecificError => mqtt_proto::PubAckReasonCode::ImplementationSpecificError,
            PubRejectReason::NotAuthorized => mqtt_proto::PubAckReasonCode::NotAuthorized,
            PubRejectReason::TopicNameInvalid => mqtt_proto::PubAckReasonCode::TopicNameInvalid,
            PubRejectReason::PacketIdentifierInUse => mqtt_proto::PubAckReasonCode::PacketIdentifierInUse,
            PubRejectReason::QuotaExceeded => mqtt_proto::PubAckReasonCode::QuotaExceeded,
            PubRejectReason::PayloadFormatInvalid => mqtt_proto::PubAckReasonCode::PayloadFormatInvalid,
        }
    }
}

impl From<PubRejectReason> for mqtt_proto::PubRecReasonCode {
    fn from(value: PubRejectReason) -> mqtt_proto::PubRecReasonCode {
        match value {
            PubRejectReason::UnspecifiedError => mqtt_proto::PubRecReasonCode::UnspecifiedError,
            PubRejectReason::ImplementationSpecificError => mqtt_proto::PubRecReasonCode::ImplementationSpecificError,
            PubRejectReason::NotAuthorized => mqtt_proto::PubRecReasonCode::NotAuthorized,
            PubRejectReason::TopicNameInvalid => mqtt_proto::PubRecReasonCode::TopicNameInvalid,
            PubRejectReason::PacketIdentifierInUse => mqtt_proto::PubRecReasonCode::PacketIdentifierInUse,
            PubRejectReason::QuotaExceeded => mqtt_proto::PubRecReasonCode::QuotaExceeded,
            PubRejectReason::PayloadFormatInvalid => mqtt_proto::PubRecReasonCode::PayloadFormatInvalid,
        }
    }
}

////////////////////////////////////////////////////////////////////

// TODO: Implement

#[derive(Debug, Clone)]
pub struct PubRelProperties {}
impl Default for PubRelProperties {
    fn default() -> Self {
        PubRelProperties {}
    }
}

#[derive(Debug, Clone)]
pub struct PubCompProperties {}
impl Default for PubCompProperties {
    fn default() -> Self {
        PubCompProperties {}
    }
}

#[derive(Debug, Clone)]
pub struct PubRel {
    pub reason: PubRelReason,
    pub properties: PubRelProperties,
}

#[derive(Debug, Clone)]
pub struct PubComp {
    pub reason: PubCompReason,
    pub properties: PubCompProperties,
}


////////////////////////////////////////////////////////////////////


#[derive(Debug, Clone)]
pub struct DisconnectProperties {}
impl Default for DisconnectProperties {
    fn default() -> Self {
        DisconnectProperties {}
    }
}

#[derive(Debug, Clone)]
pub struct SubscribeProperties {}
impl Default for SubscribeProperties {
    fn default() -> Self {
        SubscribeProperties {}
    }
}

#[derive(Debug, Clone)]
pub struct UnsubscribeProperties {}
impl Default for UnsubscribeProperties {
    fn default() -> Self {
        UnsubscribeProperties {}
    }
}

#[derive(Debug, Clone)]
pub struct AuthProperties {}
impl Default for AuthProperties {
    fn default() -> Self {
        AuthProperties {}
    }
}

#[derive(Debug, Clone)]
pub struct SubAck {
    pub reason: SubAckReason,
    pub properties: SubscribeProperties,
}

impl SubAck {
    pub fn is_success(&self) -> bool {
        unimplemented!()
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct UnsubAck {
    pub reason: UnsubAckReason,
    pub properties: UnsubscribeProperties,
}

impl UnsubAck {
    pub fn is_success(&self) -> bool {
        unimplemented!()
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnAckReason {
    Success = 0x00,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    ClientIdentifierNotValid = 0x85,
    BadUserNameOrPassword = 0x86,
    NotAuthorized = 0x87,
    ServerUnavailable = 0x88,
    ServerBusy = 0x89,
    Banned = 0x8A,
    BadAuthenticationMethod = 0x8C,
    TopicNameInvalid = 0x90,
    PacketTooLarge = 0x95,
    QuotaExceeded = 0x97,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QoSNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    ConnectionRateExceeded = 0x9F,
}

// TODO: Not all of these are valid for the application to send
// e.g. "PacketTooLarge" should be determined by the client I think...
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisconnectReason {
    NormalDisconnection = 0x00,
    DisconnectWithWillMessage = 0x04,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    NotAuthorized = 0x87,
    ServerBusy = 0x89,
    ServerShuttingDown = 0x8B,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    ReceiveMaximumExceeded = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    MessageRateTooHigh = 0x96,
    QuotaExceeded = 0x97,
    AdministrativeAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QoSNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    SharedSubscriptionsNotSupported = 0x9E,
    ConnectionRateExceeded = 0x9F,
    MaximumConnectTime = 0xA0,
    SubscriptionIdentifiersNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubAckReason {
    GrantedQoS0 = 0x00,
    GrantedQoS1 = 0x01,
    GrantedQoS2 = 0x02,
    UnspecifiedError = 0x80,
    ImplementationSpecificError = 0x83,
    NotAuthorized = 0x87,
    TopicFilterInvalid = 0x8F,
    PacketIdentifierInUse = 0x91,
    QuotaExceeded = 0x97,
    SharedSubscriptionsNotSupported = 0x9A,
    SubscriptionIdentifiersNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsubAckReason {
    Success = 0x00,
    NoSubscriptionExisted = 0x11,
    UnspecifiedError = 0x80,
    ImplementationSpecificError = 0x83,
    NotAuthorized = 0x87,
    TopicFilterInvalid = 0x8F,
    PacketIdentifierInUse = 0x91,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubRelReason {
    Success = 0x00,
    PacketIdentifierNotFound = 0x92,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubCompReason {
    Success = 0x00,
    PacketIdentifierNotFound = 0x92,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthReason {
    Success = 0x00,
    ContinueAuthentication = 0x18,
    Reauthenticate = 0x19,
}

// TODO: How to handle ack semantics re: reason codes and properties? the naming gets very weird.

// TODO: What about if you do get a subscription, but at a different QoS than you requested? success? failure?
// Anything less than 0x80 is considered a success I think


fn map_user_properties_to_bytestr<S, O>(owned: &mut O, props: Vec<(String, String)>) -> Result<Vec<(mqtt_proto::ByteStr<S>, mqtt_proto::ByteStr<S>)>, buffer_pool::Error>
where
    S: buffer_pool::Shared,
    O: buffer_pool::Owned<Shared = S>,
{
    props.into_iter().map(|(k, v)| {
        let k = mqtt_proto::ByteStr::new(owned, k)?;
        let v = mqtt_proto::ByteStr::new(owned, v)?;
        Ok((k, v))
    }).collect()
}