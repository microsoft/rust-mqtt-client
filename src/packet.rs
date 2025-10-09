// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! MQTT packet types and associated properties and reason codes.

// TODO: This may not be necessary in it's entirety - this is a straight port of API proposal stubs.
// Remove items as necessary.

use std::num::{NonZeroU16, NonZeroU32};

use bytes::Bytes;

use crate::error::OperationFailure;
pub use crate::mqtt_proto::PacketIdentifier; // TODO: repalce instead of re-export
use crate::topic::TopicName;
use crate::{buffer_pool, mqtt_proto};

// TODO: Optimize all conversions of Bytes in this module for S = SharedImpl

//////////////////// Misc. ////////////////////

/// Quality of Service
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

/// Quality of Service for an incoming PUBLISH packet, containing additional delivery info
/// for QoS 1 and 2.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DeliveryQoS {
    AtMostOnce,
    AtLeastOnce(DeliveryInfo),
    ExactlyOnce(DeliveryInfo),
}

impl From<mqtt_proto::PacketIdentifierDupQoS> for DeliveryQoS {
    fn from(value: mqtt_proto::PacketIdentifierDupQoS) -> DeliveryQoS {
        match value {
            mqtt_proto::PacketIdentifierDupQoS::AtMostOnce => DeliveryQoS::AtMostOnce,
            mqtt_proto::PacketIdentifierDupQoS::AtLeastOnce(packet_id, dup) => {
                DeliveryQoS::AtLeastOnce(DeliveryInfo {
                    dup,
                    packet_identifier: packet_id,
                })
            }
            mqtt_proto::PacketIdentifierDupQoS::ExactlyOnce(packet_id, dup) => {
                DeliveryQoS::ExactlyOnce(DeliveryInfo {
                    dup,
                    packet_identifier: packet_id,
                })
            }
        }
    }
}

impl From<DeliveryQoS> for mqtt_proto::PacketIdentifierDupQoS {
    fn from(value: DeliveryQoS) -> mqtt_proto::PacketIdentifierDupQoS {
        match value {
            DeliveryQoS::AtMostOnce => mqtt_proto::PacketIdentifierDupQoS::AtMostOnce,
            DeliveryQoS::AtLeastOnce(info) => {
                mqtt_proto::PacketIdentifierDupQoS::AtLeastOnce(info.packet_identifier, info.dup)
            }
            DeliveryQoS::ExactlyOnce(info) => {
                mqtt_proto::PacketIdentifierDupQoS::ExactlyOnce(info.packet_identifier, info.dup)
            }
        }
    }
}

/// Information about a delivery of a PUBLISH packet with QoS 1 or 2
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DeliveryInfo {
    dup: bool,
    packet_identifier: PacketIdentifier,
}

/// Indicates whether the payload is UTF-8 encoded or not
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadFormatIndicator {
    Unspecified = 0,
    UTF8 = 1,
}

//////////////////// Packets ////////////////////

/// CONNACK packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    pub reason: ConnAckReason,
    pub properties: ConnAckProperties,
}

impl ConnAck {
    pub fn is_success(&self) -> bool {
        todo!()
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        todo!()
    }
}

/// PUBLISH packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub payload: Bytes,
    pub qos: DeliveryQoS,
    pub retain: bool,
    pub topic_name: TopicName,
    pub properties: PublishProperties,
}

impl<S> From<mqtt_proto::Publish<S>> for Publish
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::Publish<S>) -> Publish {
        Publish {
            payload: Bytes::copy_from_slice(value.payload.as_ref()),
            qos: value.packet_identifier_dup_qos.into(),
            retain: value.retain,
            topic_name: value.topic_name.to_owned().into(),
            properties: value.other_properties.into(),
        }
    }
}

/// PUBACK packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubAck {
    pub packet_identifier: PacketIdentifier,
    pub reason: PubAckReason,
    pub properties: PubAckProperties,
}

impl PubAck {
    pub fn is_success(&self) -> bool {
        matches!(
            self.reason,
            PubAckReason::Success | PubAckReason::NoMatchingSubscribers
        )
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        if self.is_success() {
            Ok(())
        } else {
            let s = if let Some(reason_string) = &self.properties.reason_string {
                format!(" ({:?} - {reason_string})", self.reason)
            } else {
                format!(" ({:?})", self.reason)
            };
            Err(s.into())
        }
    }
}

impl<S> From<mqtt_proto::PubAck<S>> for PubAck
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::PubAck<S>) -> PubAck {
        PubAck {
            packet_identifier: value.packet_identifier,
            reason: value.reason_code.into(),
            properties: value.other_properties.into(),
        }
    }
}

/// PUBREC packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubRec {
    pub packet_identifier: PacketIdentifier,
    pub reason: PubRecReason,
    pub properties: PubRecProperties,
}

impl PubRec {
    pub fn is_success(&self) -> bool {
        matches!(
            self.reason,
            PubRecReason::Success | PubRecReason::NoMatchingSubscribers
        )
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        if self.is_success() {
            Ok(())
        } else {
            let s = if let Some(reason_string) = &self.properties.reason_string {
                format!(" ({:?} - {reason_string})", self.reason)
            } else {
                format!(" ({:?})", self.reason)
            };
            Err(s.into())
        }
    }
}

impl<S> From<mqtt_proto::PubRec<S>> for PubRec
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::PubRec<S>) -> PubRec {
        PubRec {
            packet_identifier: value.packet_identifier,
            reason: value.reason_code.into(),
            properties: value.other_properties.into(),
        }
    }
}

// PUBREL packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubRel {
    pub packet_identifier: PacketIdentifier,
    pub reason: PubRelReason,
    pub properties: PubRelProperties,
}

impl<S> From<mqtt_proto::PubRel<S>> for PubRel
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::PubRel<S>) -> PubRel {
        PubRel {
            packet_identifier: value.packet_identifier,
            reason: value.reason_code.into(),
            properties: value.other_properties.into(),
        }
    }
}

// PUBCOMP packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubComp {
    pub packet_identifier: PacketIdentifier,
    pub reason: PubCompReason,
    pub properties: PubCompProperties,
}

impl <S> From<mqtt_proto::PubComp<S>> for PubComp
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::PubComp<S>) -> PubComp {
        PubComp {
            packet_identifier: value.packet_identifier,
            reason: value.reason_code.into(),
            properties: value.other_properties.into(),
        }
    }
}

/// MQTT SUBACK
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    pub reason: SubAckReason,
    pub properties: SubAckProperties,
}

impl SubAck {
    pub fn is_success(&self) -> bool {
        todo!()
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        todo!()
    }
}

/// MQTT UNSUBACK
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAck {
    pub reason: UnsubAckReason,
    pub properties: UnsubAckProperties,
}

impl UnsubAck {
    pub fn is_success(&self) -> bool {
        todo!()
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        todo!()
    }
}

//////////////////// Properties ////////////////////

// TODO
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ConnectProperties {}

// TODO
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ConnAckProperties {}

/// Properties for a PUBLISH
#[derive(Debug, Clone, Eq, PartialEq)]
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

impl<S> From<mqtt_proto::PublishOtherProperties<S>> for PublishProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::PublishOtherProperties<S>) -> PublishProperties {
        let payload_format_indicator = if value.payload_is_utf8 {
            PayloadFormatIndicator::UTF8
        } else {
            PayloadFormatIndicator::Unspecified
        };
        PublishProperties {
            payload_format_indicator,
            message_expiry_interval: value.message_expiry_interval,
            topic_alias: value.topic_alias,
            response_topic: value.response_topic.map(|s| s.to_owned().into()),
            correlation_data: value
                .correlation_data
                .map(|s| Bytes::copy_from_slice(s.as_ref())),
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            subscription_identifiers: value.subscription_identifiers,
            content_type: value.content_type.map(|s| s.to_string()),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::PublishOtherProperties<O::Shared>, O> for PublishProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::PublishOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::PublishOtherProperties {
            payload_is_utf8: matches!(self.payload_format_indicator, PayloadFormatIndicator::UTF8),
            message_expiry_interval: self.message_expiry_interval,
            topic_alias: self.topic_alias,
            response_topic: self
                .response_topic
                .map(|t| t.into_inner().to_shared(owned))
                .transpose()?,
            correlation_data: self
                .correlation_data
                .map(|b| mqtt_proto::BinaryData::new(owned, b))
                .transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
            subscription_identifiers: self.subscription_identifiers,
            content_type: self
                .content_type
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
        })
    }
}

/// Properties for a PUBACK
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl<S> From<mqtt_proto::PubAckOtherProperties<S>> for PubAckProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::PubAckOtherProperties<S>) -> PubAckProperties {
        PubAckProperties {
            reason_string: value.reason_string.map(|s| s.to_string()),
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::PubAckOtherProperties<O::Shared>, O> for PubAckProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::PubAckOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::PubAckOtherProperties {
            reason_string: self
                .reason_string
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
        })
    }
}

/// Properties for a PUBREC
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PubRecProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl<S> From<mqtt_proto::PubRecOtherProperties<S>> for PubRecProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::PubRecOtherProperties<S>) -> PubRecProperties {
        PubRecProperties {
            reason_string: value.reason_string.map(|s| s.to_string()),
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::PubRecOtherProperties<O::Shared>, O> for PubRecProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::PubRecOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::PubRecOtherProperties {
            reason_string: self
                .reason_string
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
        })
    }
}

/// Properties for a PUBREL
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PubRelProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl<S> From<mqtt_proto::PubRelOtherProperties<S>> for PubRelProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::PubRelOtherProperties<S>) -> PubRelProperties {
        PubRelProperties {
            reason_string: value.reason_string.map(|s| s.to_string()),
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::PubRelOtherProperties<O::Shared>, O> for PubRelProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::PubRelOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::PubRelOtherProperties {
            reason_string: self
                .reason_string
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
        })
    }
}

// Properties for a PUBCOMP
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PubCompProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl<S> From<mqtt_proto::PubCompOtherProperties<S>> for PubCompProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::PubCompOtherProperties<S>) -> PubCompProperties {
        PubCompProperties {
            reason_string: value.reason_string.map(|s| s.to_string()),
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::PubCompOtherProperties<O::Shared>, O> for PubCompProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::PubCompOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::PubCompOtherProperties {
            reason_string: self
                .reason_string
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
        })
    }
}

// TODO
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SubscribeProperties {}

// TODO
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SubAckProperties {}

// TODO
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UnsubscribeProperties {}

// TODO
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UnsubAckProperties {}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DisconnectProperties {}

// TODO
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AuthProperties {}

//////////////////// Reasons ////////////////////

/// Reason code for a CONNACK
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

/// Reason code for a PUBACK
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

impl From<mqtt_proto::PubAckReasonCode> for PubAckReason {
    fn from(value: mqtt_proto::PubAckReasonCode) -> PubAckReason {
        match value {
            mqtt_proto::PubAckReasonCode::Success => PubAckReason::Success,
            mqtt_proto::PubAckReasonCode::NoMatchingSubscribers => {
                PubAckReason::NoMatchingSubscribers
            }
            mqtt_proto::PubAckReasonCode::UnspecifiedError => PubAckReason::UnspecifiedError,
            mqtt_proto::PubAckReasonCode::ImplementationSpecificError => {
                PubAckReason::ImplementationSpecificError
            }
            mqtt_proto::PubAckReasonCode::NotAuthorized => PubAckReason::NotAuthorized,
            mqtt_proto::PubAckReasonCode::TopicNameInvalid => PubAckReason::TopicNameInvalid,
            mqtt_proto::PubAckReasonCode::PacketIdentifierInUse => {
                PubAckReason::PacketIdentifierInUse
            }
            mqtt_proto::PubAckReasonCode::QuotaExceeded => PubAckReason::QuotaExceeded,
            mqtt_proto::PubAckReasonCode::PayloadFormatInvalid => {
                PubAckReason::PayloadFormatInvalid
            }
        }
    }
}

impl From<PubAckReason> for mqtt_proto::PubAckReasonCode {
    fn from(value: PubAckReason) -> mqtt_proto::PubAckReasonCode {
        match value {
            PubAckReason::Success => mqtt_proto::PubAckReasonCode::Success,
            PubAckReason::NoMatchingSubscribers => {
                mqtt_proto::PubAckReasonCode::NoMatchingSubscribers
            }
            PubAckReason::UnspecifiedError => mqtt_proto::PubAckReasonCode::UnspecifiedError,
            PubAckReason::ImplementationSpecificError => {
                mqtt_proto::PubAckReasonCode::ImplementationSpecificError
            }
            PubAckReason::NotAuthorized => mqtt_proto::PubAckReasonCode::NotAuthorized,
            PubAckReason::TopicNameInvalid => mqtt_proto::PubAckReasonCode::TopicNameInvalid,
            PubAckReason::PacketIdentifierInUse => {
                mqtt_proto::PubAckReasonCode::PacketIdentifierInUse
            }
            PubAckReason::QuotaExceeded => mqtt_proto::PubAckReasonCode::QuotaExceeded,
            PubAckReason::PayloadFormatInvalid => {
                mqtt_proto::PubAckReasonCode::PayloadFormatInvalid
            }
        }
    }
}

/// Reason code for a PUBREC
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

impl From<mqtt_proto::PubRecReasonCode> for PubRecReason {
    fn from(value: mqtt_proto::PubRecReasonCode) -> PubRecReason {
        match value {
            mqtt_proto::PubRecReasonCode::Success => PubRecReason::Success,
            mqtt_proto::PubRecReasonCode::NoMatchingSubscribers => {
                PubRecReason::NoMatchingSubscribers
            }
            mqtt_proto::PubRecReasonCode::UnspecifiedError => PubRecReason::UnspecifiedError,
            mqtt_proto::PubRecReasonCode::ImplementationSpecificError => {
                PubRecReason::ImplementationSpecificError
            }
            mqtt_proto::PubRecReasonCode::NotAuthorized => PubRecReason::NotAuthorized,
            mqtt_proto::PubRecReasonCode::TopicNameInvalid => PubRecReason::TopicNameInvalid,
            mqtt_proto::PubRecReasonCode::PacketIdentifierInUse => {
                PubRecReason::PacketIdentifierInUse
            }
            mqtt_proto::PubRecReasonCode::QuotaExceeded => PubRecReason::QuotaExceeded,
            mqtt_proto::PubRecReasonCode::PayloadFormatInvalid => {
                PubRecReason::PayloadFormatInvalid
            }
        }
    }
}

impl From<PubRecReason> for mqtt_proto::PubRecReasonCode {
    fn from(value: PubRecReason) -> mqtt_proto::PubRecReasonCode {
        match value {
            PubRecReason::Success => mqtt_proto::PubRecReasonCode::Success,
            PubRecReason::NoMatchingSubscribers => {
                mqtt_proto::PubRecReasonCode::NoMatchingSubscribers
            }
            PubRecReason::UnspecifiedError => mqtt_proto::PubRecReasonCode::UnspecifiedError,
            PubRecReason::ImplementationSpecificError => {
                mqtt_proto::PubRecReasonCode::ImplementationSpecificError
            }
            PubRecReason::NotAuthorized => mqtt_proto::PubRecReasonCode::NotAuthorized,
            PubRecReason::TopicNameInvalid => mqtt_proto::PubRecReasonCode::TopicNameInvalid,
            PubRecReason::PacketIdentifierInUse => {
                mqtt_proto::PubRecReasonCode::PacketIdentifierInUse
            }
            PubRecReason::QuotaExceeded => mqtt_proto::PubRecReasonCode::QuotaExceeded,
            PubRecReason::PayloadFormatInvalid => {
                mqtt_proto::PubRecReasonCode::PayloadFormatInvalid
            }
        }
    }
}

/// Reason code for a PUBACK or PUBREC indicating rejection
/// Strict subset of `PubAckReason`/`PubRecReason`
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
            PubRejectReason::ImplementationSpecificError => {
                mqtt_proto::PubAckReasonCode::ImplementationSpecificError
            }
            PubRejectReason::NotAuthorized => mqtt_proto::PubAckReasonCode::NotAuthorized,
            PubRejectReason::TopicNameInvalid => mqtt_proto::PubAckReasonCode::TopicNameInvalid,
            PubRejectReason::PacketIdentifierInUse => {
                mqtt_proto::PubAckReasonCode::PacketIdentifierInUse
            }
            PubRejectReason::QuotaExceeded => mqtt_proto::PubAckReasonCode::QuotaExceeded,
            PubRejectReason::PayloadFormatInvalid => {
                mqtt_proto::PubAckReasonCode::PayloadFormatInvalid
            }
        }
    }
}

impl From<PubRejectReason> for mqtt_proto::PubRecReasonCode {
    fn from(value: PubRejectReason) -> mqtt_proto::PubRecReasonCode {
        match value {
            PubRejectReason::UnspecifiedError => mqtt_proto::PubRecReasonCode::UnspecifiedError,
            PubRejectReason::ImplementationSpecificError => {
                mqtt_proto::PubRecReasonCode::ImplementationSpecificError
            }
            PubRejectReason::NotAuthorized => mqtt_proto::PubRecReasonCode::NotAuthorized,
            PubRejectReason::TopicNameInvalid => mqtt_proto::PubRecReasonCode::TopicNameInvalid,
            PubRejectReason::PacketIdentifierInUse => {
                mqtt_proto::PubRecReasonCode::PacketIdentifierInUse
            }
            PubRejectReason::QuotaExceeded => mqtt_proto::PubRecReasonCode::QuotaExceeded,
            PubRejectReason::PayloadFormatInvalid => {
                mqtt_proto::PubRecReasonCode::PayloadFormatInvalid
            }
        }
    }
}

/// Reason code for a PUBREL
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubRelReason {
    Success = 0x00,
    PacketIdentifierNotFound = 0x92,
}

impl From<mqtt_proto::PubRelReasonCode> for PubRelReason {
    fn from(value: mqtt_proto::PubRelReasonCode) -> PubRelReason {
        match value {
            mqtt_proto::PubRelReasonCode::Success => PubRelReason::Success,
            mqtt_proto::PubRelReasonCode::PacketIdentifierNotFound => {
                PubRelReason::PacketIdentifierNotFound
            }
        }
    }
}

impl From<PubRelReason> for mqtt_proto::PubRelReasonCode {
    fn from(value: PubRelReason) -> mqtt_proto::PubRelReasonCode {
        match value {
            PubRelReason::Success => mqtt_proto::PubRelReasonCode::Success,
            PubRelReason::PacketIdentifierNotFound => {
                mqtt_proto::PubRelReasonCode::PacketIdentifierNotFound
            }
        }
    }
}

/// Reason code for a PUBCOMP
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubCompReason {
    Success = 0x00,
    PacketIdentifierNotFound = 0x92,
}

impl From<mqtt_proto::PubCompReasonCode> for PubCompReason {
    fn from(value: mqtt_proto::PubCompReasonCode) -> PubCompReason {
        match value {
            mqtt_proto::PubCompReasonCode::Success => PubCompReason::Success,
            mqtt_proto::PubCompReasonCode::PacketIdentifierNotFound => {
                PubCompReason::PacketIdentifierNotFound
            }
        }
    }
}

impl From<PubCompReason> for mqtt_proto::PubCompReasonCode {
    fn from(value: PubCompReason) -> mqtt_proto::PubCompReasonCode {
        match value {
            PubCompReason::Success => mqtt_proto::PubCompReasonCode::Success,
            PubCompReason::PacketIdentifierNotFound => {
                mqtt_proto::PubCompReasonCode::PacketIdentifierNotFound
            }
        }
    }
}

/// Reason code for a SUBACK
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

/// Reason code for a UNSUBACK
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

/// Reason code for a DISCONNECT
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

/// Reason code for an AUTH
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthReason {
    Success = 0x00,
    ContinueAuthentication = 0x18,
    Reauthenticate = 0x19,
}

// TODO: What about if you do get a subscription, but at a different QoS than you requested? success? failure?
// Anything less than 0x80 is considered a success I think

//////////////////// Utility ////////////////////

#[allow(dead_code)] // TODO: remove suppression
fn map_user_properties_to_bytestr<S, O>(
    owned: &mut O,
    props: Vec<(String, String)>,
) -> Result<Vec<(mqtt_proto::ByteStr<S>, mqtt_proto::ByteStr<S>)>, buffer_pool::Error>
where
    S: buffer_pool::Shared,
    O: buffer_pool::Owned<Shared = S>,
{
    props
        .into_iter()
        .map(|(k, v)| {
            let k = mqtt_proto::ByteStr::new(owned, k)?;
            let v = mqtt_proto::ByteStr::new(owned, v)?;
            Ok((k, v))
        })
        .collect()
}

/// Trait for converting a packet to a buffer-backed internal variant.
// TODO: Should this be here, or will it make more sense to implement as functions in some kind of
// conversion module? Depends on where and how the boundary between public and internal types works
#[allow(dead_code)] // TODO: remove suppression
trait IntoBuffered<T, O>
where
    O: buffer_pool::Owned,
{
    fn into_buffered(self, owned: &mut O) -> Result<T, buffer_pool::Error>;
}

#[cfg(test)]
mod test {
    use crate::buffer_pool::{
        BufferPool as _,
        tests::{BufferPoolImpl, OwnedImpl, SharedImpl},
        //tests::SharedImpl,
    };
    use crate::mqtt_proto::{binary_data, byte_str, topic};
    use crate::packet::{self, IntoBuffered, PacketIdentifier};
    use crate::{mqtt_proto, topic};

    use paste::paste;

    fn compare_as_buffered<T, U>(packet: T, proto_packet: U)
    where
        T: IntoBuffered<U, OwnedImpl>,
        U: PartialEq + std::fmt::Debug,
    {
        let mut owned = BufferPoolImpl.take_empty_owned();
        let buffered = packet.into_buffered(&mut owned).unwrap();
        assert_eq!(buffered, proto_packet);
    }

    fn compare_as_unbuffered<T, U>(packet: T, proto_packet: U)
    where
        T: From<U> + PartialEq + std::fmt::Debug,
        U: PartialEq + std::fmt::Debug,
    {
        let unbuffered: T = proto_packet.into();
        assert_eq!(unbuffered, packet);
    }

    macro_rules! test_internal_to_public_conversion {
        ($( $test_name:ident, $public_packet:expr, $internal_packet:expr );* $(;)?) => {
            $(
                #[test]
                fn $test_name() {
                    compare_as_unbuffered($public_packet, $internal_packet)
                }
            )*
        };
    }

    macro_rules! test_bidirectional_conversion {
        ($( $test_name:ident, $public_packet:expr, $internal_packet:expr );* $(;)?) => {
            $(
                #[test]
                fn $test_name() {
                    compare_as_unbuffered($public_packet.clone(), $internal_packet.clone());
                    compare_as_buffered($public_packet, $internal_packet);
                }
            )*
        };
    }
    
    // Macro to define conversion tests for a packet
    // - internal to public conversion for the whole packet
    // - bidirectional conversion for the properties of the packet
    macro_rules! test_packet_conversions {
        ($( $packet_name:ident, $public_packet:expr, $internal_packet:expr );* $(;)?) => {
            $(
                paste! {
                    test_internal_to_public_conversion!(
                        [<$packet_name _to_public>],
                        $public_packet.clone(),
                        $internal_packet.clone()
                    );
                    test_bidirectional_conversion!(
                        [<$packet_name _properties_conversion>],
                        $public_packet.properties,
                        $internal_packet.other_properties
                    );
                }

            )*
        };
    }

    #[test]
    /// Validate that default values for property structures are the same on the public and internal types
    fn property_defaults() {
        // TODO: expand to include all defaultable types

        compare_as_buffered(
            packet::PublishProperties::default(),
            mqtt_proto::PublishOtherProperties::default(),
        );
        compare_as_buffered(
            packet::PubAckProperties::default(),
            mqtt_proto::PubAckOtherProperties::default(),
        );
        compare_as_buffered(
            packet::PubRecProperties::default(),
            mqtt_proto::PubRecOtherProperties::default(),
        );
        compare_as_buffered(
            packet::PubRelProperties::default(),
            mqtt_proto::PubRelOtherProperties::default(),
        );
        compare_as_buffered(
            packet::PubCompProperties::default(),
            mqtt_proto::PubCompOtherProperties::default(),
        );
    }

    test_packet_conversions!(
        publish,
        packet::Publish {
            payload: "payload".into(),
            qos: packet::DeliveryQoS::AtLeastOnce(packet::DeliveryInfo {
                dup: true,
                packet_identifier: PacketIdentifier::new(42).unwrap(),
            }),
            retain: true,
            topic_name: topic::TopicName::new("topic/name").unwrap(),
            properties: packet::PublishProperties {
                payload_format_indicator: packet::PayloadFormatIndicator::UTF8,
                message_expiry_interval: Some(3600),
                topic_alias: Some(1.try_into().unwrap()),
                response_topic: Some(topic::TopicName::new("response/topic").unwrap()),
                correlation_data: Some("correlation".into()),
                user_properties: vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ],
                subscription_identifiers: vec![1.try_into().unwrap(), 42.try_into().unwrap()],
                content_type: Some("content/type".to_string()),
            },
        },
        mqtt_proto::Publish {
            payload: SharedImpl::from_static(b"payload"),
            packet_identifier_dup_qos: mqtt_proto::PacketIdentifierDupQoS::AtLeastOnce(
                PacketIdentifier::new(42).unwrap(),
                true,
            ),
            retain: true,
            topic_name: topic("topic/name"),
            other_properties: mqtt_proto::PublishOtherProperties {
                payload_is_utf8: true,
                message_expiry_interval: Some(3600),
                topic_alias: Some(1.try_into().unwrap()),
                response_topic: Some(topic("response/topic")),
                correlation_data: Some(binary_data("correlation")),
                user_properties: vec![
                    (byte_str("key1"), byte_str("value1")),
                    (byte_str("key2"), byte_str("value2")),
                ],
                subscription_identifiers: vec![1.try_into().unwrap(), 42.try_into().unwrap()],
                content_type: Some(byte_str("content/type")),
            },
        }
    );


    test_packet_conversions!(
        puback,
        packet::PubAck {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reason: packet::PubAckReason::NotAuthorized,
            properties: packet::PubAckProperties {
                reason_string: Some("Not authorized".to_string()),
                user_properties: vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ],
            },
        },
        mqtt_proto::PubAck {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reason_code: mqtt_proto::PubAckReasonCode::NotAuthorized,
            other_properties: mqtt_proto::PubAckOtherProperties {
                reason_string: Some(byte_str("Not authorized")),
                user_properties: vec![
                    (byte_str("key1"), byte_str("value1")),
                    (byte_str("key2"), byte_str("value2")),
                ],
            },
        }
    );

    test_packet_conversions!(
        pubrec,
        packet::PubRec {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reason: packet::PubRecReason::NotAuthorized,
            properties: packet::PubRecProperties {
                reason_string: Some("Not authorized".to_string()),
                user_properties: vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ],
            },
        },
        mqtt_proto::PubRec {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reason_code: mqtt_proto::PubRecReasonCode::NotAuthorized,
            other_properties: mqtt_proto::PubRecOtherProperties {
                reason_string: Some(byte_str("Not authorized")),
                user_properties: vec![
                    (byte_str("key1"), byte_str("value1")),
                    (byte_str("key2"), byte_str("value2")),
                ],
            },
        }
    );

    test_packet_conversions!(
        pubrel,
        packet::PubRel {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reason: packet::PubRelReason::PacketIdentifierNotFound,
            properties: packet::PubRelProperties {
                reason_string: Some("Packet ID not found".to_string()),
                user_properties: vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ],
            },
        },
        mqtt_proto::PubRel {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reason_code: mqtt_proto::PubRelReasonCode::PacketIdentifierNotFound,
            other_properties: mqtt_proto::PubRelOtherProperties {
                reason_string: Some(byte_str("Packet ID not found")),
                user_properties: vec![
                    (byte_str("key1"), byte_str("value1")),
                    (byte_str("key2"), byte_str("value2")),
                ],
            },
        }
    );

    test_packet_conversions!(
        pubcomp,
        packet::PubComp {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reason: packet::PubCompReason::PacketIdentifierNotFound,
            properties: packet::PubCompProperties {
                reason_string: Some("Packet ID not found".to_string()),
                user_properties: vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ],
            },
        },
        mqtt_proto::PubComp {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reason_code: mqtt_proto::PubCompReasonCode::PacketIdentifierNotFound,
            other_properties: mqtt_proto::PubCompOtherProperties {
                reason_string: Some(byte_str("Packet ID not found")),
                user_properties: vec![
                    (byte_str("key1"), byte_str("value1")),
                    (byte_str("key2"), byte_str("value2")),
                ],
            },
        }
    );

}


