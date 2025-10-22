// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! MQTT packet types and associated properties and reason codes.

// TODO: This may not be necessary in it's entirety - this is a straight port of API proposal stubs.
// Remove items as necessary.

use std::fmt::Write as _;
use std::num::{NonZeroU16, NonZeroU32};

use bytes::Bytes;

use crate::error::OperationFailure;
// TODO: Replace instead of re-export?
pub use crate::mqtt_proto::{KeepAlive, PacketIdentifier, SessionExpiryInterval};
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

impl From<mqtt_proto::QoS> for QoS {
    fn from(value: mqtt_proto::QoS) -> QoS {
        match value {
            mqtt_proto::QoS::AtMostOnce => QoS::AtMostOnce,
            mqtt_proto::QoS::AtLeastOnce => QoS::AtLeastOnce,
            mqtt_proto::QoS::ExactlyOnce => QoS::ExactlyOnce,
        }
    }
}

impl From<QoS> for mqtt_proto::QoS {
    fn from(value: QoS) -> mqtt_proto::QoS {
        match value {
            QoS::AtMostOnce => mqtt_proto::QoS::AtMostOnce,
            QoS::AtLeastOnce => mqtt_proto::QoS::AtLeastOnce,
            QoS::ExactlyOnce => mqtt_proto::QoS::ExactlyOnce,
        }
    }
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

/// Information about extended authentication / reauthentication
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticationInfo {
    pub method: String,
    pub data: Option<Bytes>,
}

impl<S> From<mqtt_proto::Authentication<S>> for AuthenticationInfo
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::Authentication<S>) -> AuthenticationInfo {
        AuthenticationInfo {
            method: value.method.to_string(),
            data: value.data.map(|d| Bytes::copy_from_slice(d.as_ref())),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::Authentication<O::Shared>, O> for AuthenticationInfo
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::Authentication<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::Authentication {
            method: mqtt_proto::ByteStr::new(owned, self.method)?,
            data: self
                .data
                .map(|d| mqtt_proto::BinaryData::new(owned, d))
                .transpose()?,
        })
    }
}

//////////////////// Packets ////////////////////

/// CONNACK packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    pub session_present: bool,
    pub reason: ConnAckReason,
    pub properties: ConnAckProperties,
}

impl ConnAck {
    pub fn is_success(&self) -> bool {
        matches!(self.reason, ConnAckReason::Success)
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        if self.is_success() {
            Ok(())
        } else {
            let reason = if let Some(s) = &self.properties.reason_string {
                format!("{:?} - {s}", self.reason)
            } else {
                format!("{:?}", self.reason)
            };
            Err(OperationFailure { reason })
        }
    }
}

impl<S> From<mqtt_proto::ConnAck<S>> for ConnAck
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::ConnAck<S>) -> ConnAck {
        let (reason, session_present) = match value.reason_code {
            mqtt_proto::ConnectReasonCode::Success { session_present } => {
                (ConnAckReason::Success, session_present)
            }
            mqtt_proto::ConnectReasonCode::Refused(reason) => (reason.into(), false),
        };
        ConnAck {
            session_present,
            reason,
            properties: value.other_properties.into(),
        }
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

impl<S> From<mqtt_proto::PubComp<S>> for PubComp
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
    pub packet_identifier: PacketIdentifier,
    pub reasons: Vec<SubAckReason>,
    pub properties: SubAckProperties,
}

impl SubAck {
    pub fn is_success(&self) -> bool {
        self.reasons.iter().all(|r| {
            matches!(
                r,
                SubAckReason::GrantedQoS0 | SubAckReason::GrantedQoS1 | SubAckReason::GrantedQoS2
            )
        })
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        if self.is_success() {
            Ok(())
        } else {
            let mut s = String::new();
            for reason in &self.reasons {
                if !matches!(
                    reason,
                    SubAckReason::GrantedQoS0
                        | SubAckReason::GrantedQoS1
                        | SubAckReason::GrantedQoS2
                ) {
                    if !s.is_empty() {
                        s.push_str(", ");
                    }
                    let _ = write!(s, "{reason:?}");
                }
            }
            if let Some(reason_string) = &self.properties.reason_string {
                let _ = write!(s, " - {reason_string}");
            }
            Err(s.into())
        }
    }
}

impl<S> From<mqtt_proto::SubAck<S>> for SubAck
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::SubAck<S>) -> SubAck {
        SubAck {
            packet_identifier: value.packet_identifier,
            reasons: value.reason_codes.into_iter().map(Into::into).collect(),
            properties: value.other_properties.into(),
        }
    }
}

/// MQTT UNSUBACK
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAck {
    pub packet_identifier: PacketIdentifier,
    pub reasons: Vec<UnsubAckReason>,
    pub properties: UnsubAckProperties,
}

impl UnsubAck {
    pub fn is_success(&self) -> bool {
        self.reasons.iter().all(|r| {
            matches!(
                r,
                UnsubAckReason::Success | UnsubAckReason::NoSubscriptionExisted
            )
        })
    }

    pub fn as_result(&self) -> Result<(), OperationFailure> {
        if self.is_success() {
            Ok(())
        } else {
            let mut s = String::new();
            for reason in &self.reasons {
                if !matches!(
                    reason,
                    UnsubAckReason::Success | UnsubAckReason::NoSubscriptionExisted
                ) {
                    if !s.is_empty() {
                        s.push_str(", ");
                    }
                    let _ = write!(s, "{reason:?}");
                }
            }
            if let Some(reason_string) = &self.properties.reason_string {
                let _ = write!(s, " - {reason_string}");
            }
            Err(s.into())
        }
    }
}

impl<S> From<mqtt_proto::UnsubAck<S>> for UnsubAck
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::UnsubAck<S>) -> UnsubAck {
        UnsubAck {
            packet_identifier: value.packet_identifier,
            reasons: value.reason_codes.into_iter().map(Into::into).collect(),
            properties: value.other_properties.into(),
        }
    }
}

/// MQTT DISCONNECT
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disconnect {
    pub reason: DisconnectReason,
    pub properties: DisconnectProperties,
}

impl<S> From<mqtt_proto::Disconnect<S>> for Disconnect
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::Disconnect<S>) -> Disconnect {
        Disconnect {
            reason: value.reason_code.into(),
            properties: value.other_properties.into(),
        }
    }
}

/// MQTT AUTH
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auth {
    pub reason: AuthReason,
    pub authentication_info: Option<AuthenticationInfo>,
    pub properties: AuthProperties,
}

impl<S> From<mqtt_proto::Auth<S>> for Auth
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::Auth<S>) -> Auth {
        Auth {
            reason: value.reason_code.into(),
            authentication_info: value.authentication.map(Into::into),
            // NOTE: There is no concept in mqtt_proto of "AuthProperties" as it's a flat
            // structure, so unlike other packet definitions, we can't delegate to a separate
            // conversion trait.
            properties: AuthProperties {
                reason_string: value.reason_string.map(|s| s.to_string()),
                user_properties: value
                    .user_properties
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::Auth<O::Shared>, O> for Auth
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::Auth<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::Auth {
            reason_code: self.reason.into(),
            authentication: self
                .authentication_info
                .map(|a| a.into_buffered(owned))
                .transpose()?,
            reason_string: self
                .properties
                .reason_string
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            user_properties: map_user_properties_to_bytestr(
                owned,
                self.properties.user_properties,
            )?,
        })
    }
}

//////////////////// Properties ////////////////////

/// Properties for a CONNECT
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectProperties {
    pub session_expiry_interval: SessionExpiryInterval, //TODO: double-check default
    pub receive_maximum: NonZeroU16,
    pub maximum_packet_size: NonZeroU32,
    pub topic_alias_maximum: u16,
    pub request_response_information: bool,
    pub request_problem_information: bool,
    pub user_properties: Vec<(String, String)>,
    // NOTE: Authentication Method and Authentication Data are not included here, as they are part of the
    // separate `AuthenticationInfo` structure.
}

impl Default for ConnectProperties {
    fn default() -> Self {
        ConnectProperties {
            session_expiry_interval: SessionExpiryInterval::Duration(0),
            receive_maximum: NonZeroU16::MAX,
            maximum_packet_size: NonZeroU32::MAX,
            topic_alias_maximum: 0,
            request_response_information: false,
            request_problem_information: true,
            user_properties: Vec::new(),
        }
    }
}

impl<S> From<mqtt_proto::ConnectOtherProperties<S>> for ConnectProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::ConnectOtherProperties<S>) -> ConnectProperties {
        ConnectProperties {
            session_expiry_interval: value.session_expiry_interval,
            receive_maximum: value.receive_maximum,
            maximum_packet_size: value.maximum_packet_size,
            topic_alias_maximum: value.topic_alias_maximum,
            request_response_information: value.request_response_information,
            request_problem_information: value.request_problem_information,
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::ConnectOtherProperties<O::Shared>, O> for ConnectProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::ConnectOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::ConnectOtherProperties {
            session_expiry_interval: self.session_expiry_interval,
            receive_maximum: self.receive_maximum,
            maximum_packet_size: self.maximum_packet_size,
            topic_alias_maximum: self.topic_alias_maximum,
            request_response_information: self.request_response_information,
            request_problem_information: self.request_problem_information,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
            authentication: None, // TODO: Add auth support
        })
    }
}

/// Properties for a CONNACK
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ConnAckProperties {
    pub session_expiry_interval: Option<SessionExpiryInterval>,
    pub receive_maximum: NonZeroU16,
    pub maximum_qos: QoS,
    pub retain_available: bool,
    pub maximum_packet_size: NonZeroU32,
    pub assigned_client_identifier: Option<String>,
    pub topic_alias_maximum: u16,
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
    pub wildcard_subscription_available: bool,
    pub subscription_identifiers_available: bool,
    pub shared_subscription_available: bool,
    pub server_keep_alive: Option<KeepAlive>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    //pub authentication                    // TODO: Add auth support
}

impl Default for ConnAckProperties {
    fn default() -> Self {
        ConnAckProperties {
            session_expiry_interval: None,
            receive_maximum: NonZeroU16::MAX,
            maximum_qos: QoS::ExactlyOnce,
            retain_available: true,
            maximum_packet_size: NonZeroU32::MAX,
            assigned_client_identifier: None,
            topic_alias_maximum: 0,
            reason_string: None,
            user_properties: Vec::new(),
            wildcard_subscription_available: true,
            subscription_identifiers_available: true,
            shared_subscription_available: true,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
        }
    }
}

impl<S> From<mqtt_proto::ConnAckOtherProperties<S>> for ConnAckProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::ConnAckOtherProperties<S>) -> ConnAckProperties {
        ConnAckProperties {
            session_expiry_interval: value.session_expiry_interval,
            receive_maximum: value.receive_maximum,
            maximum_qos: value.maximum_qos.into(),
            retain_available: value.retain_available,
            maximum_packet_size: value.maximum_packet_size,
            assigned_client_identifier: value.assigned_client_id.map(|s| s.to_string()),
            topic_alias_maximum: value.topic_alias_maximum,
            reason_string: value.reason_string.map(|s| s.to_string()),
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            wildcard_subscription_available: value.wildcard_subscription_available,
            subscription_identifiers_available: value.subscription_identifiers_available,
            shared_subscription_available: value.shared_subscription_available,
            server_keep_alive: value.server_keep_alive,
            response_information: value.response_information.map(|s| s.to_string()),
            server_reference: value.server_reference.map(|s| s.to_string()),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::ConnAckOtherProperties<O::Shared>, O> for ConnAckProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::ConnAckOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::ConnAckOtherProperties {
            session_expiry_interval: self.session_expiry_interval,
            receive_maximum: self.receive_maximum,
            maximum_qos: self.maximum_qos.into(),
            retain_available: self.retain_available,
            maximum_packet_size: self.maximum_packet_size,
            assigned_client_id: self
                .assigned_client_identifier
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            topic_alias_maximum: self.topic_alias_maximum,
            reason_string: self
                .reason_string
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
            wildcard_subscription_available: self.wildcard_subscription_available,
            subscription_identifiers_available: self.subscription_identifiers_available,
            shared_subscription_available: self.shared_subscription_available,
            server_keep_alive: self.server_keep_alive,
            response_information: self
                .response_information
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            server_reference: self
                .server_reference
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            authentication: None, // TODO: Add auth support
        })
    }
}

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

/// Properties for a PUBCOMP
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

/// Properties for a SUBSCRIBE
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SubscribeProperties {
    pub subscription_identifier: Option<NonZeroU32>,
    pub user_properties: Vec<(String, String)>,
}

impl<S> From<mqtt_proto::SubscribeOtherProperties<S>> for SubscribeProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::SubscribeOtherProperties<S>) -> SubscribeProperties {
        SubscribeProperties {
            subscription_identifier: value.subscription_identifier,
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::SubscribeOtherProperties<O::Shared>, O> for SubscribeProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::SubscribeOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::SubscribeOtherProperties {
            subscription_identifier: self.subscription_identifier,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
        })
    }
}

/// Properties for a SUBACK
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl<S> From<mqtt_proto::SubAckOtherProperties<S>> for SubAckProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::SubAckOtherProperties<S>) -> SubAckProperties {
        SubAckProperties {
            reason_string: value.reason_string.map(|s| s.to_string()),
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::SubAckOtherProperties<O::Shared>, O> for SubAckProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::SubAckOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::SubAckOtherProperties {
            reason_string: self
                .reason_string
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
        })
    }
}

/// Properties for an UNSUBSCRIBE
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UnsubscribeProperties {
    pub user_properties: Vec<(String, String)>,
}

impl<S> From<mqtt_proto::UnsubscribeOtherProperties<S>> for UnsubscribeProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::UnsubscribeOtherProperties<S>) -> UnsubscribeProperties {
        UnsubscribeProperties {
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::UnsubscribeOtherProperties<O::Shared>, O> for UnsubscribeProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::UnsubscribeOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::UnsubscribeOtherProperties {
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
        })
    }
}

/// Properties for a UNSUBACK
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UnsubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

impl<S> From<mqtt_proto::UnsubAckOtherProperties<S>> for UnsubAckProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::UnsubAckOtherProperties<S>) -> UnsubAckProperties {
        UnsubAckProperties {
            reason_string: value.reason_string.map(|s| s.to_string()),
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::UnsubAckOtherProperties<O::Shared>, O> for UnsubAckProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::UnsubAckOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::UnsubAckOtherProperties {
            reason_string: self
                .reason_string
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
        })
    }
}

/// Properties for a DISCONNECT
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DisconnectProperties {
    pub session_expiry_interval: Option<SessionExpiryInterval>,
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
    pub server_reference: Option<String>,
}

impl<S> From<mqtt_proto::DisconnectOtherProperties<S>> for DisconnectProperties
where
    S: buffer_pool::Shared,
{
    fn from(value: mqtt_proto::DisconnectOtherProperties<S>) -> DisconnectProperties {
        DisconnectProperties {
            session_expiry_interval: value.session_expiry_interval,
            reason_string: value.reason_string.map(|s| s.to_string()),
            user_properties: value
                .user_properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            server_reference: value.server_reference.map(|s| s.to_string()),
        }
    }
}

impl<O> IntoBuffered<mqtt_proto::DisconnectOtherProperties<O::Shared>, O> for DisconnectProperties
where
    O: buffer_pool::Owned,
{
    fn into_buffered(
        self,
        owned: &mut O,
    ) -> Result<mqtt_proto::DisconnectOtherProperties<O::Shared>, buffer_pool::Error> {
        Ok(mqtt_proto::DisconnectOtherProperties {
            session_expiry_interval: self.session_expiry_interval,
            reason_string: self
                .reason_string
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
            user_properties: map_user_properties_to_bytestr(owned, self.user_properties)?,
            server_reference: self
                .server_reference
                .map(|s| mqtt_proto::ByteStr::new(owned, s))
                .transpose()?,
        })
    }
}

/// Properties for an AUTH
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AuthProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

// NOTE: there is no IntoBuffered implementation for AuthProperties because mqtt_proto::Auth does
// not have a dedicated properties struct equivalent

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

// NOTE: Unlike all other reason code enums, ConnAckReason cannot be converted back into an
// mqtt_proto equivalent, because there is a nesting of success vs. failure, so there is no valid
// target for Success 0x00
impl From<mqtt_proto::ConnectionRefusedReason> for ConnAckReason {
    fn from(value: mqtt_proto::ConnectionRefusedReason) -> ConnAckReason {
        match value {
            mqtt_proto::ConnectionRefusedReason::UnspecifiedError => {
                ConnAckReason::UnspecifiedError
            }
            mqtt_proto::ConnectionRefusedReason::MalformedPacket => ConnAckReason::MalformedPacket,
            mqtt_proto::ConnectionRefusedReason::ProtocolError => ConnAckReason::ProtocolError,
            mqtt_proto::ConnectionRefusedReason::ImplementationSpecificError => {
                ConnAckReason::ImplementationSpecificError
            }
            mqtt_proto::ConnectionRefusedReason::UnsupportedProtocolVersion => {
                ConnAckReason::UnsupportedProtocolVersion
            }
            mqtt_proto::ConnectionRefusedReason::ClientIdentifierNotValid => {
                ConnAckReason::ClientIdentifierNotValid
            }
            mqtt_proto::ConnectionRefusedReason::BadUserNameOrPassword => {
                ConnAckReason::BadUserNameOrPassword
            }
            mqtt_proto::ConnectionRefusedReason::NotAuthorized => ConnAckReason::NotAuthorized,
            mqtt_proto::ConnectionRefusedReason::ServerUnavailable => {
                ConnAckReason::ServerUnavailable
            }
            mqtt_proto::ConnectionRefusedReason::ServerBusy => ConnAckReason::ServerBusy,
            mqtt_proto::ConnectionRefusedReason::Banned => ConnAckReason::Banned,
            mqtt_proto::ConnectionRefusedReason::BadAuthenticationMethod => {
                ConnAckReason::BadAuthenticationMethod
            }
            mqtt_proto::ConnectionRefusedReason::TopicNameInvalid => {
                ConnAckReason::TopicNameInvalid
            }
            mqtt_proto::ConnectionRefusedReason::PacketTooLarge => ConnAckReason::PacketTooLarge,
            mqtt_proto::ConnectionRefusedReason::QuotaExceeded => ConnAckReason::QuotaExceeded,
            mqtt_proto::ConnectionRefusedReason::PayloadFormatInvalid => {
                ConnAckReason::PayloadFormatInvalid
            }
            mqtt_proto::ConnectionRefusedReason::RetainNotSupported => {
                ConnAckReason::RetainNotSupported
            }
            mqtt_proto::ConnectionRefusedReason::QoSNotSupported => ConnAckReason::QoSNotSupported,
            mqtt_proto::ConnectionRefusedReason::UseAnotherServer => {
                ConnAckReason::UseAnotherServer
            }
            mqtt_proto::ConnectionRefusedReason::ServerMoved => ConnAckReason::ServerMoved,
            mqtt_proto::ConnectionRefusedReason::ConnectionRateExceeded => {
                ConnAckReason::ConnectionRateExceeded
            }
        }
    }
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

impl From<PubRejectReason> for PubAckReason {
    fn from(value: PubRejectReason) -> PubAckReason {
        match value {
            PubRejectReason::UnspecifiedError => PubAckReason::UnspecifiedError,
            PubRejectReason::ImplementationSpecificError => {
                PubAckReason::ImplementationSpecificError
            }
            PubRejectReason::NotAuthorized => PubAckReason::NotAuthorized,
            PubRejectReason::TopicNameInvalid => PubAckReason::TopicNameInvalid,
            PubRejectReason::PacketIdentifierInUse => PubAckReason::PacketIdentifierInUse,
            PubRejectReason::QuotaExceeded => PubAckReason::QuotaExceeded,
            PubRejectReason::PayloadFormatInvalid => PubAckReason::PayloadFormatInvalid,
        }
    }
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

impl From<PubRejectReason> for PubRecReason {
    fn from(value: PubRejectReason) -> PubRecReason {
        match value {
            PubRejectReason::UnspecifiedError => PubRecReason::UnspecifiedError,
            PubRejectReason::ImplementationSpecificError => {
                PubRecReason::ImplementationSpecificError
            }
            PubRejectReason::NotAuthorized => PubRecReason::NotAuthorized,
            PubRejectReason::TopicNameInvalid => PubRecReason::TopicNameInvalid,
            PubRejectReason::PacketIdentifierInUse => PubRecReason::PacketIdentifierInUse,
            PubRejectReason::QuotaExceeded => PubRecReason::QuotaExceeded,
            PubRejectReason::PayloadFormatInvalid => PubRecReason::PayloadFormatInvalid,
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

impl From<mqtt_proto::SubscribeReasonCode> for SubAckReason {
    fn from(value: mqtt_proto::SubscribeReasonCode) -> SubAckReason {
        match value {
            mqtt_proto::SubscribeReasonCode::GrantedQoS0 => SubAckReason::GrantedQoS0,
            mqtt_proto::SubscribeReasonCode::GrantedQoS1 => SubAckReason::GrantedQoS1,
            mqtt_proto::SubscribeReasonCode::GrantedQoS2 => SubAckReason::GrantedQoS2,
            mqtt_proto::SubscribeReasonCode::UnspecifiedError => SubAckReason::UnspecifiedError,
            mqtt_proto::SubscribeReasonCode::ImplementationSpecificError => {
                SubAckReason::ImplementationSpecificError
            }
            mqtt_proto::SubscribeReasonCode::NotAuthorized => SubAckReason::NotAuthorized,
            mqtt_proto::SubscribeReasonCode::TopicFilterInvalid => SubAckReason::TopicFilterInvalid,
            mqtt_proto::SubscribeReasonCode::PacketIdentifierInUse => {
                SubAckReason::PacketIdentifierInUse
            }
            mqtt_proto::SubscribeReasonCode::QuotaExceeded => SubAckReason::QuotaExceeded,
            mqtt_proto::SubscribeReasonCode::SharedSubscriptionsNotSupported => {
                SubAckReason::SharedSubscriptionsNotSupported
            }
            mqtt_proto::SubscribeReasonCode::SubscriptionIdentifiersNotSupported => {
                SubAckReason::SubscriptionIdentifiersNotSupported
            }
            mqtt_proto::SubscribeReasonCode::WildcardSubscriptionsNotSupported => {
                SubAckReason::WildcardSubscriptionsNotSupported
            }
        }
    }
}

impl From<SubAckReason> for mqtt_proto::SubscribeReasonCode {
    fn from(value: SubAckReason) -> mqtt_proto::SubscribeReasonCode {
        match value {
            SubAckReason::GrantedQoS0 => mqtt_proto::SubscribeReasonCode::GrantedQoS0,
            SubAckReason::GrantedQoS1 => mqtt_proto::SubscribeReasonCode::GrantedQoS1,
            SubAckReason::GrantedQoS2 => mqtt_proto::SubscribeReasonCode::GrantedQoS2,
            SubAckReason::UnspecifiedError => mqtt_proto::SubscribeReasonCode::UnspecifiedError,
            SubAckReason::ImplementationSpecificError => {
                mqtt_proto::SubscribeReasonCode::ImplementationSpecificError
            }
            SubAckReason::NotAuthorized => mqtt_proto::SubscribeReasonCode::NotAuthorized,
            SubAckReason::TopicFilterInvalid => mqtt_proto::SubscribeReasonCode::TopicFilterInvalid,
            SubAckReason::PacketIdentifierInUse => {
                mqtt_proto::SubscribeReasonCode::PacketIdentifierInUse
            }
            SubAckReason::QuotaExceeded => mqtt_proto::SubscribeReasonCode::QuotaExceeded,
            SubAckReason::SharedSubscriptionsNotSupported => {
                mqtt_proto::SubscribeReasonCode::SharedSubscriptionsNotSupported
            }
            SubAckReason::SubscriptionIdentifiersNotSupported => {
                mqtt_proto::SubscribeReasonCode::SubscriptionIdentifiersNotSupported
            }
            SubAckReason::WildcardSubscriptionsNotSupported => {
                mqtt_proto::SubscribeReasonCode::WildcardSubscriptionsNotSupported
            }
        }
    }
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

impl From<mqtt_proto::UnsubAckReasonCode> for UnsubAckReason {
    fn from(value: mqtt_proto::UnsubAckReasonCode) -> UnsubAckReason {
        match value {
            mqtt_proto::UnsubAckReasonCode::Success => UnsubAckReason::Success,
            mqtt_proto::UnsubAckReasonCode::NoSubscriptionExisted => {
                UnsubAckReason::NoSubscriptionExisted
            }
            mqtt_proto::UnsubAckReasonCode::UnspecifiedError => UnsubAckReason::UnspecifiedError,
            mqtt_proto::UnsubAckReasonCode::ImplementationSpecificError => {
                UnsubAckReason::ImplementationSpecificError
            }
            mqtt_proto::UnsubAckReasonCode::NotAuthorized => UnsubAckReason::NotAuthorized,
            mqtt_proto::UnsubAckReasonCode::TopicFilterInvalid => {
                UnsubAckReason::TopicFilterInvalid
            }
            mqtt_proto::UnsubAckReasonCode::PacketIdentifierInUse => {
                UnsubAckReason::PacketIdentifierInUse
            }
        }
    }
}

impl From<UnsubAckReason> for mqtt_proto::UnsubAckReasonCode {
    fn from(value: UnsubAckReason) -> mqtt_proto::UnsubAckReasonCode {
        match value {
            UnsubAckReason::Success => mqtt_proto::UnsubAckReasonCode::Success,
            UnsubAckReason::NoSubscriptionExisted => {
                mqtt_proto::UnsubAckReasonCode::NoSubscriptionExisted
            }
            UnsubAckReason::UnspecifiedError => mqtt_proto::UnsubAckReasonCode::UnspecifiedError,
            UnsubAckReason::ImplementationSpecificError => {
                mqtt_proto::UnsubAckReasonCode::ImplementationSpecificError
            }
            UnsubAckReason::NotAuthorized => mqtt_proto::UnsubAckReasonCode::NotAuthorized,
            UnsubAckReason::TopicFilterInvalid => {
                mqtt_proto::UnsubAckReasonCode::TopicFilterInvalid
            }
            UnsubAckReason::PacketIdentifierInUse => {
                mqtt_proto::UnsubAckReasonCode::PacketIdentifierInUse
            }
        }
    }
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

impl From<mqtt_proto::DisconnectReasonCode> for DisconnectReason {
    fn from(value: mqtt_proto::DisconnectReasonCode) -> DisconnectReason {
        match value {
            mqtt_proto::DisconnectReasonCode::Normal => DisconnectReason::NormalDisconnection,
            mqtt_proto::DisconnectReasonCode::DisconnectWithWillMessage => {
                DisconnectReason::DisconnectWithWillMessage
            }
            mqtt_proto::DisconnectReasonCode::UnspecifiedError => {
                DisconnectReason::UnspecifiedError
            }
            mqtt_proto::DisconnectReasonCode::MalformedPacket => DisconnectReason::MalformedPacket,
            mqtt_proto::DisconnectReasonCode::ProtocolError => DisconnectReason::ProtocolError,
            mqtt_proto::DisconnectReasonCode::ImplementationSpecificError => {
                DisconnectReason::ImplementationSpecificError
            }
            mqtt_proto::DisconnectReasonCode::NotAuthorized => DisconnectReason::NotAuthorized,
            mqtt_proto::DisconnectReasonCode::ServerBusy => DisconnectReason::ServerBusy,
            mqtt_proto::DisconnectReasonCode::ServerShuttingDown => {
                DisconnectReason::ServerShuttingDown
            }
            mqtt_proto::DisconnectReasonCode::KeepAliveTimeout => {
                DisconnectReason::KeepAliveTimeout
            }
            mqtt_proto::DisconnectReasonCode::SessionTakenOver => {
                DisconnectReason::SessionTakenOver
            }
            mqtt_proto::DisconnectReasonCode::TopicFilterInvalid => {
                DisconnectReason::TopicFilterInvalid
            }
            mqtt_proto::DisconnectReasonCode::TopicNameInvalid => {
                DisconnectReason::TopicNameInvalid
            }
            mqtt_proto::DisconnectReasonCode::ReceiveMaximumExceeded => {
                DisconnectReason::ReceiveMaximumExceeded
            }
            mqtt_proto::DisconnectReasonCode::TopicAliasInvalid => {
                DisconnectReason::TopicAliasInvalid
            }
            mqtt_proto::DisconnectReasonCode::PacketTooLarge => DisconnectReason::PacketTooLarge,
            mqtt_proto::DisconnectReasonCode::MessageRateTooHigh => {
                DisconnectReason::MessageRateTooHigh
            }
            mqtt_proto::DisconnectReasonCode::QuotaExceeded => DisconnectReason::QuotaExceeded,
            mqtt_proto::DisconnectReasonCode::AdministrativeAction => {
                DisconnectReason::AdministrativeAction
            }
            mqtt_proto::DisconnectReasonCode::PayloadFormatInvalid => {
                DisconnectReason::PayloadFormatInvalid
            }
            mqtt_proto::DisconnectReasonCode::RetainNotSupported => {
                DisconnectReason::RetainNotSupported
            }
            mqtt_proto::DisconnectReasonCode::QosNotSupported => DisconnectReason::QoSNotSupported,
            mqtt_proto::DisconnectReasonCode::UseAnotherServer => {
                DisconnectReason::UseAnotherServer
            }
            mqtt_proto::DisconnectReasonCode::ServerMoved => DisconnectReason::ServerMoved,
            mqtt_proto::DisconnectReasonCode::SharedSubscriptionsNotSupported => {
                DisconnectReason::SharedSubscriptionsNotSupported
            }
            mqtt_proto::DisconnectReasonCode::ConnectionRateExceeded => {
                DisconnectReason::ConnectionRateExceeded
            }
            mqtt_proto::DisconnectReasonCode::MaximumConnectTime => {
                DisconnectReason::MaximumConnectTime
            }
            mqtt_proto::DisconnectReasonCode::SubscriptionIdentifiersNotSupported => {
                DisconnectReason::SubscriptionIdentifiersNotSupported
            }
            mqtt_proto::DisconnectReasonCode::WildcardSubscriptionsNotSupported => {
                DisconnectReason::WildcardSubscriptionsNotSupported
            }
        }
    }
}

impl From<DisconnectReason> for mqtt_proto::DisconnectReasonCode {
    fn from(value: DisconnectReason) -> mqtt_proto::DisconnectReasonCode {
        match value {
            DisconnectReason::NormalDisconnection => mqtt_proto::DisconnectReasonCode::Normal,
            DisconnectReason::DisconnectWithWillMessage => {
                mqtt_proto::DisconnectReasonCode::DisconnectWithWillMessage
            }
            DisconnectReason::UnspecifiedError => {
                mqtt_proto::DisconnectReasonCode::UnspecifiedError
            }
            DisconnectReason::MalformedPacket => mqtt_proto::DisconnectReasonCode::MalformedPacket,
            DisconnectReason::ProtocolError => mqtt_proto::DisconnectReasonCode::ProtocolError,
            DisconnectReason::ImplementationSpecificError => {
                mqtt_proto::DisconnectReasonCode::ImplementationSpecificError
            }
            DisconnectReason::NotAuthorized => mqtt_proto::DisconnectReasonCode::NotAuthorized,
            DisconnectReason::ServerBusy => mqtt_proto::DisconnectReasonCode::ServerBusy,
            DisconnectReason::ServerShuttingDown => {
                mqtt_proto::DisconnectReasonCode::ServerShuttingDown
            }
            DisconnectReason::KeepAliveTimeout => {
                mqtt_proto::DisconnectReasonCode::KeepAliveTimeout
            }
            DisconnectReason::SessionTakenOver => {
                mqtt_proto::DisconnectReasonCode::SessionTakenOver
            }
            DisconnectReason::TopicFilterInvalid => {
                mqtt_proto::DisconnectReasonCode::TopicFilterInvalid
            }
            DisconnectReason::TopicNameInvalid => {
                mqtt_proto::DisconnectReasonCode::TopicNameInvalid
            }
            DisconnectReason::ReceiveMaximumExceeded => {
                mqtt_proto::DisconnectReasonCode::ReceiveMaximumExceeded
            }
            DisconnectReason::TopicAliasInvalid => {
                mqtt_proto::DisconnectReasonCode::TopicAliasInvalid
            }
            DisconnectReason::PacketTooLarge => mqtt_proto::DisconnectReasonCode::PacketTooLarge,
            DisconnectReason::MessageRateTooHigh => {
                mqtt_proto::DisconnectReasonCode::MessageRateTooHigh
            }
            DisconnectReason::QuotaExceeded => mqtt_proto::DisconnectReasonCode::QuotaExceeded,
            DisconnectReason::AdministrativeAction => {
                mqtt_proto::DisconnectReasonCode::AdministrativeAction
            }
            DisconnectReason::PayloadFormatInvalid => {
                mqtt_proto::DisconnectReasonCode::PayloadFormatInvalid
            }
            DisconnectReason::RetainNotSupported => {
                mqtt_proto::DisconnectReasonCode::RetainNotSupported
            }
            DisconnectReason::QoSNotSupported => mqtt_proto::DisconnectReasonCode::QosNotSupported,
            DisconnectReason::UseAnotherServer => {
                mqtt_proto::DisconnectReasonCode::UseAnotherServer
            }
            DisconnectReason::ServerMoved => mqtt_proto::DisconnectReasonCode::ServerMoved,
            DisconnectReason::SharedSubscriptionsNotSupported => {
                mqtt_proto::DisconnectReasonCode::SharedSubscriptionsNotSupported
            }
            DisconnectReason::ConnectionRateExceeded => {
                mqtt_proto::DisconnectReasonCode::ConnectionRateExceeded
            }
            DisconnectReason::MaximumConnectTime => {
                mqtt_proto::DisconnectReasonCode::MaximumConnectTime
            }
            DisconnectReason::SubscriptionIdentifiersNotSupported => {
                mqtt_proto::DisconnectReasonCode::SubscriptionIdentifiersNotSupported
            }
            DisconnectReason::WildcardSubscriptionsNotSupported => {
                mqtt_proto::DisconnectReasonCode::WildcardSubscriptionsNotSupported
            }
        }
    }
}

/// Reason code for an AUTH
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthReason {
    Success = 0x00,
    ContinueAuthentication = 0x18,
    Reauthenticate = 0x19,
}

impl From<mqtt_proto::AuthenticateReasonCode> for AuthReason {
    fn from(value: mqtt_proto::AuthenticateReasonCode) -> AuthReason {
        match value {
            mqtt_proto::AuthenticateReasonCode::Success => AuthReason::Success,
            mqtt_proto::AuthenticateReasonCode::ContinueAuthentication => {
                AuthReason::ContinueAuthentication
            }
            mqtt_proto::AuthenticateReasonCode::ReAuthenticate => AuthReason::Reauthenticate,
        }
    }
}

impl From<AuthReason> for mqtt_proto::AuthenticateReasonCode {
    fn from(value: AuthReason) -> mqtt_proto::AuthenticateReasonCode {
        match value {
            AuthReason::Success => mqtt_proto::AuthenticateReasonCode::Success,
            AuthReason::ContinueAuthentication => {
                mqtt_proto::AuthenticateReasonCode::ContinueAuthentication
            }
            AuthReason::Reauthenticate => mqtt_proto::AuthenticateReasonCode::ReAuthenticate,
        }
    }
}

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
pub(crate) trait IntoBuffered<T, O>
where
    O: buffer_pool::Owned,
{
    fn into_buffered(self, owned: &mut O) -> Result<T, buffer_pool::Error>;
}

impl<O> IntoBuffered<O::Shared, O> for Bytes
where
    O: buffer_pool::Owned,
{
    fn into_buffered(self, owned: &mut O) -> Result<O::Shared, buffer_pool::Error> {
        let len = self.len();

        owned.reserve(len)?;

        // SAFETY: Requirements of `unfilled_mut` and `fill` are upheld.
        unsafe {
            let buf = owned.unfilled_mut();
            crate::buffer_pool::maybe_uninit_copy_from_slice(&mut buf[..len], &self);
            owned.fill(len);
        }

        let filled_len = owned.filled_len();
        let shared = owned.split_to(filled_len).freeze();
        Ok(shared)
    }
}

#[cfg(test)]
mod test {
    use crate::mqtt_proto::{binary_data, byte_str, topic};
    use crate::packet::{self, IntoBuffered, PacketIdentifier, SessionExpiryInterval};
    use crate::{
        buffer_pool::{
            BufferPool as _,
            tests::{BufferPoolImpl, OwnedImpl, SharedImpl},
        },
        packet::KeepAlive,
    };
    use crate::{mqtt_proto, topic};
    use std::num::{NonZeroU16, NonZeroU32};

    use paste::paste;

    #[allow(clippy::needless_pass_by_value)]
    fn compare_as_buffered<T, U>(packet: T, proto_packet: U)
    where
        T: IntoBuffered<U, OwnedImpl>,
        U: PartialEq + std::fmt::Debug,
    {
        let mut owned = BufferPoolImpl.take_empty_owned();
        let buffered = packet.into_buffered(&mut owned).unwrap();
        assert_eq!(buffered, proto_packet);
    }

    #[allow(clippy::needless_pass_by_value)]
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
    macro_rules! test_packet_and_property_conversions {
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

    macro_rules! test_property_conversions {
        ($( $properties_name:ident, $public_properties:expr, $internal_properties:expr );* $(;)?) => {
            $(
                paste! {
                    test_bidirectional_conversion!(
                        [<$properties_name _properties_conversion>],
                        $public_properties,
                        $internal_properties
                    );
                }

            )*
        };
    }

    #[test]
    /// Validate that default values for property structures are the same on the public and internal types
    fn property_defaults() {
        compare_as_buffered(
            packet::ConnectProperties::default(),
            mqtt_proto::ConnectOtherProperties::default(),
        );
        compare_as_buffered(
            packet::ConnAckProperties::default(),
            mqtt_proto::ConnAckOtherProperties::default(),
        );
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
        compare_as_buffered(
            packet::SubscribeProperties::default(),
            mqtt_proto::SubscribeOtherProperties::default(),
        );
        compare_as_buffered(
            packet::SubAckProperties::default(),
            mqtt_proto::SubAckOtherProperties::default(),
        );
        compare_as_buffered(
            packet::UnsubscribeProperties::default(),
            mqtt_proto::UnsubscribeOtherProperties::default(),
        );
        compare_as_buffered(
            packet::UnsubAckProperties::default(),
            mqtt_proto::UnsubAckOtherProperties::default(),
        );
        compare_as_buffered(
            packet::DisconnectProperties::default(),
            mqtt_proto::DisconnectOtherProperties::default(),
        );
    }

    test_property_conversions!(
        connect,
        packet::ConnectProperties {
            session_expiry_interval: SessionExpiryInterval::Duration(3600),
            receive_maximum: NonZeroU16::new(100).unwrap(),
            maximum_packet_size: NonZeroU32::new(1024).unwrap(),
            topic_alias_maximum: 10,
            request_response_information: true,
            request_problem_information: false,
            user_properties: vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ],
        },
        mqtt_proto::ConnectOtherProperties {
            session_expiry_interval: SessionExpiryInterval::Duration(3600),
            receive_maximum: NonZeroU16::new(100).unwrap(),
            maximum_packet_size: NonZeroU32::new(1024).unwrap(),
            topic_alias_maximum: 10,
            request_response_information: true,
            request_problem_information: false,
            user_properties: vec![
                (byte_str("key1"), byte_str("value1")),
                (byte_str("key2"), byte_str("value2")),
            ],
            authentication: None, // TODO: add support
        }
    );

    test_packet_and_property_conversions!(
        connack,
        packet::ConnAck {
            session_present: true,
            reason: packet::ConnAckReason::Success,
            properties: packet::ConnAckProperties {
                session_expiry_interval: Some(SessionExpiryInterval::Duration(3600)),
                receive_maximum: NonZeroU16::new(100).unwrap(),
                maximum_qos: packet::QoS::AtLeastOnce,
                retain_available: true,
                maximum_packet_size: NonZeroU32::new(1024).unwrap(),
                assigned_client_identifier: Some("client_id".to_string()),
                topic_alias_maximum: 10,
                reason_string: Some("Not authorized".to_string()),
                user_properties: vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ],
                wildcard_subscription_available: true,
                subscription_identifiers_available: true,
                shared_subscription_available: false,
                server_keep_alive: Some(KeepAlive::Duration(NonZeroU16::new(30).unwrap())),
                response_information: Some("response info".to_string()),
                server_reference: Some("server ref".to_string()),
            },
        },
        mqtt_proto::ConnAck {
            reason_code: mqtt_proto::ConnectReasonCode::Success {
                session_present: true
            },
            other_properties: mqtt_proto::ConnAckOtherProperties {
                session_expiry_interval: Some(SessionExpiryInterval::Duration(3600)),
                receive_maximum: NonZeroU16::new(100).unwrap(),
                maximum_qos: mqtt_proto::QoS::AtLeastOnce,
                retain_available: true,
                maximum_packet_size: NonZeroU32::new(1024).unwrap(),
                assigned_client_id: Some(byte_str("client_id")),
                topic_alias_maximum: 10,
                reason_string: Some(byte_str("Not authorized")),
                user_properties: vec![
                    (byte_str("key1"), byte_str("value1")),
                    (byte_str("key2"), byte_str("value2")),
                ],
                wildcard_subscription_available: true,
                subscription_identifiers_available: true,
                shared_subscription_available: false,
                server_keep_alive: Some(KeepAlive::Duration(NonZeroU16::new(30).unwrap())),
                response_information: Some(byte_str("response info")),
                server_reference: Some(byte_str("server ref")),
                authentication: None, // TODO: add support
            },
        }
    );

    test_packet_and_property_conversions!(
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

    test_packet_and_property_conversions!(
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

    test_packet_and_property_conversions!(
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

    test_packet_and_property_conversions!(
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

    test_packet_and_property_conversions!(
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

    test_property_conversions!(
        subscribe,
        packet::SubscribeProperties {
            subscription_identifier: Some(42.try_into().unwrap()),
            user_properties: vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ],
        },
        mqtt_proto::SubscribeOtherProperties {
            subscription_identifier: Some(42.try_into().unwrap()),
            user_properties: vec![
                (byte_str("key1"), byte_str("value1")),
                (byte_str("key2"), byte_str("value2")),
            ],
        }
    );

    test_packet_and_property_conversions!(
        suback,
        packet::SubAck {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reasons: vec![
                packet::SubAckReason::GrantedQoS0,
                packet::SubAckReason::NotAuthorized,
            ],
            properties: packet::SubAckProperties {
                reason_string: Some("Not authorized".to_string()),
                user_properties: vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ],
            },
        },
        mqtt_proto::SubAck {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reason_codes: vec![
                mqtt_proto::SubscribeReasonCode::GrantedQoS0,
                mqtt_proto::SubscribeReasonCode::NotAuthorized,
            ],
            other_properties: mqtt_proto::SubAckOtherProperties {
                reason_string: Some(byte_str("Not authorized")),
                user_properties: vec![
                    (byte_str("key1"), byte_str("value1")),
                    (byte_str("key2"), byte_str("value2")),
                ],
            },
        }
    );

    test_property_conversions!(
        unsubscribe,
        packet::UnsubscribeProperties {
            user_properties: vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ],
        },
        mqtt_proto::UnsubscribeOtherProperties {
            user_properties: vec![
                (byte_str("key1"), byte_str("value1")),
                (byte_str("key2"), byte_str("value2")),
            ],
        }
    );

    test_packet_and_property_conversions!(
        unsuback,
        packet::UnsubAck {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reasons: vec![
                packet::UnsubAckReason::Success,
                packet::UnsubAckReason::NotAuthorized,
            ],
            properties: packet::UnsubAckProperties {
                reason_string: Some("Not authorized".to_string()),
                user_properties: vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ],
            },
        },
        mqtt_proto::UnsubAck {
            packet_identifier: PacketIdentifier::new(42).unwrap(),
            reason_codes: vec![
                mqtt_proto::UnsubAckReasonCode::Success,
                mqtt_proto::UnsubAckReasonCode::NotAuthorized,
            ],
            other_properties: mqtt_proto::UnsubAckOtherProperties {
                reason_string: Some(byte_str("Not authorized")),
                user_properties: vec![
                    (byte_str("key1"), byte_str("value1")),
                    (byte_str("key2"), byte_str("value2")),
                ],
            },
        }
    );

    test_packet_and_property_conversions!(
        disconnect,
        packet::Disconnect {
            reason: packet::DisconnectReason::NormalDisconnection,
            properties: packet::DisconnectProperties {
                session_expiry_interval: Some(packet::SessionExpiryInterval::Duration(3600)),
                reason_string: Some("Normal disconnection".to_string()),
                user_properties: vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ],
                server_reference: Some("server/ref".to_string()),
            },
        },
        mqtt_proto::Disconnect {
            reason_code: mqtt_proto::DisconnectReasonCode::Normal,
            other_properties: mqtt_proto::DisconnectOtherProperties {
                session_expiry_interval: Some(packet::SessionExpiryInterval::Duration(3600)),
                reason_string: Some(byte_str("Normal disconnection")),
                user_properties: vec![
                    (byte_str("key1"), byte_str("value1")),
                    (byte_str("key2"), byte_str("value2")),
                ],
                server_reference: Some(byte_str("server/ref")),
            }
        }
    );

    test_bidirectional_conversion!(
        auth_conversion,
        packet::Auth {
            reason: packet::AuthReason::ContinueAuthentication,
            authentication_info: Some(packet::AuthenticationInfo {
                method: "authmethod".to_string(),
                data: Some("authdata".into()),
            }),
            properties: packet::AuthProperties {
                reason_string: Some("Continue authentication".to_string()),
                user_properties: vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ],
            },
        },
        mqtt_proto::Auth {
            reason_code: mqtt_proto::AuthenticateReasonCode::ContinueAuthentication,
            authentication: Some(mqtt_proto::Authentication {
                method: byte_str("authmethod"),
                data: Some(binary_data("authdata")),
            }),
            reason_string: Some(byte_str("Continue authentication")),
            user_properties: vec![
                (byte_str("key1"), byte_str("value1")),
                (byte_str("key2"), byte_str("value2")),
            ],
        }
    );
}
