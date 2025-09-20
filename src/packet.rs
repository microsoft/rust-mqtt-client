// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! MQTT packet types and associated properties and reason codes.

// TODO: Remove when possible.
#![allow(clippy::derivable_impls)]

// TODO: This may not be necessary in it's entirety - this is a straight port of API proposal stubs.
// Remove items as necessary.

use bytes::Bytes;

use crate::error::OperationFailure;
pub use crate::mqtt_proto::PacketIdentifier;
use crate::topic::TopicName;

#[derive(Debug, Clone)]
pub struct Publish {
    pub topic_name: TopicName,
    pub payload: Bytes,
    pub qos: QoS,
    pub properties: PublishProperties,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

#[derive(Debug, Clone)]
pub struct ConnectProperties {}
impl Default for ConnectProperties {
    fn default() -> Self {
        ConnectProperties {}
    }
}

#[derive(Debug, Clone)]
pub struct DisconnectProperties {}
impl Default for DisconnectProperties {
    fn default() -> Self {
        DisconnectProperties {}
    }
}

#[derive(Debug, Clone)]
pub struct PublishProperties {}
impl Default for PublishProperties {
    fn default() -> Self {
        PublishProperties {}
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
pub struct AckProperties {}
impl Default for AckProperties {
    fn default() -> Self {
        AckProperties {}
    }
}

// NOTE: These are aliased for clarity on specific packet types
pub type PubAckProperties = AckProperties; // For QoS 1
pub type PubRecProperties = AckProperties; // For QoS 2

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

#[derive(Debug, Clone)]
pub struct PubAck {
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

#[derive(Debug, Clone)]
pub struct PubRec {
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

// TODO: make this internal only
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubAckRecReason {
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

// NOTE: strict subset of PubAckReason/PubRecReason
// TODO: Massage naming
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

// NOTE: We implement Into instead of From here because there isn't a non-failable conversion
// the other way, as `PubRejectReason` is a strict subset of `PubAckRecReason`.
#[allow(clippy::from_over_into)]
impl Into<PubAckRecReason> for PubRejectReason {
    fn into(self) -> PubAckRecReason {
        match self {
            PubRejectReason::UnspecifiedError => PubAckRecReason::UnspecifiedError,
            PubRejectReason::ImplementationSpecificError => {
                PubAckRecReason::ImplementationSpecificError
            }
            PubRejectReason::NotAuthorized => PubAckRecReason::NotAuthorized,
            PubRejectReason::TopicNameInvalid => PubAckRecReason::TopicNameInvalid,
            PubRejectReason::PacketIdentifierInUse => PubAckRecReason::PacketIdentifierInUse,
            PubRejectReason::QuotaExceeded => PubAckRecReason::QuotaExceeded,
            PubRejectReason::PayloadFormatInvalid => PubAckRecReason::PayloadFormatInvalid,
        }
    }
}

// NOTE: These are aliased for clarity on specific packet types
pub type PubAckReason = PubAckRecReason; // For QoS 1
pub type PubRecReason = PubAckRecReason; // For QoS 2

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
