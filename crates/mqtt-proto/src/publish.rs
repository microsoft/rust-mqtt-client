// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::num::NonZeroU16;

use derive_where::derive_where;

use buffer_pool::{BytesAccumulator, Owned, Shared};

use crate::{
    ByteStr, CorrelationData, DecodeError, EncodeError, PacketIdentifier, PacketIdentifierDupQoS,
    PacketMeta, Property, PropertyRef, ProtocolVersion, PubAck, PubAckOtherProperties,
    PubAckReasonCode, PublicationOtherProperties, SharedExt as _, Topic, UserProperties,
};

/// 3.3 PUBLISH – Publish message
#[derive(Clone, Debug)]
#[derive_where(Eq, PartialEq)]
pub struct Publish<S>
where
    S: Shared,
{
    pub topic_name: Topic<ByteStr<S>>,
    pub packet_identifier_dup_qos: PacketIdentifierDupQoS,
    pub retain: bool,
    pub payload: S,
    pub other_properties: PublishOtherProperties<S>,
}

#[derive(Clone, Debug)]
#[derive_where(Default, Eq, PartialEq)]
pub struct PublishOtherProperties<S>
where
    S: Shared,
{
    pub payload_is_utf8: bool,
    pub message_expiry_interval: Option<u32>,
    pub topic_alias: Option<NonZeroU16>,
    pub response_topic: Option<Topic<ByteStr<S>>>,
    pub correlation_data: Option<CorrelationData<S>>,
    pub user_properties: UserProperties<S>,
    pub subscription_identifiers: Vec<u32>,
    pub content_type: Option<ByteStr<S>>,
}

impl<S> Publish<S>
where
    S: Shared,
{
    pub fn qos0(topic: Topic<ByteStr<S>>, payload: S) -> Self {
        Publish {
            topic_name: topic,
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtMostOnce,
            retain: false,
            payload,
            other_properties: Default::default(),
        }
    }

    pub fn qos1(
        topic: Topic<ByteStr<S>>,
        packet_identifier: u16,
        duplicate: bool,
        payload: S,
        retain: bool,
    ) -> Self {
        Publish {
            topic_name: topic,
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                PacketIdentifier::new(packet_identifier).expect("packet identifier == 0"),
                duplicate,
            ),
            retain,
            payload,
            other_properties: Default::default(),
        }
    }

    pub fn packet_id(&self) -> Option<PacketIdentifier> {
        match self.packet_identifier_dup_qos {
            PacketIdentifierDupQoS::ExactlyOnce(id, _)
            | PacketIdentifierDupQoS::AtLeastOnce(id, _) => Some(id),
            PacketIdentifierDupQoS::AtMostOnce => None,
        }
    }

    pub fn payload(&self) -> &S {
        &self.payload
    }

    /// Sets the property to the given value.
    /// Note that if there were several properties with the same name,
    /// they all will be removed and replaced with the a single new one.
    #[inline]
    pub fn set_property(&mut self, property: (ByteStr<S>, ByteStr<S>)) {
        self.other_properties
            .user_properties
            .retain(|(k, _v)| k != &property.0);

        self.other_properties.user_properties.push(property);
    }

    /// Returns the first property with the given name.
    /// Note that the protocol allows multiple props with the same name.
    #[inline]
    pub fn property(&self, prop: impl AsRef<str>) -> Option<&ByteStr<S>> {
        self.properties(prop).next()
    }

    /// Returns an iterator over all properties with the given name.
    /// Note that the protocol allows multiple props with the same name.
    #[inline]
    pub fn properties(&self, prop: impl AsRef<str>) -> impl Iterator<Item = &ByteStr<S>> {
        self.other_properties
            .user_properties
            .iter()
            .filter_map(move |(k, val)| {
                if k.as_ref() == prop.as_ref() {
                    Some(val)
                } else {
                    None
                }
            })
    }

    #[inline]
    pub fn response_topic(&self) -> Option<&Topic<ByteStr<S>>> {
        self.other_properties.response_topic.as_ref()
    }

    #[inline]
    pub fn correlation_data(&self) -> Option<&CorrelationData<S>> {
        self.other_properties.correlation_data.as_ref()
    }

    pub fn with_correlation_data(mut self, correlation_data: CorrelationData<S>) -> Self {
        self.other_properties.correlation_data = Some(correlation_data);
        self
    }

    pub fn with_user_properties(
        mut self,
        user_properties: impl IntoIterator<Item = (ByteStr<S>, ByteStr<S>)>,
    ) -> Self {
        self.other_properties
            .user_properties
            .extend(user_properties);
        self
    }

    pub fn with_response_topic(mut self, response_topic: Topic<ByteStr<S>>) -> Self {
        self.other_properties.response_topic = Some(response_topic);
        self
    }

    pub fn ack(&self) -> Option<PubAck<S>> {
        self.ack_with_reason(PubAckReasonCode::Success)
    }

    pub fn ack_unauthorized(&self) -> Option<PubAck<S>> {
        self.ack_with_reason(PubAckReasonCode::NotAuthorized)
    }

    pub fn ack_with_reason(&self, ack_reason_code: PubAckReasonCode) -> Option<PubAck<S>> {
        self.ack_with_reason_string(ack_reason_code, None)
    }

    pub fn ack_with_reason_string(
        &self,
        ack_reason_code: PubAckReasonCode,
        ack_reason_string: Option<ByteStr<S>>,
    ) -> Option<PubAck<S>> {
        match self.packet_identifier_dup_qos {
            PacketIdentifierDupQoS::AtMostOnce => None,
            PacketIdentifierDupQoS::AtLeastOnce(id, _) => Some(PubAck {
                packet_identifier: id,
                reason_code: ack_reason_code,
                other_properties: PubAckOtherProperties {
                    reason_string: ack_reason_string,
                    ..Default::default()
                },
            }),
            PacketIdentifierDupQoS::ExactlyOnce(_, _) => {
                unreachable!("QoS 2 is not supported yet. This codepath should be unreachable.")
            }
        }
    }

    /// Creates a copy of this `Publish` with another [`Shared`] type as the backing buffer.
    pub fn to_shared<O2>(&self, owned: &mut O2) -> Result<Publish<O2::Shared>, buffer_pool::Error>
    where
        O2: Owned,
    {
        let payload = self.payload.copy_to_shared(owned)?;

        Ok(Publish {
            topic_name: self.topic_name.to_shared(owned)?,
            packet_identifier_dup_qos: self.packet_identifier_dup_qos,
            retain: self.retain,
            payload,
            other_properties: self.other_properties.to_shared(owned)?,
        })
    }
}

impl<S> From<PublicationOtherProperties<S>> for PublishOtherProperties<S>
where
    S: Shared,
{
    fn from(props: PublicationOtherProperties<S>) -> Self {
        Self {
            payload_is_utf8: props.payload_is_utf8,
            message_expiry_interval: props.message_expiry_interval,
            response_topic: props.response_topic,
            correlation_data: props.correlation_data,
            user_properties: props.user_properties,
            content_type: props.content_type,
            ..Default::default()
        }
    }
}

impl<S> PacketMeta<S> for Publish<S>
where
    S: Shared,
{
    const PACKET_TYPE: u8 = 0x30;

    fn decode<const RLFML: usize>(
        flags: u8,
        src: &mut S,
        version: ProtocolVersion,
    ) -> Result<Self, DecodeError> {
        let dup = (flags & 0b0000_1000) != 0;
        let retain = (flags & 0b0000_0001) != 0;

        let topic_name = Topic::decode(src)?.ok_or(DecodeError::IncompletePacket)?;

        let packet_identifier_dup_qos = match (flags & 0b0000_0110) >> 1 {
            0x00 if dup => return Err(DecodeError::PublishDupAtMostOnce),

            0x00 => PacketIdentifierDupQoS::AtMostOnce,

            0x01 => {
                let packet_identifier = src.try_get_packet_identifier()?;
                PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, dup)
            }

            0x02 => {
                let packet_identifier = src.try_get_packet_identifier()?;
                PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, dup)
            }

            qos => return Err(DecodeError::UnrecognizedQoS(qos)),
        };

        match version {
            ProtocolVersion::V3 => {
                let payload = src.split_to(src.len());

                Ok(Self {
                    topic_name,
                    packet_identifier_dup_qos,
                    retain,
                    payload,
                    other_properties: Default::default(),
                })
            }

            ProtocolVersion::V5 => {
                decode_properties!(
                    src,
                    payload_is_utf8: PayloadIsUtf8,
                    message_expiry_interval: MessageExpiryInterval,
                    topic_alias: TopicAlias,
                    response_topic: ResponseTopic,
                    correlation_data: CorrelationData,
                    user_properties: Vec<UserProperty>,
                    subscription_identifiers: Vec<SubscriptionIdentifier>,
                    content_type: ContentType,
                );

                let payload = src.split_to(src.len());

                Ok(Self {
                    topic_name,
                    packet_identifier_dup_qos,
                    retain,
                    payload,
                    other_properties: PublishOtherProperties {
                        payload_is_utf8: payload_is_utf8.unwrap_or_default(),
                        message_expiry_interval,
                        topic_alias,
                        response_topic,
                        correlation_data,
                        user_properties,
                        subscription_identifiers,
                        content_type,
                    },
                })
            }
        }
    }

    fn encode<B, const RLFML: usize>(
        &self,
        dst: &mut B,
        version: ProtocolVersion,
    ) -> Result<(), EncodeError>
    where
        B: BytesAccumulator<Shared = S>,
    {
        let Self {
            topic_name,
            packet_identifier_dup_qos,
            retain: _,
            payload,
            other_properties,
        } = self;

        topic_name.encode(dst)?;

        match packet_identifier_dup_qos {
            PacketIdentifierDupQoS::AtMostOnce => (),
            PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, _)
            | PacketIdentifierDupQoS::ExactlyOnce(packet_identifier, _) => {
                dst.try_put_u16_be(packet_identifier.0.get())
                    .ok_or(EncodeError::InsufficientBuffer)?;
            }
        }

        if version.is_v5() {
            let PublishOtherProperties {
                payload_is_utf8,
                message_expiry_interval,
                topic_alias,
                response_topic,
                correlation_data,
                user_properties,
                subscription_identifiers,
                content_type,
            } = other_properties;

            encode_properties! {
                dst,
                payload_is_utf8: PayloadIsUtf8,
                message_expiry_interval: Option<MessageExpiryInterval>,
                topic_alias: Option<TopicAlias>,
                response_topic: Option<ResponseTopic>,
                correlation_data: Option<CorrelationData>,
                user_properties: Vec<UserProperty>,
                subscription_identifiers: Vec<SubscriptionIdentifier>,
                content_type: Option<ContentType>,
            }
        }

        dst.put_shared(payload.clone());

        Ok(())
    }
}

impl<S> PublishOtherProperties<S>
where
    S: Shared,
{
    /// Creates a copy of this `PublishOtherProperties` with another [`Shared`] type as the backing buffer.
    pub fn to_shared<O2>(
        &self,
        owned: &mut O2,
    ) -> Result<PublishOtherProperties<O2::Shared>, buffer_pool::Error>
    where
        O2: Owned,
    {
        let response_topic = match &self.response_topic {
            Some(value) => Some(value.to_shared(owned)?),
            None => None,
        };

        let correlation_data = match &self.correlation_data {
            Some(value) => Some(value.to_shared(owned)?),
            None => None,
        };

        let mut user_properties = Vec::with_capacity(self.user_properties.len());
        for (key, val) in &self.user_properties {
            let key = key.to_shared(owned)?;
            let val = val.to_shared(owned)?;
            user_properties.push((key, val));
        }

        let content_type = match &self.content_type {
            Some(value) => Some(value.to_shared(owned)?),
            None => None,
        };

        Ok(PublishOtherProperties {
            payload_is_utf8: self.payload_is_utf8,
            message_expiry_interval: self.message_expiry_interval,
            topic_alias: self.topic_alias,
            response_topic,
            correlation_data,
            user_properties,
            subscription_identifiers: self.subscription_identifiers.clone(),
            content_type,
        })
    }
}

#[cfg(all(test, feature = "tests"))]
mod tests {
    use buffer_pool::{
        BufferSource as _,
        tests::{BufferPoolImpl, SharedImpl},
    };

    use super::*;
    use crate::{Packet, UserProperty, byte_str, correlation_data, topic};

    encode_decode_v3! {
        Packet::Publish(Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(PacketIdentifier::new(1).unwrap(), false),
            retain: false,
            topic_name: topic("foo/bar"),
            payload: SharedImpl::from_static(b"hello world"),
            other_properties: Default::default(),
        }),
    }

    encode_decode_v5! {
        Packet::Publish(Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtMostOnce,
            retain: false,
            topic_name: topic("foo/bar"),
            payload: SharedImpl::from_static(b"hello world"),
            other_properties: PublishOtherProperties {
                user_properties: vec![(byte_str("hello"), byte_str("world"))],
                ..Default::default()
            },
        }),

        Packet::Publish(Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(PacketIdentifier::new(1).unwrap(), false),
            retain: false,
            topic_name: topic("foo/bar"),
            payload: SharedImpl::from_static(b"hello world"),
            other_properties: PublishOtherProperties {
                user_properties: vec![(byte_str("hello"), byte_str("world"))],
                ..Default::default()
            },
        }),

        Packet::Publish(Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::ExactlyOnce(PacketIdentifier::new(1).unwrap(), false),
            retain: false,
            topic_name: topic("foo/bar"),
            payload: SharedImpl::from_static(b"hello world"),
            other_properties: PublishOtherProperties {
                user_properties: vec![(byte_str("hello"), byte_str("world"))],
                ..Default::default()
            },
        }),

        Packet::Publish(Publish {
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(PacketIdentifier::new(1).unwrap(), false),
            retain: false,
            topic_name: topic("foo/bar"),
            payload: SharedImpl::from_static(b"hello world"),
            other_properties: PublishOtherProperties {
                user_properties: vec![(byte_str("hello"), byte_str("world"))],
                payload_is_utf8: true,
                message_expiry_interval: Some(10),
                topic_alias: Some(NonZeroU16::new(16).unwrap()),
                response_topic: Some(topic("response/topic")),
                correlation_data: Some(correlation_data("cd")),
                subscription_identifiers: vec![1,2],
                content_type: Some(byte_str("stuff")),
            },
        }),
    }

    #[test]
    fn test_to_shared() {
        let publish = Publish::<SharedImpl> {
            topic_name: topic("kittens"),
            packet_identifier_dup_qos: PacketIdentifierDupQoS::AtMostOnce,
            retain: false,
            payload: SharedImpl::from_static(b"meow"),
            other_properties: PublishOtherProperties {
                response_topic: Some(topic("cute")),
                user_properties: vec![(byte_str("genus"), byte_str("felix"))],
                correlation_data: Some(correlation_data(b"corr_data")),
                ..Default::default()
            },
        };

        let pool = BufferPoolImpl;
        let mut owned = pool.take_empty_owned();

        let publish_shared = publish.to_shared(&mut owned).unwrap();

        assert_eq!(publish, publish_shared);
    }

    #[test]
    fn publish_builder_qos0() {
        let publish =
            Publish::<SharedImpl>::qos0(topic("kittens"), SharedImpl::from_static(b"meow"))
                .with_response_topic(topic("cute"))
                .with_user_properties([(byte_str("genus"), byte_str("felix"))]);

        assert_eq!(
            publish,
            Publish {
                topic_name: topic("kittens"),
                packet_identifier_dup_qos: PacketIdentifierDupQoS::AtMostOnce,
                retain: false,
                payload: SharedImpl::from_static(b"meow"),
                other_properties: PublishOtherProperties {
                    response_topic: Some(topic("cute")),
                    user_properties: vec![(byte_str("genus"), byte_str("felix"))],
                    ..Default::default()
                },
            }
        );
    }

    #[test]
    fn publish_builder_qos1() {
        let publish = Publish::<SharedImpl>::qos1(
            topic("kittens"),
            1,
            false,
            SharedImpl::from_static(b"meow"),
            false,
        )
        .with_response_topic(topic("cute"))
        .with_user_properties([(byte_str("genus"), byte_str("felix"))]);

        assert_eq!(
            publish,
            Publish {
                topic_name: topic("kittens"),
                packet_identifier_dup_qos: PacketIdentifierDupQoS::AtLeastOnce(
                    PacketIdentifier::new(1).unwrap(),
                    false
                ),
                retain: false,
                payload: SharedImpl::from_static(b"meow"),
                other_properties: PublishOtherProperties {
                    response_topic: Some(topic("cute")),
                    user_properties: vec![(byte_str("genus"), byte_str("felix"))],
                    ..Default::default()
                },
            }
        );
    }

    #[test]
    fn test_properties() {
        let publish = Publish::<SharedImpl>::qos1(
            topic("kittens"),
            1,
            false,
            SharedImpl::from_static(b"meow"),
            false,
        )
        .with_user_properties([
            (byte_str("key"), byte_str("val1")),
            (byte_str("key"), byte_str("val2")),
            (byte_str("dummy"), byte_str("val1")),
        ]);

        let result = publish.properties("key");
        assert_eq!(2, result.count());
    }

    #[test]
    fn test_set_property() {
        let mut publish = Publish::<SharedImpl>::qos1(
            topic("kittens"),
            1,
            false,
            SharedImpl::from_static(b"meow"),
            false,
        );

        publish.set_property((byte_str("key"), byte_str("val1")));
        assert_eq!(Some("val1"), publish.property("key").map(AsRef::as_ref));
    }

    #[test]
    fn test_ack_with_reason_string_successful() {
        let publish = Publish::<SharedImpl>::qos1(
            topic("kittens"),
            1,
            false,
            SharedImpl::from_static(b"meow"),
            false,
        );

        let puback = publish
            .ack_with_reason_string(PubAckReasonCode::Success, Some(byte_str("succeed")))
            .unwrap();
        assert_eq!(
            Some(PacketIdentifier::new(1)),
            Some(Some(puback.packet_identifier))
        );
        assert_eq!(PubAckReasonCode::Success, puback.reason_code);
        assert_eq!(
            Some(byte_str("succeed")),
            puback.other_properties.reason_string
        );

        let expected_user_properties: Vec<UserProperty<SharedImpl>> = vec![];
        assert_eq!(
            expected_user_properties,
            puback.other_properties.user_properties
        );
    }

    #[test]
    fn test_ack_with_reason_string_unsuccessful() {
        let publish = Publish::<SharedImpl>::qos1(
            topic("kittens"),
            1,
            false,
            SharedImpl::from_static(b"meow"),
            false,
        );

        let puback = publish
            .ack_with_reason_string(
                PubAckReasonCode::UnspecifiedError,
                Some(byte_str("specific-error")),
            )
            .unwrap();
        assert_eq!(
            Some(PacketIdentifier::new(1)),
            Some(Some(puback.packet_identifier))
        );
        assert_eq!(PubAckReasonCode::UnspecifiedError, puback.reason_code);
        assert_eq!(
            Some(byte_str("specific-error")),
            puback.other_properties.reason_string
        );

        let expected_user_properties: Vec<UserProperty<SharedImpl>> = vec![];
        assert_eq!(
            expected_user_properties,
            puback.other_properties.user_properties
        );
    }
}
