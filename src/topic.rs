// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::fmt;

use thiserror::Error;

use crate::buffer_pool;
use crate::buffer_pool::BufferPool;
use crate::mqtt_proto;

/// Error type for validating topics.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct TopicError(#[from] mqtt_proto::DecodeError);

/// MQTT Topic Name as described in MQTT v5, section "4.7 Topic Names and Topic Filters".
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TopicName(mqtt_proto::Topic<mqtt_proto::ByteStr<buffer_pool::SharedImpl>>);

impl TopicName {
    /// Constructs a new `TopicName` after validating the input string.
    ///
    /// # Errors
    /// Returns an error if the topic is invalid.
    /// See <https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901241>.
    pub fn new<S>(s: S) -> Result<Self, TopicError>
    where
        S: AsRef<str>,
    {
        let mut o = buffer_pool::BufferPoolImpl.take_empty_owned();
        let bs = mqtt_proto::ByteStr::new(&mut o, &s).unwrap();
        let topic = mqtt_proto::Topic::new(bs)?;
        Ok(TopicName(topic))
    }

    /// Returns the topic name as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Returns true if the topic name matches the given topic filter.
    #[allow(unused_variables)]
    pub fn matches_topic_filter(&self, filter: &TopicFilter) -> bool {
        todo!("Implement topic filter matching at mqtt_proto level")
    }
}

impl fmt::Display for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

impl From<TopicName> for mqtt_proto::Topic<mqtt_proto::ByteStr<buffer_pool::SharedImpl>> {
    fn from(t: TopicName) -> Self {
        t.0
    }
}

impl From<mqtt_proto::Topic<mqtt_proto::ByteStr<buffer_pool::SharedImpl>>> for TopicName {
    fn from(t: mqtt_proto::Topic<mqtt_proto::ByteStr<buffer_pool::SharedImpl>>) -> Self {
        TopicName(t)
    }
}

/// MQTT Topic Filter as described in MQTT v5, section "4.7 Topic Names and Topic Filters".
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TopicFilter(mqtt_proto::Filter<mqtt_proto::ByteStr<buffer_pool::SharedImpl>>);

impl TopicFilter {
    /// Constructs a new `TopicFilter` after validating the input string.
    ///
    /// # Errors
    /// Returns an error if the topic filter is invalid.
    /// See <https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901241>.
    pub fn new<S>(s: S) -> Result<Self, TopicError>
    where
        S: AsRef<str>,
    {
        let mut o = buffer_pool::BufferPoolImpl.take_empty_owned();
        let bs = mqtt_proto::ByteStr::new(&mut o, &s).unwrap();
        let filter = mqtt_proto::Filter::new(bs)?;
        Ok(TopicFilter(filter))
    }

    /// Returns the topic filter as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Returns true if the topic filter matches the given topic name.
    #[allow(unused_variables)]
    pub fn matches_topic_name(&self, topic: &TopicName) -> bool {
        todo!("Implement topic filter matching at mqtt_proto level")
    }
}

impl fmt::Display for TopicFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

impl From<TopicFilter> for mqtt_proto::Filter<mqtt_proto::ByteStr<buffer_pool::SharedImpl>> {
    fn from(f: TopicFilter) -> Self {
        f.0
    }
}

impl From<mqtt_proto::Filter<mqtt_proto::ByteStr<buffer_pool::SharedImpl>>> for TopicFilter {
    fn from(f: mqtt_proto::Filter<mqtt_proto::ByteStr<buffer_pool::SharedImpl>>) -> Self {
        TopicFilter(f)
    }
}
