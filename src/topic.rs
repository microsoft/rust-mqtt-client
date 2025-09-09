// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Topic name and filter structures


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicName {}

impl TopicName {
    pub fn from(s: &str) -> Self {
        // Validate the topic name according to MQTT rules
        // For simplicity, we assume the topic name is valid here
        TopicName {}
    }

    /// Returns true if the TopicName matches the given TopicFilter
    pub fn matches(&self, filter: &TopicFilter) -> bool {
        unimplemented!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicFilter {}

impl TopicFilter {
    pub fn from(s: &str) -> Self {
        // Validate the topic filter according to MQTT rules
        // For simplicity, we assume the topic filter is valid here
        TopicFilter {}
    }

    /// Returns true if the TopicFilter matches the given TopicName
    pub fn matches(&self, name: &TopicName) -> bool {
        unimplemented!()
    }
}