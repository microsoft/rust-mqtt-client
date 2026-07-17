// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz topic-name / topic-filter validation and wildcard matching.
//!
//! Structured input: two arbitrary strings, interpreted as a topic filter and a topic name.
//! Validation must never panic, and matching must always terminate.

#![no_main]

use libfuzzer_sys::fuzz_target;
use ms_mqtt_client::topic::{TopicFilter, TopicName};

fuzz_target!(|input: (&str, &str)| {
    let (filter, topic) = input;

    let Ok(filter) = TopicFilter::new(filter) else {
        return;
    };
    let _ = filter.as_str();

    if let Ok(topic) = TopicName::new(topic) {
        let _ = filter.matches_topic_name(&topic);
    }
});
