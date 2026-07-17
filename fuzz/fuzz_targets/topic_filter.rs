// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz topic-name / topic-filter validation and wildcard matching.
//!
//! Structured input: two arbitrary strings, interpreted as a topic filter and a topic name.
//! Validation must never panic, and matching must always terminate.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: (&str, &str)| {
    let (filter, topic) = input;
    ms_mqtt_client::fuzz::topic_filter(filter, topic);
});
