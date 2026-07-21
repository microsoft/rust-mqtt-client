// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz target: topic name / topic filter validation and matching.
//!
//! Purpose: prove topic handling is robust against arbitrary strings — whether hostile or merely
//! malformed user input. Validation must never panic, and matching a validated filter against a
//! validated name must always terminate and return a bool (no infinite loop or blow-up on
//! adversarial wildcard patterns such as nested `+`/`#`, `$share/…`, or empty levels).
//!
//! Scope: the PUBLIC topic API — `TopicFilter::new` / `TopicName::new` validation and the
//! `matches_topic_name` wildcard algorithm. Input is structured (two `&str`s produced via
//! `arbitrary`), so this exercises the matching logic far more densely than the byte-level `decode`
//! target reaches it. Packet framing is out of scope here (that is covered by `decode`).

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
