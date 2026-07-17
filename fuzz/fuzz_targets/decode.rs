// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz the MQTT wire decoder against arbitrary bytes.
//!
//! The decoder is the crate's primary untrusted-input boundary. For any input it must only
//! return `Ok`/`Err` — never panic, hang, or trigger undefined behaviour.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    ms_mqtt_client::fuzz::decode(data);
});
