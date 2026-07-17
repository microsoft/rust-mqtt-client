// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Decode → re-encode → re-decode round-trip oracle.
//!
//! Any bytes that decode to a valid packet must re-encode and decode again to an equal packet.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    ms_mqtt_client::fuzz::roundtrip(data);
});
