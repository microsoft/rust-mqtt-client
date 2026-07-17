// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz the MQTT wire decoder against arbitrary bytes.
//!
//! The decoder is the crate's primary untrusted-input boundary. For any input it must only
//! return `Ok`/`Err` — never panic, hang, or trigger undefined behaviour.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use ms_mqtt_client::mqtt_proto::{Packet, ProtocolVersion};

fuzz_target!(|data: &[u8]| {
    for version in [ProtocolVersion::V3, ProtocolVersion::V5] {
        let mut src = Bytes::copy_from_slice(data);
        let _ = Packet::<Bytes>::decode_full(&mut src, version);
    }
});
