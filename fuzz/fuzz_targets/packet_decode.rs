// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz target: MQTT packet decoding.
//!
//! Purpose: prove the wire decoder is robust against arbitrary, potentially hostile input.
//! `Packet::decode_full` is the crate's primary untrusted-input boundary — it sits directly
//! behind bytes arriving off the network — so for ANY input it must only return `Ok`/`Err`,
//! never panic, overflow, hang, or trigger undefined behaviour.
//!
//! Scope: the full decode path for BOTH protocol versions (V3 and V5) — fixed header, the
//! remaining-length varint, packet-type dispatch, every per-packet decoder, v5 properties,
//! `ByteStr` UTF-8 validation, and topic/filter decoding. The decoded value is intentionally
//! discarded: the property under test is "decoding terminates safely", not the result.

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
