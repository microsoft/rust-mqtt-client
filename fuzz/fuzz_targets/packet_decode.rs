// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz target: MQTT packet decoding.
//!
//! Purpose: prove the wire decoder is robust against arbitrary, potentially hostile input.
//! `Packet::decode_full` is the crate's primary untrusted-input boundary — it sits directly
//! behind bytes arriving off the network — so for ANY input it must only return `Ok`/`Err`,
//! never panic, overflow, hang, or trigger undefined behaviour.
//!
//! Scope: the full decode path — fixed header, the remaining-length varint, packet-type dispatch,
//! every per-packet decoder, v5 properties, `ByteStr` UTF-8 validation, and topic/filter decoding.
//! The first input byte selects the protocol version (see `split_version`); the decoded value is
//! intentionally discarded, because the property under test is "decoding terminates safely", not
//! the result. All non-empty inputs are kept — malformed inputs that fail to decode are the whole
//! point here (they exercise the decoder's error paths), so we deliberately do NOT reject them.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::{Corpus, fuzz_target};
use ms_mqtt_client::mqtt_proto::Packet;
use ms_mqtt_client_fuzz::split_version;

fuzz_target!(|data: &[u8]| -> Corpus {
    let Some((version, mut src)) = split_version(data) else {
        return Corpus::Reject;
    };

    let _ = Packet::<Bytes>::decode_full(&mut src, version);

    Corpus::Keep
});
