// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzzing entry points.
//!
//! This module is gated behind the `fuzzing` feature and is hidden from the docs.
//! It is deliberately NOT part of the public, semver-stable API: it exists only so the
//! detached `fuzz/` crate can reach the internal decoders without those internals being
//! exposed to ordinary consumers. Keep this surface as small as possible.

use bytes::Bytes;

use crate::buffer_pool::SingleAccumulator;
use crate::mqtt_proto::{Packet, ProtocolVersion};
use crate::topic::{TopicFilter, TopicName};

const VERSIONS: [ProtocolVersion; 2] = [ProtocolVersion::V3, ProtocolVersion::V5];

/// Decode arbitrary bytes as a full MQTT packet under both protocol versions.
///
/// This targets the primary untrusted-input boundary: the wire decoder. For *any* input
/// it must only ever return `Ok(packet)` or `Err(DecodeError)` — never panic, hang, or
/// trigger undefined behaviour.
pub fn decode(data: &[u8]) {
    for version in VERSIONS {
        let mut src = Bytes::copy_from_slice(data);
        let _ = Packet::<Bytes>::decode_full(&mut src, version);
    }
}

/// Decode → re-encode → re-decode round-trip oracle.
///
/// If arbitrary bytes happen to decode to a valid packet, then encoding that packet (with the
/// same protocol version) and decoding the result again must reproduce an equal packet. We compare
/// the decoded *packets*, not the raw bytes, because the spec permits non-canonical encodings
/// (e.g. over-long varints) that legitimately re-encode to a different byte sequence.
pub fn roundtrip(data: &[u8]) {
    for version in VERSIONS {
        let mut src = Bytes::copy_from_slice(data);
        let Ok(packet) = Packet::<Bytes>::decode_full(&mut src, version) else {
            continue;
        };

        let mut encoded = SingleAccumulator::<Bytes>::new();
        packet
            .encode(&mut encoded, version)
            .expect("a packet that decoded successfully must re-encode");

        let mut reencoded = Bytes::copy_from_slice(encoded.as_ref());
        let redecoded = Packet::<Bytes>::decode_full(&mut reencoded, version)
            .expect("a freshly-encoded packet must decode");

        assert_eq!(
            packet, redecoded,
            "decode/encode round-trip mismatch (version {version})"
        );
    }
}

/// Exercise topic-name / topic-filter validation and wildcard matching.
///
/// Validation must never panic on arbitrary strings, and matching a validated filter against a
/// validated topic name must always terminate and return a bool.
pub fn topic_filter(filter: &str, topic: &str) {
    let Ok(filter) = TopicFilter::new(filter) else {
        return;
    };
    let _ = filter.as_str();

    if let Ok(topic) = TopicName::new(topic) {
        let _ = filter.matches_topic_name(&topic);
    }
}
