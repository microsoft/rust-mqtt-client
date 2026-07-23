// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz target: packet decode/encode round-trip oracle.
//!
//! Purpose: check that decoding and encoding agree. Any bytes that decode to a valid packet must
//! re-encode and decode again to an EQUAL packet. This catches a bug class that "does not crash"
//! cannot: decoder/encoder asymmetries, fields accepted on decode but mangled on encode, and state
//! that silently changes across a round trip.
//!
//! Scope: the decode path AND the encode path (`Packet::encode`) — this is the only target that
//! exercises the encoder, and it does so on the inputs that matter (packets an attacker could
//! actually cause us to encode, i.e. ones derived from decoding untrusted bytes). The first input
//! byte selects the protocol version (see `split_version`). Comparison is on the decoded `Packet`
//! value, NOT the raw bytes, because the spec permits non-canonical encodings (e.g. over-long
//! varints) that legitimately re-encode to a different byte sequence. Inputs that do not decode
//! contribute nothing to the oracle, so they are rejected from the corpus (`Corpus::Reject`) to
//! keep this target's corpus focused on valid packets — exercising decode error paths is the job
//! of `packet_decode`, not this target.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::{Corpus, fuzz_target};
use ms_mqtt_client::buffer_pool::SingleAccumulator;
use ms_mqtt_client::mqtt_proto::Packet;
use ms_mqtt_client_fuzz::split_version;

fuzz_target!(|data: &[u8]| -> Corpus {
    let Some((version, mut src)) = split_version(data) else {
        return Corpus::Reject;
    };

    let Ok(packet) = Packet::<Bytes>::decode_full(&mut src, version) else {
        // Only decodable inputs are interesting to the round-trip oracle.
        return Corpus::Reject;
    };

    // A packet that decoded successfully must re-encode, and decoding the result must
    // reproduce an equal packet. We compare the decoded *packets*, not the raw bytes,
    // because the spec permits non-canonical encodings (e.g. over-long varints) that
    // legitimately re-encode to a different byte sequence.
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

    Corpus::Keep
});
