// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Decode → re-encode → re-decode round-trip oracle.
//!
//! Any bytes that decode to a valid packet must re-encode and decode again to an equal packet.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use ms_mqtt_client::buffer_pool::SingleAccumulator;
use ms_mqtt_client::mqtt_proto::{Packet, ProtocolVersion};

fuzz_target!(|data: &[u8]| {
    for version in [ProtocolVersion::V3, ProtocolVersion::V5] {
        let mut src = Bytes::copy_from_slice(data);
        let Ok(packet) = Packet::<Bytes>::decode_full(&mut src, version) else {
            continue;
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
    }
});
