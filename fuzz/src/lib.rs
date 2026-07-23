// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Shared helpers for the packet fuzz targets.

use bytes::Bytes;
use ms_mqtt_client::mqtt_proto::ProtocolVersion;

// ============================================================================================
// V3/V5 VERSION SELECTION — DEFENSE-IN-DEPTH FOR THE SHARED LOW-LEVEL CODEC.
//
// This client only ever speaks MQTT 5 on the wire: the sole inbound decode path
// (`client::mqtt_receive`) hardcodes `ProtocolVersion::V5`, so the V3 decode/encode paths in the
// low-level codec are NOT reachable through the client's network input. We nonetheless fuzz V3
// deliberately — the V3 paths exist in this crate and we want them hardened for as long as they do.
//
// The entire V3 concern is intentionally confined to THIS function so it can be removed in a single
// edit. To drop V3 later: delete this function and have the packet targets decode the whole input
// as `ProtocolVersion::V5` directly (i.e. `Bytes::copy_from_slice(data)`, no selector byte).
// ============================================================================================

/// Split a fuzz input into a protocol version and the packet bytes to decode.
///
/// The first byte selects the protocol version (even → V3, odd → V5); the remaining bytes are the
/// packet body. Returns `None` for an empty input (nothing to fuzz), which callers should map to
/// `Corpus::Reject`. Giving the fuzzer explicit control of the version via a single byte (rather
/// than decoding every input under both versions) keeps each execution to one decode and makes
/// corpus entries self-describing.
pub fn split_version(data: &[u8]) -> Option<(ProtocolVersion, Bytes)> {
    let (&selector, packet) = data.split_first()?;
    let version = if selector & 1 == 0 {
        ProtocolVersion::V3
    } else {
        ProtocolVersion::V5
    };
    Some((version, Bytes::copy_from_slice(packet)))
}
