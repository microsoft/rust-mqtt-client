// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use buffer_pool::Shared;

use crate::{DecodeError, EncodeError, PacketMeta, ProtocolVersion};

/// Ref: 3.13 PINGRESP – PING response
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PingResp;

impl<S> PacketMeta<S> for PingResp
where
    S: Shared,
{
    const PACKET_TYPE: u8 = 0xD0;

    fn decode<const RLFML: usize>(
        _flags: u8,
        _src: &mut S,
        _version: ProtocolVersion,
    ) -> Result<Self, DecodeError> {
        Ok(Self)
    }

    fn encode<B, const RLFML: usize>(
        &self,
        _dst: &mut B,
        _version: ProtocolVersion,
    ) -> Result<(), EncodeError> {
        Ok(())
    }
}

#[cfg(all(test, feature = "tests"))]
mod tests {
    use super::*;
    use crate::Packet;

    encode_decode_v3! {
        Packet::PingResp(PingResp),
    }

    encode_decode_v5! {
        Packet::PingResp(PingResp),
    }
}
