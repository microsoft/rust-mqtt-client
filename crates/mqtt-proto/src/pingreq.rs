// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use buffer_pool::{BytesAccumulator, Shared};

use crate::{DecodeError, EncodeError, PacketMeta, ProtocolVersion};

/// Ref: 3.12 PINGREQ – PING request
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PingReq;

impl<S> PacketMeta<S> for PingReq
where
    S: Shared,
{
    const PACKET_TYPE: u8 = 0xC0;

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
    ) -> Result<(), EncodeError>
    where
        B: BytesAccumulator<Shared = S>,
    {
        Ok(())
    }
}

#[cfg(all(test, feature = "tests"))]
mod tests {
    use super::*;
    use crate::Packet;

    encode_decode_v3! {
        Packet::PingReq(PingReq),
    }

    encode_decode_v5! {
        Packet::PingReq(PingReq),
    }
}
