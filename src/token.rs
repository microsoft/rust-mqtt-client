// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! Token types for awaiting completion of MQTT operations and issuing acknowledgements.

// TODO: Remove when possible.
#![allow(unused_variables)]
#![allow(clippy::unused_async)]

use crate::error::ClientError;
use crate::packet::{
    PubAckProperties, PubComp, PubCompProperties, PubRecProperties, PubRejectReason, PubRel,
    PubRelProperties,
};

mod completion;
pub use completion::CompletionToken;
pub(crate) use completion::{CompletionTransmitter, completion_pair};

// TODO: These tokens for acknowledgement should likely get their own submodule, and `token` should strictly be for re-exports.
// However, it may also make sense for them to be implemented alongside whatever mechanism tracks acknowledgements.
// For now the stubs are here.

// NOTE: It is unlikely that `Clone` can be derived in the final implementation, it will likely have to be manually implemented.

/// Token that allows the user to acknowledge a received PUBLISH on QoS 1 with a PUBACK.
#[derive(Clone)]
pub struct PubAckToken {}

impl PubAckToken {
    // NOTE: Even though the return values are the same for these two methods (unlike in PubRecToken),
    // we keep the methods separate for
    // 1) consistency with PubRecToken
    // 2) preventing the illegal 0x10 reason code

    /// Accept the received PUBLISH by issuing a PUBACK indicating success.
    ///
    /// Consumes itself on call, so it cannot be used again.
    ///
    /// Returns once the PUBACK has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBACK is sent (*after* any ordering necessary).
    ///
    /// Can only be successfully used during the same connection epoch on which it was received.
    pub async fn accept(
        self,
        properties: PubAckProperties,
    ) -> Result<CompletionToken<()>, ClientError> {
        // TODO: Should CompletionToken be provided before the ordering?

        unimplemented!()
    }

    /// Reject the received PUBLISH by issuing a PUBACK with an error reason code.
    ///
    /// Consumes itself on call so it cannot be used again.
    ///
    /// Returns once the PUBACK has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBACK is sent (*after* any ordering necessary).
    pub async fn reject(
        self,
        reason: PubRejectReason,
        properties: PubAckProperties,
    ) -> Result<CompletionToken<()>, ClientError> {
        unimplemented!()
    }
}

impl Drop for PubAckToken {
    fn drop(&mut self) {
        // Must accept
        unimplemented!()
    }
}

/// Token that allows the user to acknowledge a received PUBLISH on QoS 2 with a PUBREC.
#[derive(Clone)]
pub struct PubRecToken {}

impl PubRecToken {
    /// Accept the received PUBLISH by issuing a PUBREC indicating success.
    ///
    /// Consumes itself on call, so it cannot be used again.
    ///
    /// Returns once the PUBREC has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBREC is sent (*after* any ordering necessary).
    ///
    /// Can only be successfully used during the same session epoch on which it was received.
    pub async fn accept(
        self,
        properties: PubRecProperties,
    ) -> Result<CompletionToken<(PubRel, PubCompToken)>, ClientError> {
        unimplemented!()
    }

    /// Reject the received PUBLISH by issuing a PUBREC with an error reason code.
    ///
    /// Consumes itself on call so it cannot be used again.
    ///
    /// Returns once the PUBREC has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBREC is sent (*after* any ordering necessary).
    ///
    /// Can only be successfully used during the same session epoch on which it was received.
    pub async fn reject(
        self,
        reason: PubRejectReason,
        properties: PubRecProperties,
    ) -> Result<CompletionToken<()>, ClientError> {
        unimplemented!()
    }
}

impl Drop for PubRecToken {
    fn drop(&mut self) {
        // Must accept
        unimplemented!()
    }
}

/// Token that allows the user to acknowledge a received PUBREC with a PUBREL (QoS 2).
#[derive(Clone)]
pub struct PubRelToken {}
impl PubRelToken {
    /// Confirm the PUBREC was received by issuing a PUBREL.
    ///
    /// Consumes itself on call so it cannot be used again.
    ///
    /// Returns once the PUBREL has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBREL is sent (*after* any ordering necessary).
    ///
    /// Can only be successfully used during the same session epoch on which it was received.
    pub async fn confirm(
        self,
        properties: PubRelProperties,
    ) -> Result<CompletionToken<PubComp>, ClientError> {
        unimplemented!()
    }
}

impl Drop for PubRelToken {
    fn drop(&mut self) {
        // Must confirm
        unimplemented!()
    }
}

/// Token that allows the user to acknowledge a received PUBREL with a PUBCOMP (QoS 2).
#[derive(Clone)]
pub struct PubCompToken {}

impl PubCompToken {
    /// Confirm the PUBREL was received by issuing a PUBCOMP.
    ///
    /// Consumes itself on call so it cannot be used again.
    ///
    /// Returns once the PUBCOMP has been accepted into the MQTT session.
    /// The returned `CompletionToken` resolves once the PUBCOMP is sent (*after* any ordering necessary).
    ///
    /// Can only be successfully used during the same session epoch on which it was received.
    pub async fn confirm(
        self,
        properties: PubCompProperties,
    ) -> Result<CompletionToken<()>, ClientError> {
        unimplemented!()
    }
}

impl Drop for PubCompToken {
    fn drop(&mut self) {
        // Must confirm
        unimplemented!()
    }
}
