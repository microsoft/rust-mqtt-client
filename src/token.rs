use std::pin::Pin;
use tokio::sync::oneshot::Receiver;

use crate::packet::{PubAckProperties, PubComp, PubCompProperties, PubRecProperties, PubRejectReason, PubRel, PubRelProperties};
use crate::error::{ClientError, CompletionError};

/// Await this token for a notice of the completion of an MQTT operation.
/// It is not required to await this token, only if you want to be notified of the completion.
/// 
/// Ideally, if cloned, this token can report the notice of completion to all copies.
pub struct CompletionToken<T> {
    // NOTE: We cannot actually use a receiver here, as it cannot be cloned. There will need to be a more complex implementation.
    oneshot: Receiver<T>,
}

impl <T> Future for CompletionToken<T> {
    type Output = Result<T, CompletionError>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        Pin::new(&mut self.oneshot).poll(cx).map_err(|_| CompletionError {})
    }
}



//     // Can only be successfully used:
//     // - QoS 1: During the same connection epoch on which it was received.
//     // - QoS 2: During the same session epoch on which it was received.

// ---PUBACK--
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
    /// The returned CompletionToken resolves once the PUBACK is sent (*after* any ordering necessary).
    pub async fn accept(self, properties: PubAckProperties) -> Result<CompletionToken<()>, ClientError> {

        // TODO: Should CompletionToken be provided before the ordering?

        unimplemented!()
    }

    /// Reject the received PUBLISH by issuing a PUBACK with an error reason code.
    /// 
    /// Consumes itself on call so it cannot be used again.
    /// 
    /// Returns once the PUBACK has been accepted into the MQTT session.
    /// The returned CompletionToken resolves once the PUBACK is sent (*after* any ordering necessary).
    pub async fn reject(self, reason: PubRejectReason, properties: PubAckProperties) -> Result<CompletionToken<()>, ClientError> {
        unimplemented!()
    }
}

impl Drop for PubAckToken {
    fn drop(&mut self) {
        // Must accept
        unimplemented!()
    }
}


// ---PUBREC---

pub struct PubRecToken {}

impl PubRecToken {
    pub async fn accept(self, properties: PubRecProperties) -> Result<CompletionToken<(PubRel, PubCompToken)>, ClientError> {
        unimplemented!()
    }

    pub async fn reject(self, reason: PubRejectReason, properties: PubRecProperties) -> Result<CompletionToken<()>, ClientError> {
        unimplemented!()
    }
}

impl Drop for PubRecToken {
    fn drop(&mut self) {
        // Must accept
        unimplemented!()
    }
}


// ---PUBREL---
pub struct PubRelToken {}
impl PubRelToken {
    pub async fn confirm(self, properties: PubRelProperties) -> Result<CompletionToken<PubComp>, ClientError> {
        unimplemented!()
    }
}

impl Drop for PubRelToken {
    fn drop(&mut self) {
        // Must confirm
        unimplemented!()
    }
}


// ---PUBCOMP---
pub struct PubCompToken {}

impl PubCompToken {
    pub async fn confirm(self, properties: PubCompProperties) -> Result<CompletionToken<()>, ClientError> {
        unimplemented!()
    }
}

impl Drop for PubCompToken {
    fn drop(&mut self) {
        // Must confirm
        unimplemented!()
    }
}
