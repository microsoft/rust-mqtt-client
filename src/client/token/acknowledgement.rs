// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Controls for acknowledging incoming MQTT packet flows.
//!
//! Acknowledgement tokens are protocol controls, not passive completion observers. Dropping an
//! unused token attempts to submit the successful default response for its protocol phase so the
//! exchange can continue. Retain the token to control timing, rejection, or packet properties.
//!
//! On success, [`PubAckToken::accept`] and [`PubAckToken::reject`] have submitted the selected
//! PUBACK to the MQTT session and return a completion token. These methods take ownership of the
//! acknowledgement token. Canceling an in-progress call drops that token, so its default behavior
//! applies; the selected reason code and properties are not submitted.
//!
//! Awaiting the returned completion token reports when the session releases the PUBACK for
//! transmission after any required ordering. Dropping the completion token does not undo the
//! submitted acknowledgement.
//!
//! QoS 1 tokens are connection-scoped. QoS 2 tokens are MQTT-session-scoped and remain valid
//! across a reconnect only when the server reports that the previous session is present.

use bytes::Bytes;

use crate::client::token::completion::{
    PubAckCompletionToken, PubCompConfirmCompletionToken, PubRecAcceptCompletionToken,
    PubRecRejectCompletionToken, PubRelCompletionToken,
};
use crate::error::DetachedError;
use crate::packet::{
    PubAckProperties, PubCompProperties, PubRecProperties, PubRejectReason, PubRelProperties,
};

/// Used to accept or reject an incoming QoS 1 PUBLISH with PUBACK.
///
/// Dropping an unused token attempts to accept the publish with default PUBACK properties. To
/// submit different properties or reject the publish, use [`Self::accept`] or [`Self::reject`].
/// These methods take ownership of the token. Canceling an in-progress call drops the token, so
/// the same default behavior applies.
///
/// The token is valid only during the connection epoch in which it was received. A PUBACK from an
/// earlier epoch is never transmitted on a later connection.
#[derive(Debug)]
pub struct PubAckToken(pub(crate) buffered::PubAckToken<Bytes>);

impl PubAckToken {
    /// Accept the received PUBLISH by issuing a PUBACK indicating success.
    ///
    /// Consumes the token, so it cannot be used again.
    ///
    /// On success, the PUBACK has been submitted to the MQTT session and a completion token is
    /// returned; this does not mean the packet has been written to the transport. Awaiting the
    /// completion token reports when the session releases the PUBACK for transmission after any
    /// required ordering.
    ///
    /// Can only be successfully used during the same connection epoch on which it was received.
    ///
    /// # Cancellation
    ///
    /// Canceling this operation before it returns drops the token. The requested properties are
    /// not submitted, and the token's default acknowledgement behavior applies.
    pub async fn accept(
        self,
        properties: PubAckProperties,
    ) -> Result<PubAckCompletionToken, DetachedError> {
        self.0.accept(properties.into()).await
    }

    /// Reject the received PUBLISH by issuing a PUBACK with an error reason code.
    ///
    /// Consumes the token, so it cannot be used again.
    ///
    /// On success, the PUBACK has been submitted to the MQTT session and a completion token is
    /// returned; this does not mean the packet has been written to the transport. Awaiting the
    /// completion token reports when the session releases the PUBACK for transmission after any
    /// required ordering.
    ///
    /// # Cancellation
    ///
    /// Canceling this operation before it returns drops the token. The requested rejection and
    /// properties are not submitted, and the token's default acknowledgement behavior applies.
    pub async fn reject(
        self,
        reason: PubRejectReason,
        properties: PubAckProperties,
    ) -> Result<PubAckCompletionToken, DetachedError> {
        self.0.reject(reason.into(), properties.into()).await
    }
}

/// Used to accept or reject an incoming QoS 2 PUBLISH with PUBREC.
///
/// Dropping an unused token attempts to accept the publish with default PUBREC properties. To
/// submit different properties or reject the publish, use [`Self::accept`] or [`Self::reject`].
/// These methods take ownership of the token. Canceling an in-progress call drops the token, so
/// the same default behavior applies.
#[derive(Debug)]
pub struct PubRecToken(pub(crate) buffered::PubRecToken<Bytes>);

impl PubRecToken {
    /// Accept the received PUBLISH by issuing a PUBREC indicating success.
    ///
    /// Takes ownership of the token, so it cannot be used again.
    ///
    /// On success, the PUBREC has been submitted to the MQTT session and a completion token is
    /// returned; this does not mean the packet has been written to the transport. Awaiting the
    /// completion token returns the server's [`crate::packet::PubRel`] and its [`PubCompToken`].
    ///
    /// Can only be successfully used during the same MQTT session epoch in which it was
    /// received.
    ///
    /// # Cancellation
    ///
    /// Canceling this operation before it returns drops the token. The requested properties are
    /// not submitted, and the token's default acknowledgement behavior applies.
    pub async fn accept(
        self,
        properties: PubRecProperties,
    ) -> Result<PubRecAcceptCompletionToken, DetachedError> {
        self.0
            .accept(properties.into())
            .await
            .map(|token| PubRecAcceptCompletionToken(token.0))
    }

    /// Reject the received PUBLISH by issuing a PUBREC with an error reason code.
    ///
    /// Takes ownership of the token, so it cannot be used again.
    ///
    /// On success, the PUBREC has been submitted to the MQTT session and a completion token is
    /// returned; this does not mean the packet has been written to the transport. Awaiting the
    /// completion token reports when the session releases the PUBREC for transmission after any
    /// required ordering.
    ///
    /// Can only be successfully used during the same MQTT session epoch in which it was
    /// received.
    ///
    /// # Cancellation
    ///
    /// Canceling this operation before it returns drops the token. The requested rejection and
    /// properties are not submitted, and the token's default acknowledgement behavior applies.
    pub async fn reject(
        self,
        reason: PubRejectReason,
        properties: PubRecProperties,
    ) -> Result<PubRecRejectCompletionToken, DetachedError> {
        self.0.reject(reason.into(), properties.into()).await
    }
}

/// Used to confirm a received PUBREC with PUBREL.
///
/// Dropping an unused token attempts confirmation with default PUBREL properties. To supply
/// different properties, use [`Self::confirm`]. This method takes ownership of the token.
/// Canceling an in-progress call drops the token, so the same default behavior applies.
#[derive(Debug)]
pub struct PubRelToken(pub(crate) buffered::PubRelToken<Bytes>);

impl PubRelToken {
    /// Confirm the PUBREC was received by issuing a PUBREL.
    ///
    /// Takes ownership of the token, so it cannot be used again.
    ///
    /// On success, the PUBREL has been submitted to the MQTT session and a completion token is
    /// returned; this does not mean the packet has been written to the transport. Awaiting the
    /// completion token returns the server's [`crate::packet::PubComp`].
    ///
    /// Can only be successfully used during the same MQTT session epoch in which it was
    /// received.
    ///
    /// # Cancellation
    ///
    /// Canceling this operation before it returns drops the token. The requested properties are
    /// not submitted, and the token's default confirmation behavior applies.
    pub async fn confirm(
        self,
        properties: PubRelProperties,
    ) -> Result<PubRelCompletionToken, DetachedError> {
        self.0
            .confirm(properties.into())
            .await
            .map(|token| PubRelCompletionToken(token.0))
    }
}

/// Used to confirm a received PUBREL with PUBCOMP.
///
/// Dropping an unused token attempts confirmation with default PUBCOMP properties. To supply
/// different properties, use [`Self::confirm`]. This method takes ownership of the token.
/// Canceling an in-progress call drops the token, so the same default behavior applies.
#[derive(Debug)]
pub struct PubCompToken(pub(crate) buffered::PubCompToken<Bytes>);

impl PubCompToken {
    /// Confirm the PUBREL was received by issuing a PUBCOMP.
    ///
    /// Takes ownership of the token, so it cannot be used again.
    ///
    /// On success, the PUBCOMP has been submitted to the MQTT session and a completion token is
    /// returned; this does not mean the packet has been written to the transport. Awaiting the
    /// completion token reports when the session releases the PUBCOMP for transmission.
    ///
    /// Can only be successfully used during the same MQTT session epoch in which it was
    /// received.
    ///
    /// # Cancellation
    ///
    /// Canceling this operation before it returns drops the token. The requested properties are
    /// not submitted, and the token's default confirmation behavior applies.
    pub async fn confirm(
        self,
        properties: PubCompProperties,
    ) -> Result<PubCompConfirmCompletionToken, DetachedError> {
        self.0.confirm(properties.into()).await
    }
}

pub(crate) mod buffered {

    use tokio::sync::mpsc::UnboundedSender;

    use crate::buffer_pool::Shared;
    use crate::client::channel_data::AcknowledgementRequest;
    use crate::client::token::completion::buffered::{
        PubAckCompletionToken, PubCompConfirmCompletionToken, PubRecAcceptCompletionToken,
        PubRecRejectCompletionToken, PubRelConfirmCompletionToken, completion_pair,
    };
    use crate::error::DetachedError;
    use crate::mqtt_proto::{
        PacketIdentifier, PubAck, PubAckOtherProperties, PubAckReasonCode, PubComp,
        PubCompOtherProperties, PubCompReasonCode, PubRec, PubRecOtherProperties, PubRecReasonCode,
        PubRel, PubRelOtherProperties, PubRelReasonCode,
    };

    /// Used to accept or reject an incoming QoS 1 PUBLISH with PUBACK.
    ///
    /// Dropping an unused token attempts acceptance with default PUBACK properties.
    #[derive(Debug)]
    pub struct PubAckToken<S>
    where
        S: Shared,
    {
        pkid: PacketIdentifier,
        epoch: u64,
        tx: UnboundedSender<AcknowledgementRequest<S>>,
        triggered: bool,
    }

    impl<S> PubAckToken<S>
    where
        S: Shared,
    {
        pub(crate) fn new(
            pkid: PacketIdentifier,
            epoch: u64,
            tx: UnboundedSender<AcknowledgementRequest<S>>,
        ) -> Self {
            Self {
                pkid,
                epoch,
                tx,
                triggered: false,
            }
        }

        // NOTE: Even though the return values are the same for these two methods (unlike in PubRecToken),
        // we keep the methods separate for
        // 1) consistency with PubRecToken
        // 2) preventing the illegal 0x10 reason code

        /// Accept the received PUBLISH by issuing a PUBACK indicating success.
        ///
        /// Takes ownership of the token, so it cannot be used again.
        ///
        /// On success, the PUBACK has been submitted to the MQTT session and a completion token is
        /// returned; this does not mean the packet has been written to the transport. Awaiting the
        /// completion token reports when the session releases the PUBACK for transmission after
        /// any required ordering.
        ///
        /// Can only be successfully used during the same connection epoch on which it was received.
        ///
        /// # Cancellation
        ///
        /// Canceling this operation before it returns drops the token, so its default
        /// acknowledgement behavior applies.
        pub async fn accept(
            self,
            properties: PubAckOtherProperties<S>,
        ) -> Result<PubAckCompletionToken, DetachedError> {
            self.send(properties, PubAckReasonCode::Success).await
        }

        /// Reject the received PUBLISH by issuing a PUBACK with an error reason code.
        ///
        /// Takes ownership of the token, so it cannot be used again.
        ///
        /// On success, the PUBACK has been submitted to the MQTT session and a completion token is
        /// returned; this does not mean the packet has been written to the transport. Awaiting the
        /// completion token reports when the session releases the PUBACK for transmission after
        /// any required ordering.
        ///
        /// # Cancellation
        ///
        /// Canceling this operation before it returns drops the token, so its default
        /// acknowledgement behavior applies.
        pub async fn reject(
            self,
            reason: PubAckReasonCode,
            properties: PubAckOtherProperties<S>,
        ) -> Result<PubAckCompletionToken, DetachedError> {
            self.send(properties, reason).await
        }

        /// Internal helper to send the acknowledgement request.
        async fn send(
            mut self,
            properties: PubAckOtherProperties<S>,
            reason: PubAckReasonCode,
        ) -> Result<PubAckCompletionToken, DetachedError> {
            let completion =
                PubAckToken::inner_send(&self.tx, self.pkid, properties, reason, self.epoch)?;
            self.triggered = true;
            Ok(completion)
        }

        /// Internal helper to send the acknowledgement request.
        /// Does not operate on self in order to allow for use in drop efficiently.
        fn inner_send(
            tx: &UnboundedSender<AcknowledgementRequest<S>>,
            packet_identifier: PacketIdentifier,
            other_properties: PubAckOtherProperties<S>,
            reason_code: PubAckReasonCode,
            epoch: u64,
        ) -> Result<PubAckCompletionToken, DetachedError> {
            let (notifier, token) = completion_pair();
            let puback = PubAck {
                packet_identifier,
                reason_code,
                other_properties,
            };
            tx.send(AcknowledgementRequest::PubAck(notifier, puback, epoch))
                .map_err(|_| DetachedError {})?;
            Ok(PubAckCompletionToken(token))
        }
    }

    impl<S> Drop for PubAckToken<S>
    where
        S: Shared,
    {
        fn drop(&mut self) {
            // Must acknowledge if the token was not used in order to prevent locking the
            // ack ordering flow.
            if !self.triggered {
                let _ = PubAckToken::inner_send(
                    &self.tx,
                    self.pkid,
                    Default::default(),
                    PubAckReasonCode::Success,
                    self.epoch,
                );
            }
        }
    }

    /// Used to accept or reject an incoming QoS 2 PUBLISH with PUBREC.
    ///
    /// The intended drop behavior accepts an unused token with default PUBREC properties.
    #[derive(Debug)]
    pub struct PubRecToken<S>
    where
        S: Shared,
    {
        pkid: PacketIdentifier,
        epoch: u64,
        tx: UnboundedSender<AcknowledgementRequest<S>>,
        triggered: bool,
    }

    impl<S> PubRecToken<S>
    where
        S: Shared,
    {
        pub(crate) fn new(
            pkid: PacketIdentifier,
            epoch: u64,
            tx: UnboundedSender<AcknowledgementRequest<S>>,
        ) -> Self {
            Self {
                pkid,
                epoch,
                tx,
                triggered: false,
            }
        }

        /// Accept the received PUBLISH by issuing a PUBREC indicating success.
        ///
        /// Takes ownership of the token, so it cannot be used again.
        ///
        /// On success, the PUBREC has been submitted to the MQTT session and a completion token is
        /// returned; this does not mean the packet has been written to the transport. Awaiting the
        /// completion token returns the server's PUBREL and its [`PubCompToken`].
        ///
        /// Can only be successfully used during the same MQTT session epoch in which it was
        /// received.
        ///
        /// # Cancellation
        ///
        /// Canceling this operation before it returns drops the token, so its intended default
        /// acknowledgement behavior applies.
        pub async fn accept(
            self,
            properties: PubRecOtherProperties<S>,
        ) -> Result<PubRecAcceptCompletionToken<S>, DetachedError> {
            self.send_accept(properties).await
        }

        /// Reject the received PUBLISH by issuing a PUBREC with an error reason code.
        ///
        /// Takes ownership of the token, so it cannot be used again.
        ///
        /// On success, the PUBREC has been submitted to the MQTT session and a completion token is
        /// returned; this does not mean the packet has been written to the transport. Awaiting the
        /// completion token reports when the session releases the PUBREC for transmission after
        /// any required ordering.
        ///
        /// Can only be successfully used during the same MQTT session epoch in which it was
        /// received.
        ///
        /// # Cancellation
        ///
        /// Canceling this operation before it returns drops the token, so its intended default
        /// acknowledgement behavior applies.
        pub async fn reject(
            self,
            reason: PubRecReasonCode,
            properties: PubRecOtherProperties<S>,
        ) -> Result<PubRecRejectCompletionToken, DetachedError> {
            self.send_reject(reason, properties).await
        }

        async fn send_accept(
            mut self,
            properties: PubRecOtherProperties<S>,
        ) -> Result<PubRecAcceptCompletionToken<S>, DetachedError> {
            let ct = Self::inner_accept(&self.tx, self.pkid, properties, self.epoch)?;
            self.triggered = true;
            Ok(ct)
        }

        fn inner_accept(
            tx: &UnboundedSender<AcknowledgementRequest<S>>,
            packet_identifier: PacketIdentifier,
            other_properties: PubRecOtherProperties<S>,
            epoch: u64,
        ) -> Result<PubRecAcceptCompletionToken<S>, DetachedError> {
            let (notifier, token) = completion_pair();
            let pubrec = PubRec {
                packet_identifier,
                reason_code: PubRecReasonCode::Success,
                other_properties,
            };
            tx.send(AcknowledgementRequest::PubRecAccept(
                notifier, pubrec, epoch,
            ))
            .map_err(|_| DetachedError {})?;
            Ok(PubRecAcceptCompletionToken(token))
        }

        async fn send_reject(
            mut self,
            reason_code: PubRecReasonCode,
            other_properties: PubRecOtherProperties<S>,
        ) -> Result<PubRecRejectCompletionToken, DetachedError> {
            let (notifier, token) = completion_pair();
            let pubrec = PubRec {
                packet_identifier: self.pkid,
                reason_code,
                other_properties,
            };
            self.tx
                .send(AcknowledgementRequest::PubRecReject(
                    notifier, pubrec, self.epoch,
                ))
                .map_err(|_| DetachedError {})?;
            self.triggered = true;
            Ok(PubRecRejectCompletionToken(token))
        }
    }

    impl<S> Drop for PubRecToken<S>
    where
        S: Shared,
    {
        fn drop(&mut self) {
            // Must accept if the token was not used in order to prevent locking the ack ordering flow.
            if !self.triggered {
                let _ = Self::inner_accept(&self.tx, self.pkid, Default::default(), self.epoch);
            }
        }
    }

    /// Used to confirm a received PUBREC with PUBREL.
    ///
    /// The intended drop behavior confirms an unused token with default PUBREL properties.
    #[derive(Debug)]
    pub struct PubRelToken<S>
    where
        S: Shared,
    {
        pkid: PacketIdentifier,
        epoch: u64,
        tx: UnboundedSender<AcknowledgementRequest<S>>,
        triggered: bool,
    }

    impl<S> PubRelToken<S>
    where
        S: Shared,
    {
        pub(crate) fn new(
            pkid: PacketIdentifier,
            epoch: u64,
            tx: UnboundedSender<AcknowledgementRequest<S>>,
        ) -> Self {
            Self {
                pkid,
                epoch,
                tx,
                triggered: false,
            }
        }

        /// Confirm the PUBREC was received by issuing a PUBREL.
        ///
        /// Takes ownership of the token, so it cannot be used again.
        ///
        /// On success, the PUBREL has been submitted to the MQTT session and a completion token is
        /// returned; this does not mean the packet has been written to the transport. Awaiting the
        /// completion token returns the server's PUBCOMP.
        ///
        /// Can only be successfully used during the same MQTT session epoch in which it was
        /// received.
        ///
        /// # Cancellation
        ///
        /// Canceling this operation before it returns drops the token, so its intended default
        /// confirmation behavior applies.
        pub async fn confirm(
            self,
            properties: PubRelOtherProperties<S>,
        ) -> Result<PubRelConfirmCompletionToken<S>, DetachedError> {
            self.send(properties).await
        }

        async fn send(
            mut self,
            properties: PubRelOtherProperties<S>,
        ) -> Result<PubRelConfirmCompletionToken<S>, DetachedError> {
            let ct = Self::inner_send(&self.tx, self.pkid, properties, self.epoch)?;
            self.triggered = true;
            Ok(ct)
        }

        fn inner_send(
            tx: &UnboundedSender<AcknowledgementRequest<S>>,
            packet_identifier: PacketIdentifier,
            other_properties: PubRelOtherProperties<S>,
            epoch: u64,
        ) -> Result<PubRelConfirmCompletionToken<S>, DetachedError> {
            let (notifier, token) = completion_pair();
            let pubrel = PubRel {
                packet_identifier,
                reason_code: PubRelReasonCode::Success,
                other_properties,
            };
            tx.send(AcknowledgementRequest::PubRel(notifier, pubrel, epoch))
                .map_err(|_| DetachedError {})?;
            Ok(PubRelConfirmCompletionToken(token))
        }
    }

    impl<S> Drop for PubRelToken<S>
    where
        S: Shared,
    {
        fn drop(&mut self) {
            // Must confirm if the token was not used in order to prevent locking the ack ordering flow.
            if !self.triggered {
                let _ = Self::inner_send(&self.tx, self.pkid, Default::default(), self.epoch);
            }
        }
    }

    /// Used to confirm a received PUBREL with PUBCOMP.
    ///
    /// The intended drop behavior confirms an unused token with default PUBCOMP properties.
    #[derive(Debug)]
    pub struct PubCompToken<S>
    where
        S: Shared,
    {
        pkid: PacketIdentifier,
        epoch: u64,
        tx: UnboundedSender<AcknowledgementRequest<S>>,
        triggered: bool,
    }

    impl<S> PubCompToken<S>
    where
        S: Shared,
    {
        pub(crate) fn new(
            pkid: PacketIdentifier,
            epoch: u64,
            tx: UnboundedSender<AcknowledgementRequest<S>>,
        ) -> Self {
            Self {
                pkid,
                epoch,
                tx,
                triggered: false,
            }
        }

        /// Confirm the PUBREL was received by issuing a PUBCOMP.
        ///
        /// Takes ownership of the token, so it cannot be used again.
        ///
        /// On success, the PUBCOMP has been submitted to the MQTT session and a completion token
        /// is returned; this does not mean the packet has been written to the transport. Awaiting
        /// the completion token reports when the session releases the PUBCOMP for transmission.
        ///
        /// Can only be successfully used during the same MQTT session epoch in which it was
        /// received.
        ///
        /// # Cancellation
        ///
        /// Canceling this operation before it returns drops the token, so its intended default
        /// confirmation behavior applies.
        pub async fn confirm(
            self,
            properties: PubCompOtherProperties<S>,
        ) -> Result<PubCompConfirmCompletionToken, DetachedError> {
            self.send(properties).await
        }

        async fn send(
            mut self,
            properties: PubCompOtherProperties<S>,
        ) -> Result<PubCompConfirmCompletionToken, DetachedError> {
            let ct = Self::inner_send(&self.tx, self.pkid, properties, self.epoch)?;
            self.triggered = true;
            Ok(ct)
        }

        fn inner_send(
            tx: &UnboundedSender<AcknowledgementRequest<S>>,
            packet_identifier: PacketIdentifier,
            other_properties: PubCompOtherProperties<S>,
            epoch: u64,
        ) -> Result<PubCompConfirmCompletionToken, DetachedError> {
            let (notifier, token) = completion_pair();
            let pubcomp = PubComp {
                packet_identifier,
                reason_code: PubCompReasonCode::Success,
                other_properties,
            };
            tx.send(AcknowledgementRequest::PubComp(notifier, pubcomp, epoch))
                .map_err(|_| DetachedError {})?;
            Ok(PubCompConfirmCompletionToken(token))
        }
    }

    impl<S> Drop for PubCompToken<S>
    where
        S: Shared,
    {
        fn drop(&mut self) {
            // Must confirm if the tokenw as not used in order to prevent locking the ack ordering flow.
            if !self.triggered {
                let _ = Self::inner_send(&self.tx, self.pkid, Default::default(), self.epoch);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use futures_util::FutureExt;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

    use super::buffered::*;
    use crate::client::channel_data::AcknowledgementRequest;
    use crate::mqtt_proto::{
        PacketIdentifier, PubAckOtherProperties, PubAckReasonCode, PubComp, PubCompOtherProperties,
        PubCompReasonCode, PubRecOtherProperties, PubRecReasonCode, PubRel, PubRelOtherProperties,
        PubRelReasonCode,
    };

    fn acknowledgement_channel() -> (
        UnboundedSender<AcknowledgementRequest<Bytes>>,
        UnboundedReceiver<AcknowledgementRequest<Bytes>>,
    ) {
        tokio::sync::mpsc::unbounded_channel()
    }

    fn assert_default_puback(
        request: Option<AcknowledgementRequest<Bytes>>,
        packet_identifier: PacketIdentifier,
        epoch: u64,
    ) {
        let Some(AcknowledgementRequest::PubAck(notifier, puback, request_epoch)) = request else {
            panic!("Did not receive automatic PubAck acknowledgement request");
        };
        assert_eq!(request_epoch, epoch);
        assert_eq!(puback.packet_identifier, packet_identifier);
        assert_eq!(puback.reason_code, PubAckReasonCode::Success);
        assert_eq!(puback.other_properties, Default::default());
        assert!(notifier.complete(()).is_err());
    }

    fn assert_default_pubrec(
        request: Option<AcknowledgementRequest<Bytes>>,
        packet_identifier: PacketIdentifier,
        epoch: u64,
    ) {
        let Some(AcknowledgementRequest::PubRecAccept(_, pubrec, request_epoch)) = request else {
            panic!("Did not receive automatic PubRec acknowledgement request");
        };
        assert_eq!(request_epoch, epoch);
        assert_eq!(pubrec.packet_identifier, packet_identifier);
        assert_eq!(pubrec.reason_code, PubRecReasonCode::Success);
        assert_eq!(pubrec.other_properties, Default::default());
    }

    fn assert_default_pubrel(
        request: Option<AcknowledgementRequest<Bytes>>,
        packet_identifier: PacketIdentifier,
        epoch: u64,
    ) {
        let Some(AcknowledgementRequest::PubRel(_, pubrel, request_epoch)) = request else {
            panic!("Did not receive automatic PubRel acknowledgement request");
        };
        assert_eq!(request_epoch, epoch);
        assert_eq!(pubrel.packet_identifier, packet_identifier);
        assert_eq!(pubrel.reason_code, PubRelReasonCode::Success);
        assert_eq!(pubrel.other_properties, Default::default());
    }

    fn assert_default_pubcomp(
        request: Option<AcknowledgementRequest<Bytes>>,
        packet_identifier: PacketIdentifier,
        epoch: u64,
    ) {
        let Some(AcknowledgementRequest::PubComp(notifier, pubcomp, request_epoch)) = request
        else {
            panic!("Did not receive automatic PubComp acknowledgement request");
        };
        assert_eq!(request_epoch, epoch);
        assert_eq!(pubcomp.packet_identifier, packet_identifier);
        assert_eq!(pubcomp.reason_code, PubCompReasonCode::Success);
        assert_eq!(pubcomp.other_properties, Default::default());
        assert!(notifier.complete(()).is_err());
    }

    #[tokio::test]
    async fn puback_token_accept() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubAckOtherProperties {
            reason_string: Some("Test Success".into()),
            user_properties: vec![
                ("key1".into(), "value1".into()),
                ("key2".into(), "value2".into()),
            ],
        };
        let token = PubAckToken::new(pkid, epoch, tx);
        let completion_token = token.accept(properties.clone()).await.unwrap();
        if let Some(AcknowledgementRequest::PubAck(notifier, puback, req_epoch)) = rx.recv().await {
            // The correct data was sent in the acknowledgement request
            assert_eq!(req_epoch, epoch);
            assert_eq!(puback.packet_identifier, pkid);
            assert_eq!(puback.reason_code, PubAckReasonCode::Success);
            assert_eq!(puback.other_properties, properties);
            // Using the acknowledgement request notifier completes the completion token that was returned
            let completion_value = ();
            notifier.complete(completion_value).unwrap();
            assert_eq!(completion_token.await, Ok(completion_value));
        } else {
            panic!("Did not receive PubAck acknowledgement request");
        }
    }

    #[tokio::test]
    async fn puback_token_reject() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubAckOtherProperties {
            reason_string: Some("Test Reject".into()),
            user_properties: vec![
                ("key1".into(), "value1".into()),
                ("key2".into(), "value2".into()),
            ],
        };
        let token = PubAckToken::new(pkid, epoch, tx);
        let completion_token = token
            .reject(PubAckReasonCode::NotAuthorized, properties.clone())
            .await
            .unwrap();
        if let Some(AcknowledgementRequest::PubAck(notifier, puback, req_epoch)) = rx.recv().await {
            assert_eq!(req_epoch, epoch);
            assert_eq!(puback.packet_identifier, pkid);
            assert_eq!(puback.reason_code, PubAckReasonCode::NotAuthorized);
            assert_eq!(puback.other_properties, properties);
            // Using the acknowledgement request notifier completes the completion token that was returned
            let completion_value = ();
            notifier.complete(completion_value).unwrap();
            assert_eq!(completion_token.await, Ok(completion_value));
        } else {
            panic!("Did not receive PubAck acknowledgement request");
        }
    }

    #[tokio::test]
    async fn public_puback_accept_future_drop_before_poll() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let token = super::PubAckToken(PubAckToken::new(pkid, epoch, tx));
        let properties = crate::packet::PubAckProperties {
            reason_string: Some("not submitted".into()),
            user_properties: Vec::new(),
        };

        let accept = token.accept(properties);
        drop(accept);

        assert_default_puback(rx.recv().await, pkid, epoch);
    }

    #[tokio::test]
    async fn puback_accept_submits_on_first_poll() {
        let (tx, mut rx) = acknowledgement_channel();
        let epoch = 3;
        let pkid = PacketIdentifier::new(1).unwrap();
        let token = PubAckToken::new(pkid, epoch, tx);
        let properties = PubAckOtherProperties {
            reason_string: Some("submitted".into()),
            user_properties: vec![("key".into(), "value".into())],
        };
        let accept = token.accept(properties.clone());
        drop(
            accept
                .now_or_never()
                .expect("PubAck submission yielded unexpectedly")
                .unwrap(),
        );

        let Some(AcknowledgementRequest::PubAck(_, puback, req_epoch)) = rx.recv().await else {
            panic!("Did not receive PubAck acknowledgement request");
        };
        assert_eq!(req_epoch, epoch);
        assert_eq!(puback.packet_identifier, pkid);
        assert_eq!(puback.reason_code, PubAckReasonCode::Success);
        assert_eq!(puback.other_properties, properties);
        assert_eq!(rx.len(), 0);
    }

    #[tokio::test]
    async fn puback_token_drop_before_use() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let token = PubAckToken::<Bytes>::new(pkid, epoch, tx);
        // Drop the token without accepting or rejecting it
        drop(token);

        assert_eq!(rx.len(), 1);
        assert_default_puback(rx.recv().await, pkid, epoch);
        assert_eq!(rx.len(), 0);
    }

    #[tokio::test]
    async fn puback_token_drop_after_use() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubAckOtherProperties {
            reason_string: Some("Test Success".into()),
            user_properties: vec![
                ("key1".into(), "value1".into()),
                ("key2".into(), "value2".into()),
            ],
        };
        let token = PubAckToken::new(pkid, epoch, tx);
        // Use the token to send an acceptance
        let completion_token = token.accept(properties.clone()).await.unwrap();
        if let Some(AcknowledgementRequest::PubAck(_, puback, req_epoch)) = rx.recv().await {
            assert_eq!(req_epoch, epoch);
            assert_eq!(puback.packet_identifier, pkid);
            assert_eq!(puback.reason_code, PubAckReasonCode::Success);
            assert_eq!(puback.other_properties, properties);
        } else {
            panic!("Did not receive PubAck acknowledgement request");
        }
        // There are currently no other items in the channel
        assert_eq!(rx.len(), 0);
        // Now drop the token
        drop(completion_token);
        // There should still be no additional items in the channel (i.e. was only accepted once)
        assert_eq!(rx.len(), 0);
    }

    #[tokio::test]
    async fn pubrec_token_accept() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubRecOtherProperties {
            reason_string: Some("Test Success".into()),
            user_properties: vec![
                ("key1".into(), "value1".into()),
                ("key2".into(), "value2".into()),
            ],
        };
        let token = PubRecToken::new(pkid, epoch, tx.clone());
        let completion_token = token.accept(properties.clone()).await.unwrap();
        if let Some(AcknowledgementRequest::PubRecAccept(notifier, pubrec, req_epoch)) =
            rx.recv().await
        {
            assert_eq!(req_epoch, epoch);
            assert_eq!(pubrec.packet_identifier, pkid);
            assert_eq!(pubrec.reason_code, PubRecReasonCode::Success);
            assert_eq!(pubrec.other_properties, properties);

            let pubrel = PubRel {
                packet_identifier: pkid,
                reason_code: PubRelReasonCode::Success,
                other_properties: Default::default(),
            };
            let pubcomp_token = PubCompToken::new(pkid, epoch, tx);
            notifier.complete((pubrel.clone(), pubcomp_token)).unwrap();
            let (completion_pubrel, pubcomp_token) = completion_token.await.unwrap();
            assert_eq!(completion_pubrel, pubrel);
            drop(pubcomp_token);
            assert_default_pubcomp(rx.recv().await, pkid, epoch);
        } else {
            panic!("Did not receive PubRec acceptance request");
        }
    }

    #[tokio::test]
    async fn pubrec_token_reject() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubRecOtherProperties {
            reason_string: Some("Test Reject".into()),
            user_properties: vec![
                ("key1".into(), "value1".into()),
                ("key2".into(), "value2".into()),
            ],
        };
        let token = PubRecToken::new(pkid, epoch, tx);
        let completion_token = token
            .reject(PubRecReasonCode::NotAuthorized, properties.clone())
            .await
            .unwrap();
        if let Some(AcknowledgementRequest::PubRecReject(notifier, pubrec, req_epoch)) =
            rx.recv().await
        {
            assert_eq!(req_epoch, epoch);
            assert_eq!(pubrec.packet_identifier, pkid);
            assert_eq!(pubrec.reason_code, PubRecReasonCode::NotAuthorized);
            assert_eq!(pubrec.other_properties, properties);
            let completion_value = ();
            notifier.complete(completion_value).unwrap();
            assert_eq!(completion_token.await, Ok(completion_value));
        } else {
            panic!("Did not receive PubRec rejection request");
        }
    }

    #[tokio::test]
    async fn public_pubrec_accept_future_drop_before_poll() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let token = super::PubRecToken(PubRecToken::new(pkid, epoch, tx));
        let properties = crate::packet::PubRecProperties {
            reason_string: Some("not submitted".into()),
            user_properties: Vec::new(),
        };

        let accept = token.accept(properties);
        drop(accept);

        assert_default_pubrec(rx.recv().await, pkid, epoch);
    }

    #[tokio::test]
    async fn public_pubrec_reject_future_drop_before_poll() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let token = super::PubRecToken(PubRecToken::new(pkid, epoch, tx));
        let properties = crate::packet::PubRecProperties {
            reason_string: Some("not submitted".into()),
            user_properties: Vec::new(),
        };

        let reject = token.reject(crate::packet::PubRejectReason::NotAuthorized, properties);
        drop(reject);

        assert_default_pubrec(rx.recv().await, pkid, epoch);
    }

    #[tokio::test]
    async fn pubrec_token_drop_before_use() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let token = PubRecToken::<Bytes>::new(pkid, epoch, tx);
        drop(token);

        assert_eq!(rx.len(), 1);
        assert_default_pubrec(rx.recv().await, pkid, epoch);
        assert_eq!(rx.len(), 0);
    }

    #[tokio::test]
    async fn pubrec_token_drop_after_use() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubRecOtherProperties {
            reason_string: Some("Test Success".into()),
            user_properties: vec![("key".into(), "value".into())],
        };
        let token = PubRecToken::new(pkid, epoch, tx);
        let completion_token = token.accept(properties.clone()).await.unwrap();
        if let Some(AcknowledgementRequest::PubRecAccept(_, pubrec, req_epoch)) = rx.recv().await {
            assert_eq!(req_epoch, epoch);
            assert_eq!(pubrec.packet_identifier, pkid);
            assert_eq!(pubrec.reason_code, PubRecReasonCode::Success);
            assert_eq!(pubrec.other_properties, properties);
        } else {
            panic!("Did not receive PubRec acceptance request");
        }
        assert_eq!(rx.len(), 0);
        drop(completion_token);
        assert_eq!(rx.len(), 0);
    }

    #[tokio::test]
    async fn pubrel_token_confirm() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubRelOtherProperties {
            reason_string: Some("Test Confirm".into()),
            user_properties: vec![
                ("key1".into(), "value1".into()),
                ("key2".into(), "value2".into()),
            ],
        };
        let token = PubRelToken::new(pkid, epoch, tx);
        let completion_token = token.confirm(properties.clone()).await.unwrap();
        if let Some(AcknowledgementRequest::PubRel(notifier, pubrel, req_epoch)) = rx.recv().await {
            assert_eq!(req_epoch, epoch);
            assert_eq!(pubrel.packet_identifier, pkid);
            assert_eq!(pubrel.reason_code, PubRelReasonCode::Success);
            assert_eq!(pubrel.other_properties, properties);

            let pubcomp = PubComp {
                packet_identifier: pkid,
                reason_code: PubCompReasonCode::Success,
                other_properties: Default::default(),
            };
            notifier.complete(pubcomp.clone()).unwrap();
            assert_eq!(completion_token.await, Ok(pubcomp));
        } else {
            panic!("Did not receive PubRel confirmation request");
        }
    }

    #[tokio::test]
    async fn public_pubrel_confirm_future_drop_before_poll() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let token = super::PubRelToken(PubRelToken::new(pkid, epoch, tx));
        let properties = crate::packet::PubRelProperties {
            reason_string: Some("not submitted".into()),
            user_properties: Vec::new(),
        };

        let confirm = token.confirm(properties);
        drop(confirm);

        assert_default_pubrel(rx.recv().await, pkid, epoch);
    }

    #[tokio::test]
    async fn pubrel_token_drop_before_use() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let token = PubRelToken::<Bytes>::new(pkid, epoch, tx);
        drop(token);

        assert_eq!(rx.len(), 1);
        assert_default_pubrel(rx.recv().await, pkid, epoch);
        assert_eq!(rx.len(), 0);
    }

    #[tokio::test]
    async fn pubrel_token_drop_after_use() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubRelOtherProperties {
            reason_string: Some("Test Confirm".into()),
            user_properties: vec![("key".into(), "value".into())],
        };
        let token = PubRelToken::new(pkid, epoch, tx);
        let completion_token = token.confirm(properties.clone()).await.unwrap();
        if let Some(AcknowledgementRequest::PubRel(_, pubrel, req_epoch)) = rx.recv().await {
            assert_eq!(req_epoch, epoch);
            assert_eq!(pubrel.packet_identifier, pkid);
            assert_eq!(pubrel.reason_code, PubRelReasonCode::Success);
            assert_eq!(pubrel.other_properties, properties);
        } else {
            panic!("Did not receive PubRel confirmation request");
        }
        assert_eq!(rx.len(), 0);
        drop(completion_token);
        assert_eq!(rx.len(), 0);
    }

    #[tokio::test]
    async fn pubcomp_token_confirm() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubCompOtherProperties {
            reason_string: Some("Test Confirm".into()),
            user_properties: vec![
                ("key1".into(), "value1".into()),
                ("key2".into(), "value2".into()),
            ],
        };
        let token = PubCompToken::new(pkid, epoch, tx);
        let completion_token = token.confirm(properties.clone()).await.unwrap();
        if let Some(AcknowledgementRequest::PubComp(notifier, pubcomp, req_epoch)) = rx.recv().await
        {
            assert_eq!(req_epoch, epoch);
            assert_eq!(pubcomp.packet_identifier, pkid);
            assert_eq!(pubcomp.reason_code, PubCompReasonCode::Success);
            assert_eq!(pubcomp.other_properties, properties);
            let completion_value = ();
            notifier.complete(completion_value).unwrap();
            assert_eq!(completion_token.await, Ok(completion_value));
        } else {
            panic!("Did not receive PubComp confirmation request");
        }
    }

    #[tokio::test]
    async fn public_pubcomp_confirm_future_drop_before_poll() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let token = super::PubCompToken(PubCompToken::new(pkid, epoch, tx));
        let properties = crate::packet::PubCompProperties {
            reason_string: Some("not submitted".into()),
            user_properties: Vec::new(),
        };

        let confirm = token.confirm(properties);
        drop(confirm);

        assert_default_pubcomp(rx.recv().await, pkid, epoch);
    }

    #[tokio::test]
    async fn pubcomp_token_drop_before_use() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let token = PubCompToken::<Bytes>::new(pkid, epoch, tx);
        drop(token);

        assert_eq!(rx.len(), 1);
        assert_default_pubcomp(rx.recv().await, pkid, epoch);
        assert_eq!(rx.len(), 0);
    }

    #[tokio::test]
    async fn pubcomp_token_drop_after_use() {
        let (tx, mut rx) = acknowledgement_channel();
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubCompOtherProperties {
            reason_string: Some("Test Confirm".into()),
            user_properties: vec![("key".into(), "value".into())],
        };
        let token = PubCompToken::new(pkid, epoch, tx);
        let completion_token = token.confirm(properties.clone()).await.unwrap();
        if let Some(AcknowledgementRequest::PubComp(_, pubcomp, req_epoch)) = rx.recv().await {
            assert_eq!(req_epoch, epoch);
            assert_eq!(pubcomp.packet_identifier, pkid);
            assert_eq!(pubcomp.reason_code, PubCompReasonCode::Success);
            assert_eq!(pubcomp.other_properties, properties);
        } else {
            panic!("Did not receive PubComp confirmation request");
        }
        assert_eq!(rx.len(), 0);
        drop(completion_token);
        assert_eq!(rx.len(), 0);
    }

    #[test]
    fn bulk_drop_of_all_token_types_without_receiver_progress_is_synchronous() {
        let (tx, rx) = acknowledgement_channel();
        let epoch = 3;

        for packet_identifier in (1..=1_024).step_by(4) {
            drop(PubAckToken::<Bytes>::new(
                PacketIdentifier::new(packet_identifier).unwrap(),
                epoch,
                tx.clone(),
            ));
            drop(PubRecToken::<Bytes>::new(
                PacketIdentifier::new(packet_identifier + 1).unwrap(),
                epoch,
                tx.clone(),
            ));
            drop(PubRelToken::<Bytes>::new(
                PacketIdentifier::new(packet_identifier + 2).unwrap(),
                epoch,
                tx.clone(),
            ));
            drop(PubCompToken::<Bytes>::new(
                PacketIdentifier::new(packet_identifier + 3).unwrap(),
                epoch,
                tx.clone(),
            ));
        }

        assert_eq!(rx.len(), 1_024);
    }

    #[tokio::test]
    async fn detached_channel_errors_on_explicit_use_without_panicking_on_drop() {
        let (tx, rx) = acknowledgement_channel();
        let epoch = 3;
        drop(rx);

        let puback =
            PubAckToken::<Bytes>::new(PacketIdentifier::new(1).unwrap(), epoch, tx.clone());
        assert!(puback.accept(Default::default()).await.is_err());

        let pubrec_accept =
            PubRecToken::<Bytes>::new(PacketIdentifier::new(2).unwrap(), epoch, tx.clone());
        assert!(pubrec_accept.accept(Default::default()).await.is_err());

        let pubrec_reject =
            PubRecToken::<Bytes>::new(PacketIdentifier::new(3).unwrap(), epoch, tx.clone());
        assert!(
            pubrec_reject
                .reject(PubRecReasonCode::NotAuthorized, Default::default())
                .await
                .is_err()
        );

        let pubrel =
            PubRelToken::<Bytes>::new(PacketIdentifier::new(4).unwrap(), epoch, tx.clone());
        assert!(pubrel.confirm(Default::default()).await.is_err());

        let pubcomp =
            PubCompToken::<Bytes>::new(PacketIdentifier::new(5).unwrap(), epoch, tx.clone());
        assert!(pubcomp.confirm(Default::default()).await.is_err());

        drop(PubAckToken::<Bytes>::new(
            PacketIdentifier::new(6).unwrap(),
            epoch,
            tx.clone(),
        ));
        drop(PubRecToken::<Bytes>::new(
            PacketIdentifier::new(7).unwrap(),
            epoch,
            tx.clone(),
        ));
        drop(PubRelToken::<Bytes>::new(
            PacketIdentifier::new(8).unwrap(),
            epoch,
            tx.clone(),
        ));
        drop(PubCompToken::<Bytes>::new(
            PacketIdentifier::new(9).unwrap(),
            epoch,
            tx,
        ));
    }
}
