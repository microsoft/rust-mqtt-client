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
    /// Can only be successfully used during the same MQTT session generation in which it was
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
    /// Can only be successfully used during the same MQTT session generation in which it was
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
    /// Can only be successfully used during the same MQTT session generation in which it was
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
    /// Can only be successfully used during the same MQTT session generation in which it was
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
            let ct = PubAckToken::inner_send(&self.tx, self.pkid, properties, reason, self.epoch)?;
            self.triggered = true;
            Ok(ct)
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
        generation: u64,
        tx: UnboundedSender<AcknowledgementRequest<S>>,
        triggered: bool,
    }

    impl<S> PubRecToken<S>
    where
        S: Shared,
    {
        pub(crate) fn new(
            pkid: PacketIdentifier,
            generation: u64,
            tx: UnboundedSender<AcknowledgementRequest<S>>,
        ) -> Self {
            Self {
                pkid,
                generation,
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
        /// Can only be successfully used during the same MQTT session generation in which it was
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
        /// Can only be successfully used during the same MQTT session generation in which it was
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
            let ct = Self::inner_accept(&self.tx, self.pkid, properties, self.generation)?;
            self.triggered = true;
            Ok(ct)
        }

        fn inner_accept(
            tx: &UnboundedSender<AcknowledgementRequest<S>>,
            packet_identifier: PacketIdentifier,
            other_properties: PubRecOtherProperties<S>,
            generation: u64,
        ) -> Result<PubRecAcceptCompletionToken<S>, DetachedError> {
            let (notifier, token) = completion_pair();
            let pubrec = PubRec {
                packet_identifier,
                reason_code: PubRecReasonCode::Success,
                other_properties,
            };
            tx.send(AcknowledgementRequest::PubRecAccept(
                notifier, pubrec, generation,
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
                    notifier,
                    pubrec,
                    self.generation,
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
            if !self.triggered {
                let _ =
                    Self::inner_accept(&self.tx, self.pkid, Default::default(), self.generation);
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
        generation: u64,
        tx: UnboundedSender<AcknowledgementRequest<S>>,
        triggered: bool,
    }

    impl<S> PubRelToken<S>
    where
        S: Shared,
    {
        pub(crate) fn new(
            pkid: PacketIdentifier,
            generation: u64,
            tx: UnboundedSender<AcknowledgementRequest<S>>,
        ) -> Self {
            Self {
                pkid,
                generation,
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
        /// Can only be successfully used during the same MQTT session generation in which it was
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
            let ct = Self::inner_send(&self.tx, self.pkid, properties, self.generation)?;
            self.triggered = true;
            Ok(ct)
        }

        fn inner_send(
            tx: &UnboundedSender<AcknowledgementRequest<S>>,
            packet_identifier: PacketIdentifier,
            other_properties: PubRelOtherProperties<S>,
            generation: u64,
        ) -> Result<PubRelConfirmCompletionToken<S>, DetachedError> {
            let (notifier, token) = completion_pair();
            let pubrel = PubRel {
                packet_identifier,
                reason_code: PubRelReasonCode::Success,
                other_properties,
            };
            tx.send(AcknowledgementRequest::PubRel(notifier, pubrel, generation))
                .map_err(|_| DetachedError {})?;
            Ok(PubRelConfirmCompletionToken(token))
        }
    }

    impl<S> Drop for PubRelToken<S>
    where
        S: Shared,
    {
        fn drop(&mut self) {
            if !self.triggered {
                let _ = Self::inner_send(&self.tx, self.pkid, Default::default(), self.generation);
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
        generation: u64,
        tx: UnboundedSender<AcknowledgementRequest<S>>,
        triggered: bool,
    }

    impl<S> PubCompToken<S>
    where
        S: Shared,
    {
        pub(crate) fn new(
            pkid: PacketIdentifier,
            generation: u64,
            tx: UnboundedSender<AcknowledgementRequest<S>>,
        ) -> Self {
            Self {
                pkid,
                generation,
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
        /// Can only be successfully used during the same MQTT session generation in which it was
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
            let ct = Self::inner_send(&self.tx, self.pkid, properties, self.generation)?;
            self.triggered = true;
            Ok(ct)
        }

        fn inner_send(
            tx: &UnboundedSender<AcknowledgementRequest<S>>,
            packet_identifier: PacketIdentifier,
            other_properties: PubCompOtherProperties<S>,
            generation: u64,
        ) -> Result<PubCompConfirmCompletionToken, DetachedError> {
            let (notifier, token) = completion_pair();
            let pubcomp = PubComp {
                packet_identifier,
                reason_code: PubCompReasonCode::Success,
                other_properties,
            };
            tx.send(AcknowledgementRequest::PubComp(
                notifier, pubcomp, generation,
            ))
            .map_err(|_| DetachedError {})?;
            Ok(PubCompConfirmCompletionToken(token))
        }
    }

    impl<S> Drop for PubCompToken<S>
    where
        S: Shared,
    {
        fn drop(&mut self) {
            if !self.triggered {
                let _ = Self::inner_send(&self.tx, self.pkid, Default::default(), self.generation);
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
    use crate::mqtt_proto::{PacketIdentifier, PubAckOtherProperties, PubAckReasonCode};

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
        let ct = token.accept(properties.clone()).await.unwrap();
        if let Some(AcknowledgementRequest::PubAck(notifier, puback, req_epoch)) = rx.recv().await {
            // The correct data was sent in the acknowledgement request
            assert_eq!(req_epoch, epoch);
            assert_eq!(puback.packet_identifier, pkid);
            assert_eq!(puback.reason_code, PubAckReasonCode::Success);
            assert_eq!(puback.other_properties, properties);
            // Using the acknowledgement request notifier completes the completion token that was returned
            let completion_value = ();
            notifier.complete(completion_value).unwrap();
            assert_eq!(ct.await, Ok(completion_value));
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
        let ct = token
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
            assert_eq!(ct.await, Ok(completion_value));
        } else {
            panic!("Did not receive PubAck acknowledgement request");
        }
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
        let ct = token.accept(properties.clone()).await.unwrap();
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
        drop(ct);
        // There should still be no additional items in the channel (i.e. was only accepted once)
        assert_eq!(rx.len(), 0);
    }

    #[test]
    fn bulk_drop_without_receiver_progress_is_synchronous() {
        let (tx, rx) = acknowledgement_channel();
        let epoch = 3;

        for packet_identifier in 1..=1_024 {
            let token = PubAckToken::<Bytes>::new(
                PacketIdentifier::new(packet_identifier).unwrap(),
                epoch,
                tx.clone(),
            );
            drop(token);
        }

        assert_eq!(rx.len(), 1_024);
    }

    #[tokio::test]
    async fn detached_session_rejects_manual_submission_and_ignores_drop() {
        let (tx, rx) = acknowledgement_channel();
        let epoch = 3;
        drop(rx);

        let explicit =
            PubAckToken::<Bytes>::new(PacketIdentifier::new(1).unwrap(), epoch, tx.clone());
        assert!(explicit.accept(Default::default()).await.is_err());

        let automatic = PubAckToken::<Bytes>::new(PacketIdentifier::new(2).unwrap(), epoch, tx);
        drop(automatic);
    }
}
