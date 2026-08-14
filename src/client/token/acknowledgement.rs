// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Controls for acknowledging incoming MQTT packet flows.
//!
//! Acknowledgement tokens are protocol controls, not passive completion observers. For the
//! supported QoS 1 flow, dropping an unused [`PubAckToken`] attempts to submit a successful PUBACK
//! with default properties so acknowledgement ordering can continue. Retain the token to choose
//! when to acknowledge, reject the publish, or supply acknowledgement properties.
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
//! QoS 2 acknowledgement token types reserve APIs with the same ownership and default-on-drop
//! model, but end-to-end QoS 2 publishing and receiving are not yet supported.

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
///
/// Receiving at QoS 2 is not yet supported.
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
    /// Can only be successfully used during the same session epoch on which it was received.
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
    /// Can only be successfully used during the same session epoch on which it was received.
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
///
/// QoS 2 publishing is not yet supported end to end.
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
    /// Can only be successfully used during the same session epoch on which it was received.
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
///
/// Receiving at QoS 2 is not yet supported.
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
    /// Can only be successfully used during the same session epoch on which it was received.
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

    use futures_executor::block_on;
    use tokio::sync::mpsc::Sender;

    use crate::buffer_pool::Shared;
    use crate::client::channel_data::AcknowledgementRequest;
    use crate::client::token::completion::buffered::{
        PubAckCompletionToken, PubCompConfirmCompletionToken, PubRecAcceptCompletionToken,
        PubRecRejectCompletionToken, PubRelConfirmCompletionToken, completion_pair,
    };
    use crate::error::DetachedError;
    use crate::mqtt_proto::{
        PacketIdentifier, PubAck, PubAckOtherProperties, PubAckReasonCode, PubCompOtherProperties,
        PubRecOtherProperties, PubRecReasonCode, PubRelOtherProperties,
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
        tx: Sender<AcknowledgementRequest<S>>,
        triggered: bool,
    }

    impl<S> PubAckToken<S>
    where
        S: Shared,
    {
        pub(crate) fn new(
            pkid: PacketIdentifier,
            epoch: u64,
            tx: Sender<AcknowledgementRequest<S>>,
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
                PubAckToken::inner_send(&self.tx, self.pkid, properties, reason, self.epoch)
                    .await?;
            self.triggered = true;
            Ok(completion)
        }

        /// Internal helper to send the acknowledgement request.
        /// Does not operate on self in order to allow for use in drop efficiently.
        async fn inner_send(
            tx: &Sender<AcknowledgementRequest<S>>,
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
                .await
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
                // TODO: Consider using Option to avoid cloning for better performance
                let tx = self.tx.clone();
                let pkid = self.pkid;
                let epoch = self.epoch;
                std::thread::spawn(move || {
                    block_on(async move {
                        let _ = PubAckToken::inner_send(
                            &tx,
                            pkid,
                            Default::default(),
                            PubAckReasonCode::Success,
                            epoch,
                        )
                        .await;
                    });
                });
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
        tx: Sender<AcknowledgementRequest<S>>,
        triggered: bool,
    }

    impl<S> PubRecToken<S>
    where
        S: Shared,
    {
        pub(crate) fn new(pkid: PacketIdentifier, tx: Sender<AcknowledgementRequest<S>>) -> Self {
            Self {
                pkid,
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
        /// Can only be successfully used during the same session epoch on which it was received.
        ///
        /// # Cancellation
        ///
        /// Canceling this operation before it returns drops the token, so its intended default
        /// acknowledgement behavior applies.
        pub async fn accept(
            self,
            properties: PubRecOtherProperties<S>,
        ) -> Result<PubRecAcceptCompletionToken<S>, DetachedError> {
            unimplemented!()
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
        /// Can only be successfully used during the same session epoch on which it was received.
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
            unimplemented!()
        }
    }

    impl<S> Drop for PubRecToken<S>
    where
        S: Shared,
    {
        fn drop(&mut self) {
            // Must accept
            unimplemented!()
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
        tx: Sender<AcknowledgementRequest<S>>,
        triggered: bool,
    }

    impl<S> PubRelToken<S>
    where
        S: Shared,
    {
        pub(crate) fn new(pkid: PacketIdentifier, tx: Sender<AcknowledgementRequest<S>>) -> Self {
            Self {
                pkid,
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
        /// Can only be successfully used during the same session epoch on which it was received.
        ///
        /// # Cancellation
        ///
        /// Canceling this operation before it returns drops the token, so its intended default
        /// confirmation behavior applies.
        pub async fn confirm(
            self,
            properties: PubRelOtherProperties<S>,
        ) -> Result<PubRelConfirmCompletionToken<S>, DetachedError> {
            unimplemented!()
        }
    }

    impl<S> Drop for PubRelToken<S>
    where
        S: Shared,
    {
        fn drop(&mut self) {
            // Must confirm
            unimplemented!()
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
        tx: Sender<AcknowledgementRequest<S>>,
        triggered: bool,
    }

    impl<S> PubCompToken<S>
    where
        S: Shared,
    {
        pub(crate) fn new(pkid: PacketIdentifier, tx: Sender<AcknowledgementRequest<S>>) -> Self {
            Self {
                pkid,
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
        /// Can only be successfully used during the same session epoch on which it was received.
        ///
        /// # Cancellation
        ///
        /// Canceling this operation before it returns drops the token, so its intended default
        /// confirmation behavior applies.
        pub async fn confirm(
            self,
            properties: PubCompOtherProperties<S>,
        ) -> Result<PubCompConfirmCompletionToken, DetachedError> {
            unimplemented!()
        }
    }

    impl<S> Drop for PubCompToken<S>
    where
        S: Shared,
    {
        fn drop(&mut self) {
            // Must confirm
            unimplemented!()
        }
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use futures_util::FutureExt;

    use super::buffered::*;
    use crate::client::channel_data::AcknowledgementRequest;
    use crate::mqtt_proto::{PacketIdentifier, PubAckOtherProperties, PubAckReasonCode};

    #[tokio::test]
    async fn puback_token_accept() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
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
    async fn puback_token_drop_before_use() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let token = PubAckToken::<Bytes>::new(pkid, epoch, tx);
        // Drop the token without accepting or rejecting it
        drop(token);
        // It was accepted automatically with default properties
        if let Some(AcknowledgementRequest::PubAck(_, puback, req_epoch)) = rx.recv().await {
            assert_eq!(req_epoch, epoch);
            assert_eq!(puback.packet_identifier, pkid);
            assert_eq!(puback.reason_code, PubAckReasonCode::Success);
            assert_eq!(puback.other_properties, Default::default());
        } else {
            panic!("Did not receive PubAck acknowledgement request");
        }
        // There are no additional items in the channel (i.e. was only accepted once)
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        assert_eq!(rx.len(), 0);
    }

    #[tokio::test]
    async fn public_puback_accept_future_drop_before_poll() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let token = super::PubAckToken(PubAckToken::new(pkid, epoch, tx));
        let properties = crate::packet::PubAckProperties {
            reason_string: Some("not submitted".into()),
            user_properties: Vec::new(),
        };

        let accept = token.accept(properties);
        drop(accept);

        if let Some(AcknowledgementRequest::PubAck(_, puback, req_epoch)) = rx.recv().await {
            assert_eq!(req_epoch, epoch);
            assert_eq!(puback.packet_identifier, pkid);
            assert_eq!(puback.reason_code, PubAckReasonCode::Success);
            assert_eq!(puback.other_properties, Default::default());
        } else {
            panic!("Did not receive automatic PubAck acknowledgement request");
        }
    }

    #[tokio::test]
    async fn puback_accept_cancelled_while_channel_full_falls_back_to_default() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let epoch = 3;

        let first_pkid = PacketIdentifier::new(1).unwrap();
        let first_token = PubAckToken::new(first_pkid, epoch, tx.clone());
        drop(first_token.accept(Default::default()).await.unwrap());

        let cancelled_pkid = PacketIdentifier::new(2).unwrap();
        let cancelled_token = super::PubAckToken(PubAckToken::new(cancelled_pkid, epoch, tx));
        let properties = crate::packet::PubAckProperties {
            reason_string: Some("cancelled submission".into()),
            user_properties: Vec::new(),
        };
        let accept = cancelled_token.accept(properties);
        assert!(accept.now_or_never().is_none());

        let Some(AcknowledgementRequest::PubAck(_, first_puback, _)) = rx.recv().await else {
            panic!("Did not receive the first PubAck acknowledgement request");
        };
        assert_eq!(first_puback.packet_identifier, first_pkid);

        let fallback = tokio::time::timeout(tokio::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("Timed out waiting for automatic PubAck acknowledgement request");
        let Some(AcknowledgementRequest::PubAck(_, puback, req_epoch)) = fallback else {
            panic!("Did not receive automatic PubAck acknowledgement request");
        };
        assert_eq!(req_epoch, epoch);
        assert_eq!(puback.packet_identifier, cancelled_pkid);
        assert_eq!(puback.reason_code, PubAckReasonCode::Success);
        assert_eq!(puback.other_properties, Default::default());
    }

    #[tokio::test]
    async fn puback_token_drop_after_use() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
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
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        assert_eq!(rx.len(), 0);
    }
}
