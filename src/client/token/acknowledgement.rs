// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! Synchronization for portable triggering of acknowledgement flows

use crate::client::channel_data::AcknowledgementRequest;
use crate::client::token::{CompletionToken, completion_pair};
use crate::error::ClientError;
use crate::packet::{
    PacketIdentifier, PubAck, PubAckProperties, PubAckReason, PubComp, PubCompProperties,
    PubRecProperties, PubRejectReason, PubRel, PubRelProperties,
};

/// Token that allows the user to acknowledge a received PUBLISH on QoS 1 with a PUBACK.
#[derive(Debug)]
pub struct PubAckToken {
    pkid: PacketIdentifier,
    epoch: u64,
    tx: tokio::sync::mpsc::Sender<AcknowledgementRequest>,
    triggered: bool,
}

impl PubAckToken {
    pub(crate) fn new(
        pkid: PacketIdentifier,
        epoch: u64,
        tx: tokio::sync::mpsc::Sender<AcknowledgementRequest>,
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
        self.inner_send(properties, PubAckReason::Success).await
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
        self.inner_send(properties, reason.into()).await
    }

    async fn inner_send(
        mut self,
        properties: PubAckProperties,
        reason: PubAckReason,
    ) -> Result<CompletionToken<()>, ClientError> {
        let (notifier, token) = completion_pair();
        let puback = PubAck {
            packet_identifier: self.pkid,
            reason,
            properties,
        };
        self.tx
            .send(AcknowledgementRequest::PubAck(notifier, puback, self.epoch))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        self.triggered = true;
        Ok(token)
    }
}

impl Drop for PubAckToken {
    fn drop(&mut self) {
        // Must acknowledge if the token was not used in order to prevent locking the
        // ack ordering flow.
        if !self.triggered {
            // Clone tx because we can't move out of self.
            // TODO: Consider using Option in the future for better performance
            let owned_self = std::mem::replace(
                self,
                PubAckToken {
                    pkid: self.pkid,
                    epoch: self.epoch,
                    tx: self.tx.clone(),
                    triggered: true,
                },
            );
            tokio::task::spawn(async move { owned_self.accept(PubAckProperties::default()).await });
        }
    }
}

/// Token that allows the user to acknowledge a received PUBLISH on QoS 2 with a PUBREC.
#[derive(Debug)]
pub struct PubRecToken {
    pkid: PacketIdentifier,
    epoch: u64,
    tx: tokio::sync::mpsc::Sender<AcknowledgementRequest>,
    triggered: bool,
}

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
#[derive(Debug)]
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
#[derive(Debug)]
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

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn puback_token_accept() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubAckProperties {
            reason_string: Some("Test Success".to_string()),
            user_properties: vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ],
        };
        let token = PubAckToken::new(pkid, epoch, tx);
        let completion_token = token.accept(properties.clone()).await.unwrap();
        if let Some(AcknowledgementRequest::PubAck(notifier, puback, req_epoch)) = rx.recv().await {
            // The correct data was sent in the acknowledgement request
            assert_eq!(req_epoch, epoch);
            assert_eq!(puback.packet_identifier, pkid);
            assert_eq!(puback.reason, PubAckReason::Success);
            assert_eq!(puback.properties, properties);
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
        let properties = PubAckProperties {
            reason_string: Some("Test Reject".to_string()),
            user_properties: vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ],
        };
        let token = PubAckToken::new(pkid, epoch, tx);
        let completion_token = token
            .reject(PubRejectReason::NotAuthorized, properties.clone())
            .await
            .unwrap();
        if let Some(AcknowledgementRequest::PubAck(notifier, puback, req_epoch)) = rx.recv().await {
            assert_eq!(req_epoch, epoch);
            assert_eq!(puback.packet_identifier, pkid);
            assert_eq!(puback.reason, PubAckReason::NotAuthorized);
            assert_eq!(puback.properties, properties);
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
        let token = PubAckToken::new(pkid, epoch, tx);
        // Drop the token without accepting or rejecting it
        drop(token);
        // It was accepted automatically with default properties
        if let Some(AcknowledgementRequest::PubAck(_, puback, req_epoch)) = rx.recv().await {
            assert_eq!(req_epoch, epoch);
            assert_eq!(puback.packet_identifier, pkid);
            assert_eq!(puback.reason, PubAckReason::Success);
            assert_eq!(puback.properties, PubAckProperties::default());
        } else {
            panic!("Did not receive PubAck acknowledgement request");
        }
        // There are no additional items in the channel (i.e. was only accepted once)
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        assert_eq!(rx.len(), 0);
    }

    #[tokio::test]
    async fn puback_token_drop_after_use() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let pkid = PacketIdentifier::new(1).unwrap();
        let epoch = 3;
        let properties = PubAckProperties {
            reason_string: Some("Test Success".to_string()),
            user_properties: vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ],
        };
        let token = PubAckToken::new(pkid, epoch, tx);
        // Use the token to send an acceptance
        let completion_token = token.accept(properties.clone()).await.unwrap();
        if let Some(AcknowledgementRequest::PubAck(_, puback, req_epoch)) = rx.recv().await {
            assert_eq!(req_epoch, epoch);
            assert_eq!(puback.packet_identifier, pkid);
            assert_eq!(puback.reason, PubAckReason::Success);
            assert_eq!(puback.properties, properties);
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
