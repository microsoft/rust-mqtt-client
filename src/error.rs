// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Error types for the MQTT client library.

use thiserror::Error;

// TODO: It may make more sense for these to be exported from elsewhere and just exposed here.

// DISCUSS: Is too large worth preventing? Technically, you're allowed to send 256mb, even though the broker will tell you it
// can reject anything above a certain size. Worth validating? Or should we just let the broker reject it? Technically the only
// time you aren't allowed per spec to send something large is in the SUBACK/UNSUBACK/PUBACK/PUBCOMP/PUBREL/PUBREC flow.
// I think we probably still need the too large error just simply because the 256mb hard limit exists.
//
// I would also say that, it's fairly impractical to expect the application to simply know the max size, given that, we only find it
// out in the CONNACK, and so it requires the user to set up state for the application to track it, which is... odd
//
// On the other hand, you can hardly validate it before receiving the CONNACK, so it isn't well suited to a ClientError, it's probably
// more of a CompletionError thing? If so, is the only Client Error really that it became detached?

// TODO: In a real implementation, this (and all other errors) would be a struct, not an enum. Should it also contain T where T is the
// packet type, so you get back the packet data on failure? e.g. ClientError<Publish>? where error.packet() -> Publish?

/// Indicates a failure in the MQTT client before any operation takes place.
#[derive(Debug, Clone, Error)]
pub enum ClientError {
    #[error("Communication channels with the client have been closed")]
    DetachedClient,
    #[error("The packet is too large to be sent according to the server's maximum packet size")]
    TooLarge, // This could happen even without payload due to large user properties, of, say, a subscribe
              // TODO: are we getting rid of TooLarge?
}

/// Indicates that the MQTT operation did not complete successfully
/// NOTE: Does NOT contain the reason code as an enum, as it must be agnostic to the operation type.
#[derive(Debug, Clone, Error)]
pub struct OperationFailure {
    pub reason: String,
}

impl From<String> for OperationFailure {
    fn from(value: String) -> Self {
        OperationFailure { reason: value }
    }
}

impl std::fmt::Display for OperationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Operation failed: {}", self.reason)
    }
}
