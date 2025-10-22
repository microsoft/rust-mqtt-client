// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! Token types for awaiting completion of MQTT operations and issuing acknowledgements.

// TODO: Remove when possible.
#![allow(unused_variables)]
#![allow(clippy::unused_async)]

pub use crate::packet::{PubAck, PubComp, PubRec, PubRel, SubAck, UnsubAck};
pub use acknowledgement::{PubAckToken, PubCompToken, PubRecToken, PubRelToken};
pub use completion::CompletionToken;
pub(crate) use completion::{CompletionNotifier, completion_pair};

mod acknowledgement;
mod completion;

// Aliases for completion notifier types.
// For internal use where we'd prefer to avoid the mix of user-facing and internal packet types.
pub(crate) type PublishQoS0CompletionNotifier = CompletionNotifier<()>;
pub(crate) type PublishQoS1CompletionNotifier = CompletionNotifier<PubAck>;
pub(crate) type PublishQoS2CompletionNotifier = CompletionNotifier<(PubRec, Option<PubRelToken>)>;
pub(crate) type SubscribeCompletionNotifier = CompletionNotifier<SubAck>;
pub(crate) type UnsubscribeCompletionNotifier = CompletionNotifier<UnsubAck>;
pub(crate) type PubAckCompletionNotifier = CompletionNotifier<()>;
pub(crate) type PubRecAcceptCompletionNotifier = CompletionNotifier<(PubRel, PubCompToken)>;
pub(crate) type PubRecRejectCompletionNotifier = CompletionNotifier<()>;
pub(crate) type PubRelCompletionNotifier = CompletionNotifier<PubComp>;
pub(crate) type PubCompCompletionNotifier = CompletionNotifier<()>;
