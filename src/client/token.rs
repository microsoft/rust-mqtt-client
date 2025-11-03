// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! Token types for awaiting completion of MQTT operations and issuing acknowledgements.

// TODO: Remove when possible.
#![allow(unused_variables)]
#![allow(clippy::unused_async)]

use crate::client::AuthResponse;
use crate::client::channel_data::ReauthResponse;
use crate::mqtt_proto::{PubAck, PubComp, PubRec, PubRel, SubAck, UnsubAck};
pub use acknowledgement::{
    AckHandle, PubAckCompletionToken, PubAckToken, PubCompConfirmCompletionToken, PubCompToken,
    PubRecAcceptCompletionToken, PubRecRejectCompletionToken, PubRecToken,
    PubRelConfirmCompletionToken, PubRelToken,
};
pub(crate) use completion::{CompletionError, CompletionToken};
pub(crate) use completion::{CompletionNotifier, completion_pair};

mod acknowledgement;
pub(crate) mod completion;

// Aliases for completion notifier types.
// For internal use where we'd prefer to avoid the mix of user-facing and internal packet types.
pub(crate) type PublishQoS0CompletionNotifier = CompletionNotifier<()>;
pub(crate) type PublishQoS1CompletionNotifier<S> = CompletionNotifier<PubAck<S>>;
pub(crate) type PublishQoS2CompletionNotifier<S> =
    CompletionNotifier<(PubRec<S>, Option<PubRelToken<S>>)>;
pub(crate) type SubscribeCompletionNotifier<S> = CompletionNotifier<SubAck<S>>;
pub(crate) type UnsubscribeCompletionNotifier<S> = CompletionNotifier<UnsubAck<S>>;
pub(crate) type PubAckCompletionNotifier = CompletionNotifier<()>;
pub(crate) type PubRecAcceptCompletionNotifier<S> =
    CompletionNotifier<(PubRel<S>, PubCompToken<S>)>;
pub(crate) type PubRecRejectCompletionNotifier = CompletionNotifier<()>;
pub(crate) type PubRelCompletionNotifier<S> = CompletionNotifier<PubComp<S>>;
pub(crate) type PubCompCompletionNotifier = CompletionNotifier<()>;
pub(crate) type AuthCompletionNotifier = CompletionNotifier<AuthResponse>;
pub(crate) type ReauthCompletionNotifier<S> = CompletionNotifier<ReauthResponse<S>>;
