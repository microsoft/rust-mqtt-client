// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Futures for observing operation-specific completion events.
//!
//! Completion tokens are returned after an operation has been accepted by the client. Awaiting a
//! token reports the operation-specific completion event. Dropping a token does not cancel or undo
//! the accepted operation.

use bytes::Bytes;
use thiserror::Error;

/// Indicates a failure that occurred during the completion of an MQTT operation.
#[derive(Clone, PartialEq, Debug, Error)]
pub enum CompletionError {
    /// The client session was dropped before the operation produced a result.
    ///
    /// A completion token does not keep its [`crate::client::ConnectHandle`] or
    /// [`crate::client::Connection`] alive.
    #[error("Communication channels with the client have been closed")]
    Detached,
    /// The client canceled the operation because the connection ended or its MQTT session
    /// expired.
    #[error("The operation was canceled due to {0}")]
    Canceled(String),
}

// TODO: can we make this only available in the crate?
macro_rules! make_completion_token_ty {
    (
        $(#[$meta:meta])*
        $vis:vis struct $token_ty:ident
        $( < $($ty_param_name:ident : $ty_param_bound:path ),* > )?
        (CompletionToken< $element_ty:ty >)
    ) => {
        $(#[$meta])*
        #[derive(Debug)]
        $vis struct $token_ty $(< $($ty_param_name : $ty_param_bound),* >)? (
            pub(crate) crate::client::token::completion::buffered::CompletionToken<$element_ty>
        );

        impl $(< $($ty_param_name : $ty_param_bound),* >)? std::future::Future for $token_ty $(< $($ty_param_name ),* >)? {
            type Output = Result<$element_ty, $crate::client::token::completion::CompletionError>;

            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                std::pin::Pin::new(&mut self.0).poll(cx)
            }
        }
    };

    (
        $(#[$meta:meta])*
        $vis:vis struct $token_ty:ident
        (CompletionToken< $original_element_ty:ty > -> $element_ty:ty $map_fn:block )
    ) => {
        $(#[$meta])*
        #[derive(Debug)]
        $vis struct $token_ty(pub(crate) buffered::CompletionToken<$original_element_ty>);

        impl std::future::Future for $token_ty {
            type Output = Result<$element_ty, $crate::client::token::completion::CompletionError>;

            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                match std::pin::Pin::new(&mut self.0).poll(cx) {
                    std::task::Poll::Ready(Ok(value)) => {
                        std::task::Poll::Ready(Ok(($map_fn)(value)))
                    }
                    std::task::Poll::Ready(Err(error)) => {
                        std::task::Poll::Ready(Err(error))
                    }
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            }
        }
    };
}

make_completion_token_ty!(
    /// Completion token returned by [`crate::client::Client::publish_qos0`].
    ///
    /// Awaiting this token returns `Ok(())` when the session releases the PUBLISH for transmission,
    /// or a [`CompletionError`] if the accepted operation cannot complete.
    pub struct PublishQoS0CompletionToken(CompletionToken<()>)
);

make_completion_token_ty!(
    /// Completion token returned by [`crate::client::Client::publish_qos1`].
    ///
    /// Awaiting this token returns the server's [`crate::packet::PubAck`], or a
    /// [`CompletionError`] if the accepted operation cannot complete.
    pub struct PublishQoS1CompletionToken(CompletionToken<crate::mqtt_proto::PubAck<Bytes>> -> crate::packet::PubAck { Into::into })
);

make_completion_token_ty!(
    /// Completion token returned by [`crate::client::Client::publish_qos2`].
    ///
    /// Awaiting this token returns the server's [`crate::packet::PubRec`] and an optional
    /// [`crate::client::token::acknowledgement::PubRelToken`], or a [`CompletionError`] if the
    /// accepted operation cannot complete. The PUBREL token is present when PUBREC indicates
    /// success; confirm it to continue the QoS 2 flow and receive PUBCOMP. Call
    /// [`crate::packet::PubRec::as_result`] to check the PUBREC reason code.
    pub struct PublishQoS2CompletionToken(
    CompletionToken<(
        crate::mqtt_proto::PubRec<Bytes>,
        Option<crate::client::token::acknowledgement::buffered::PubRelToken<Bytes>>,
    )> -> (
        crate::packet::PubRec,
        Option<crate::client::token::acknowledgement::PubRelToken>,
    ) {
        |(pubrec, token): (_, Option<_>)| (crate::packet::PubRec::from(pubrec), token.map(crate::client::token::acknowledgement::PubRelToken))
    })
);

make_completion_token_ty!(
    /// Completion token returned by
    /// [`crate::client::token::acknowledgement::PubRecToken::accept`].
    ///
    /// Awaiting this token returns the server's [`crate::packet::PubRel`] and a
    /// [`crate::client::token::acknowledgement::PubCompToken`], or a [`CompletionError`] if the
    /// submitted acknowledgement cannot complete. Use the PUBCOMP token to confirm the PUBREL and
    /// complete the incoming QoS 2 flow.
    pub struct PubRecAcceptCompletionToken(
    CompletionToken<(
        crate::mqtt_proto::PubRel<Bytes>,
        crate::client::token::acknowledgement::buffered::PubCompToken<Bytes>,
    )> -> (
        crate::packet::PubRel,
        crate::client::token::acknowledgement::PubCompToken,
    ) {
        |(pubrel, pubcomp_token)| (crate::packet::PubRel::from(pubrel), crate::client::token::acknowledgement::PubCompToken(pubcomp_token))
    })
);

make_completion_token_ty!(
    /// Completion token returned by
    /// [`crate::client::token::acknowledgement::PubRecToken::reject`].
    ///
    /// Awaiting this token returns `Ok(())` when the session releases the rejecting PUBREC for
    /// transmission after any required acknowledgement ordering, or a [`CompletionError`] if the
    /// submitted acknowledgement cannot complete.
    pub struct PubRecRejectCompletionToken(CompletionToken<()>));

make_completion_token_ty!(
    /// Completion token returned by
    /// [`crate::client::token::acknowledgement::PubRelToken::confirm`].
    ///
    /// Awaiting this token returns the server's [`crate::packet::PubComp`], or a
    /// [`CompletionError`] if the submitted confirmation cannot complete.
    pub struct PubRelCompletionToken(CompletionToken<crate::mqtt_proto::PubComp<Bytes>> -> crate::packet::PubComp { Into::into })
);

make_completion_token_ty!(
    /// Completion token returned by [`crate::client::Client::subscribe`].
    ///
    /// Awaiting this token returns the server's [`crate::packet::SubAck`], or a
    /// [`CompletionError`] if the accepted operation cannot complete.
    pub struct SubscribeCompletionToken(CompletionToken<crate::mqtt_proto::SubAck<Bytes>> -> crate::packet::SubAck { Into::into })
);

make_completion_token_ty!(
    /// Completion token returned by [`crate::client::Client::unsubscribe`].
    ///
    /// Awaiting this token returns the server's [`crate::packet::UnsubAck`], or a
    /// [`CompletionError`] if the accepted operation cannot complete.
    pub struct UnsubscribeCompletionToken(CompletionToken<crate::mqtt_proto::UnsubAck<Bytes>> -> crate::packet::UnsubAck { Into::into })
);

make_completion_token_ty!(
    /// Completion token returned by [`crate::client::ReauthHandle::reauth`].
    ///
    /// Awaiting this token returns the server's [`crate::client::ReauthResult`], or a
    /// [`CompletionError`] if the accepted operation cannot complete.
    pub struct ReauthCompletionToken(CompletionToken<crate::client::buffered::ReauthResult<Bytes>> -> crate::client::ReauthResult { Into::into }));

make_completion_token_ty!(
    /// Completion token returned by [`crate::client::token::acknowledgement::PubAckToken::accept`]
    /// or [`crate::client::token::acknowledgement::PubAckToken::reject`].
    ///
    /// Awaiting this token returns `Ok(())` when the session releases the PUBACK for transmission
    /// after any required acknowledgement ordering, or a [`CompletionError`] if the submitted
    /// acknowledgement cannot complete.
    pub struct PubAckCompletionToken(CompletionToken<()>)
);

make_completion_token_ty!(
    /// Completion token returned by
    /// [`crate::client::token::acknowledgement::PubCompToken::confirm`].
    ///
    /// Awaiting this token returns `Ok(())` when the session releases the PUBCOMP for transmission,
    /// or a [`CompletionError`] if the submitted confirmation cannot complete.
    pub struct PubCompConfirmCompletionToken(CompletionToken<()>)
);

pub(crate) mod buffered {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Poll;

    use tokio::sync::oneshot;

    use super::CompletionError;
    use crate::buffer_pool::Shared;
    use crate::client::buffered::ReauthResult;
    use crate::client::token::acknowledgement::buffered::{PubCompToken, PubRelToken};
    use crate::mqtt_proto::{PubAck, PubComp, PubRec, PubRel, SubAck, UnsubAck}; // TODO

    /// Create a new completion pair, consisting of a [`CompletionNotifier`] and a [`CompletionToken`].
    pub fn completion_pair<T>() -> (CompletionNotifier<T>, CompletionToken<T>) {
        let (tx, rx) = oneshot::channel();
        let token = CompletionToken(rx);
        let notifier = CompletionNotifier(tx);
        (notifier, token)
    }

    // NOTE: Currently there are not buffered equivalents for all tokens defined in the main module.
    // This is because they are not currently used, but that may at some point be desirable.

    pub use super::{
        PubAckCompletionToken, PubCompConfirmCompletionToken, PubRecRejectCompletionToken,
    };

    make_completion_token_ty!(
        /// Token that can be awaited for the eventual completion of a re-authentication operation.
        /// (i.e. when the AUTH response has been received from the server).
        pub struct ReauthCompletionToken<S: Shared>(CompletionToken<ReauthResult<S>>)
    );
    make_completion_token_ty!(
        /// Token that can be awaited for the eventual completion of a PUBREC acceptance operation
        /// (i.e. when the PUBREL has been received from the server).
        pub struct PubRecAcceptCompletionToken<S: Shared>(CompletionToken<(PubRel<S>, PubCompToken<S>)>)
    );
    make_completion_token_ty!(
        /// Token that can be awaited for the eventual completion of a PUBREL confirm operation.
        /// (i.e. when the PUBCOMP has been received from the server).
        pub struct PubRelConfirmCompletionToken<S: Shared>(CompletionToken<PubComp<S>>)
    );

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
    pub(crate) type ReauthCompletionNotifier<S> = CompletionNotifier<ReauthResult<S>>;

    #[derive(Debug)]
    pub struct CompletionToken<T>(oneshot::Receiver<Result<T, CompletionError>>);

    impl<T> Future for CompletionToken<T> {
        type Output = Result<T, CompletionError>;

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<Self::Output> {
            match Pin::new(&mut self.0).poll(cx) {
                Poll::Ready(Ok(value)) => Poll::Ready(value),
                Poll::Ready(Err(_)) => Poll::Ready(Err(CompletionError::Detached)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    /// Notifier half of a completion pair
    #[derive(Debug)]
    pub(crate) struct CompletionNotifier<T>(oneshot::Sender<Result<T, CompletionError>>);

    impl<T> CompletionNotifier<T> {
        /// Complete the associated token(s) with the given value.
        /// If all the token(s) have been dropped, the value is returned.
        pub fn complete(self, value: T) -> Result<(), T> {
            match self.0.send(Ok(value)) {
                Ok(()) => Ok(()),
                Err(Ok(v)) => Err(v),
                Err(Err(_)) => unreachable!(),
            }
        }

        /// Issue a cancellation to the associated token(s).
        /// If all the token(s) have been dropped, an error is returned.
        pub fn cancel(self, reason: &str) -> Result<(), String> {
            match self
                .0
                .send(Err(CompletionError::Canceled(reason.to_string())))
            {
                Ok(()) => Ok(()),
                Err(Ok(_)) => unreachable!(),
                Err(Err(_)) => Err("Token dropped".to_string()),
            }
        }

        // TODO:
        // What other failures could there be other than cancellation?
        // Do they need distinct methods?
        // - packet size?
        // - wildcard sub?
        // - qos exceeded?
        // - connect while connected i.e. state error
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use super::CompletionError;
    use super::PublishQoS1CompletionToken;
    use super::buffered::*;

    #[tokio::test]
    async fn simple_completion() {
        let (notifier, token) = completion_pair();

        notifier.complete("hello_world".to_string()).unwrap();

        let res = token.await;
        assert_eq!(res, Ok("hello_world".to_string()));
    }

    #[tokio::test]
    async fn simple_cancellation() {
        let (notifier, token): (CompletionNotifier<String>, CompletionToken<String>) =
            completion_pair();

        notifier.cancel("test").unwrap();

        let res = token.await;
        assert_eq!(res, Err(CompletionError::Canceled("test".to_string())));
    }

    #[tokio::test]
    async fn mapped_completion_preserves_cancellation() {
        let (notifier, token): (
            CompletionNotifier<crate::mqtt_proto::PubAck<Bytes>>,
            CompletionToken<crate::mqtt_proto::PubAck<Bytes>>,
        ) = completion_pair();
        let token = PublishQoS1CompletionToken(token);

        notifier.cancel("test").unwrap();

        assert_eq!(
            token.await,
            Err(CompletionError::Canceled("test".to_string()))
        );
    }

    #[tokio::test]
    async fn portability_completion() {
        let (notifier, token) = completion_pair();

        let handle = tokio::spawn(token);

        notifier.complete("hello_world".to_string()).unwrap();

        let res = handle.await.unwrap();
        assert_eq!(res, Ok("hello_world".to_string()));
    }

    #[tokio::test]
    async fn portability_cancellation() {
        let (notifier, token): (CompletionNotifier<String>, CompletionToken<String>) =
            completion_pair();

        let handle = tokio::spawn(token);

        notifier.cancel("test").unwrap();

        let res = handle.await.unwrap();
        assert_eq!(res, Err(CompletionError::Canceled("test".to_string())));
    }

    #[tokio::test]
    async fn dropped_token() {
        let (notifier, token): (CompletionNotifier<String>, CompletionToken<String>) =
            completion_pair();

        drop(token);

        let res = notifier.complete("hello_world".to_string());
        assert_eq!(res, Err("hello_world".to_string()));
    }

    #[tokio::test]
    async fn dropped_notifier() {
        let (notifier, token): (CompletionNotifier<String>, CompletionToken<String>) =
            completion_pair();

        drop(notifier);

        let res = token.await;
        assert_eq!(res, Err(CompletionError::Detached));
    }
}
