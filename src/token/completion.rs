// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! Synchronization for portable reporting of remote operations

// TODO: Remove when possible.
#![allow(dead_code)]

use futures::future::{FutureExt, Shared};
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use tokio::sync::oneshot;

/// Create a new completion pair, consisting of a [`CompletionNotifier`] and a [`CompletionToken`].
pub fn completion_pair<T: Clone>() -> (CompletionNotifier<T>, CompletionToken<T>) {
    let (tx, rx) = oneshot::channel();
    let token = CompletionToken(rx.shared());
    let notifier = CompletionNotifier(tx);
    (notifier, token)
}

#[derive(Clone, PartialEq, Debug)]
pub enum CompletionError {
    Detatched,
    Cancelled,
}

#[derive(Clone)]
pub struct CompletionToken<T>(Shared<oneshot::Receiver<Result<T, CompletionError>>>)
where
    T: Clone;

impl<T> Future for CompletionToken<T>
where
    T: Clone,
{
    type Output = Result<T, CompletionError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Ready(Ok(value)) => Poll::Ready(value),
            Poll::Ready(Err(_)) => Poll::Ready(Err(CompletionError::Detatched)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Notifier half of a completion pair
pub struct CompletionNotifier<T>(oneshot::Sender<Result<T, CompletionError>>)
where
    T: Clone;

impl<T> CompletionNotifier<T>
where
    T: Clone,
{
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
    pub fn cancel(self) -> Result<(), String> {
        match self.0.send(Err(CompletionError::Cancelled)) {
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

#[cfg(test)]
mod test {
    use super::*;

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

        notifier.cancel().unwrap();

        let res = token.await;
        assert_eq!(res, Err(CompletionError::Cancelled));
    }

    #[tokio::test]
    async fn clonability_completion() {
        let (notifier, token) = completion_pair();
        let token_clone = token.clone();

        notifier.complete("hello_world".to_string()).unwrap();

        let r1 = token.await;
        let r2 = token_clone.await;
        assert_eq!(r1, r2);
        assert_eq!(r1, Ok("hello_world".to_string()));
    }

    #[tokio::test]
    async fn clonability_cancellation() {
        let (notifier, token): (CompletionNotifier<String>, CompletionToken<String>) =
            completion_pair();
        let token_clone = token.clone();

        notifier.cancel().unwrap();

        let r1 = token.await;
        let r2 = token_clone.await;
        assert_eq!(r1, r2);
        assert_eq!(r1, Err(CompletionError::Cancelled));
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

        notifier.cancel().unwrap();

        let res = handle.await.unwrap();
        assert_eq!(res, Err(CompletionError::Cancelled));
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
    async fn dropped_token_multiple() {
        let (notifier, token): (CompletionNotifier<String>, CompletionToken<String>) =
            completion_pair();
        let token_clone = token.clone();

        // Drop both tokens
        drop(token);
        drop(token_clone);

        let res = notifier.complete("hello_world".to_string());
        assert_eq!(res, Err("hello_world".to_string()));
    }

    #[tokio::test]
    async fn droped_token_one_of_multiple() {
        let (notifier, token): (CompletionNotifier<String>, CompletionToken<String>) =
            completion_pair();
        let token_clone = token.clone();

        // Drop one of the tokens
        drop(token_clone);

        // Completion can still be sent to the other token
        notifier.complete("hello_world".to_string()).unwrap();
        let res = token.await;
        assert_eq!(res, Ok("hello_world".to_string()));
    }

    #[tokio::test]
    async fn dropped_notifier_single_token() {
        let (notifier, token): (CompletionNotifier<String>, CompletionToken<String>) =
            completion_pair();

        drop(notifier);

        let res = token.await;
        assert_eq!(res, Err(CompletionError::Detatched));
    }

    #[tokio::test]
    async fn dropped_notifier_multiple_tokens() {
        let (notifier, token): (CompletionNotifier<String>, CompletionToken<String>) =
            completion_pair();
        let token_clone = token.clone();

        drop(notifier);

        let res1 = token.await;
        let res2 = token_clone.await;
        assert_eq!(res1, Err(CompletionError::Detatched));
        assert_eq!(res2, Err(CompletionError::Detatched));
    }
}
