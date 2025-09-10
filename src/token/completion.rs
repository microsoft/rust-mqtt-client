// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! Synchronization for MQTT operations

// TODO: Remove when possible.
#![allow(dead_code)]

use futures::future::{FutureExt, Shared};
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use tokio::sync::oneshot;

pub fn completion_pair<T: Clone>() -> (CompletionTransmitter<T>, CompletionToken<T>) {
    let (tx, rx) = oneshot::channel();
    // TODO: put the sharing logic here
    let token = CompletionToken(rx.shared());
    let transmitter = CompletionTransmitter(tx);
    (transmitter, token)
}

#[derive(Clone, PartialEq, Debug)]
pub enum CompletionError {
    Detatched, // is this really the correct place for it?
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

// TODO: Finalize naming - Signaller?
pub struct CompletionTransmitter<T>(oneshot::Sender<Result<T, CompletionError>>)
where
    T: Clone;

impl<T> CompletionTransmitter<T>
where
    T: Clone,
{
    /// Complete the token(s) with the given value.
    /// If the token(s) have been dropped, the value is returned.
    pub fn complete(self, value: T) -> Result<(), T> {
        match self.0.send(Ok(value)) {
            Ok(()) => Ok(()),
            Err(Ok(v)) => Err(v),
            Err(Err(_)) => unreachable!(),
        }
    }

    pub fn cancel(self) -> Result<(), String> {
        match self.0.send(Err(CompletionError::Cancelled)) {
            Ok(()) => Ok(()),
            Err(Ok(_)) => unreachable!(),
            Err(Err(_)) => Err("Token dropped".to_string()),
        }
    }

    // what other failures could there be other than cancellation?
    // - packet size?
    // - wildcard sub?
    // - qos exceeded?
    // - connect while connected i.e. state error
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn clonability() {
        let (transmitter, token) = completion_pair();
        let token_clone = token.clone();

        transmitter.complete("hello_world".to_string()).unwrap();

        let r1 = token.await;
        let r2 = token_clone.await;
        assert_eq!(r1, r2);
        assert_eq!(r1, Ok("hello_world".to_string()));
    }

    #[tokio::test]
    async fn portability() {
        let (transmitter, token) = completion_pair();

        let handle = tokio::spawn(token);

        transmitter.complete("hello_world".to_string()).unwrap();

        let res = handle.await.unwrap();
        assert_eq!(res, Ok("hello_world".to_string()));
    }

    // test todo: drops, cancellations, cancel safety, thread portability etc.
}
