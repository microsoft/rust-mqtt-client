// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::time::{Duration, Sleep};

pub struct Timer {
    inner: Pin<Box<Sleep>>,
    duration: Duration,
}

impl Timer {
    pub fn new(duration: Duration) -> Self {
        Self {
            inner: Box::pin(tokio::time::sleep(duration)),
            duration,
        }
    }

    pub fn reset(&mut self) {
        self.inner
            .as_mut()
            .reset(tokio::time::Instant::now() + self.duration);
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}
