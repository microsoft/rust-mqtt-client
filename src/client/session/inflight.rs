// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Inflight operation tracker

use std::collections::HashMap;

use crate::buffer_pool::Shared;
use crate::mqtt_proto::{PacketIdentifier, Publish};

pub struct InflightTracker<S>
where
    S: Shared,
{
    placeholder: HashMap<PacketIdentifier, Publish<S>>,
}

impl<S> InflightTracker<S>
where
    S: Shared,
{
    pub fn new() -> Self {
        unimplemented!()
    }
}
