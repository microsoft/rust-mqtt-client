// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! Token types for awaiting completion of MQTT operations and issuing acknowledgements.

// TODO: Remove when possible.
#![allow(unused_variables)]
#![allow(clippy::unused_async)]

pub(crate) mod acknowledgement;
pub(crate) mod completion;
pub(crate) mod reauth;
