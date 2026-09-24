// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Regression tests for targeted hotfixes, isolated so they can be merged into the main suites later.
// TODO: Make sure to refine and integrate these regression tests into broader suites as part of
// Session refactor

#[path = "../common/mod.rs"]
mod common;

mod connection_termination;
mod puback_reconnect;
