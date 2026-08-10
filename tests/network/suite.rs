// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Live network tests, written to be server-agnostic.
//!
//! The server is chosen at run time, not at compile time: these tests connect to whatever
//! `MQTT_HOST`/`MQTT_PORT` point at, so the same suite validates every server fixture. Pick one with
//! `make network-test BROKER=<name>` (see `tests/network/brokers/`).
//!
//! Enabled by the `__network` and `websockets` features, which leaves `#[ignore]` free to
//! quarantine a flaky test. Add a new area of coverage as another module here.

mod common;
mod messaging;
mod meta;
mod session;
mod transport_configuration;
mod transport_io;
