// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! MQTT client library

// TODO: Revisit the exposed API of these modules, and remove the linting suppressions as necessary
pub mod buffer_pool;
pub(crate) mod io;
pub(crate) mod mqtt_proto;
mod opensslext;
