// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! A low-level asynchronous MQTT 5 client library.
//!
//! QoS 0 and QoS 1 are supported end to end. QoS 2 types and methods reserve the intended public
//! API, but QoS 2 publishing and receiving are not yet implemented and must not be used in
//! applications.
//!
//! # Client components
//!
//! [`client::new_client`] creates three components with independent ownership:
//!
//! - [`client::Client`] sends publish, subscribe, and unsubscribe operations.
//! - [`client::ConnectHandle`] establishes a connection, which produces a
//!   [`client::Connection`] that drives network I/O.
//! - [`client::Receiver`] receives incoming publishes.
//!
//! The library does not drive the MQTT connection in the background. An application must
//! continuously drive [`client::Connection::run_until_disconnect`] while using the client and
//! receiver. When the connection ends, the method returns the [`client::ConnectHandle`] for a
//! later connection attempt.
//!
//! # Common tasks
//!
//! | Goal | Start with |
//! | --- | --- |
//! | Construct a client | [`client::new_client`] and [`client::ClientOptions`] |
//! | Connect and drive MQTT I/O | [`client::ConnectHandle::connect`] and [`client::Connection::run_until_disconnect`] |
//! | Configure TCP, TLS, or `WebSockets` | [`transport::ConnectionTransportConfig`] and [`transport::ConnectionTransportType`] |
//! | Publish at QoS 0 or QoS 1 | [`client::Client::publish_qos0`] or [`client::Client::publish_qos1`] |
//! | Subscribe or unsubscribe | [`client::Client::subscribe`] or [`client::Client::unsubscribe`] |
//! | Receive and acknowledge publishes | [`client::Receiver::recv`] and [`client::ManualAcknowledgement`] |
//! | Disconnect cleanly | [`client::DisconnectHandle::disconnect`], while continuing to drive the connection |
//! | Reconnect | Reuse the [`client::ConnectHandle`] returned by [`client::Connection::run_until_disconnect`] |
//!
//! # Connect and drive I/O
//!
//! Use [`client::ConnectHandle::connect`] for a standard MQTT5 connection or
//! [`client::ConnectHandle::connect_enhanced_auth`] for MQTT5 enhanced authentication. Each
//! method documents its connection flow and returned handles.
//!
//! After connecting, drive the connection concurrently with application logic. If the connection
//! ends first, the application future is dropped. If the application ends first, request an
//! orderly disconnect and continue driving the connection until it closes:
//!
//! ```no_run
//! use ms_mqtt_client::client::{
//!     Client, ConnectHandle, Connection, DisconnectHandle, DisconnectedEvent, Receiver,
//! };
//! use ms_mqtt_client::packet::DisconnectProperties;
//!
//! async fn application_logic(client: Client, receiver: Receiver) {
//!     // Publish, subscribe, and process incoming messages until ready to shut down.
//! }
//!
//! async fn run_connected(
//!     client: Client,
//!     connection: Connection,
//!     receiver: Receiver,
//!     disconnect_handle: DisconnectHandle,
//! ) -> (ConnectHandle, DisconnectedEvent) {
//!     let connection = connection.run_until_disconnect();
//!
//!     tokio::pin!(connection);
//!
//!     tokio::select! {
//!         result = &mut connection => result,
//!         () = application_logic(client, receiver) => {
//!             let _ = disconnect_handle.disconnect(&DisconnectProperties::default());
//!             connection.await
//!         }
//!     }
//! }
//! ```
//!
//! # Operation completion
//!
//! Operations sent through [`client::Client`] complete in stages. When the future returned by an
//! operation method completes successfully, the operation has been accepted by the client and an
//! operation-specific completion token is returned. If the client has been orphaned because its
//! corresponding [`client::ConnectHandle`] or [`client::Connection`] was dropped, the future
//! instead returns [`error::DetachedError`] without accepting the operation or affecting client
//! state.
//!
//! Awaiting the completion token waits for the corresponding session or protocol event and may
//! return [`error::CompletionError`] if the accepted operation cannot complete. Dropping the token
//! does not cancel the accepted operation; it only gives up observing completion and any response.
//! Server acknowledgements, such as [`packet::PubAck`], retain their MQTT reason code so the
//! application can inspect the server's result separately.
//!
//! | Operation | Completion token observes | MQTT result check |
//! | --- | --- | --- |
//! | [`client::Client::publish_qos0`] | Release of the PUBLISH for transmission | None; QoS 0 has no server acknowledgement |
//! | [`client::Client::publish_qos1`] | Receipt of [`packet::PubAck`] | [`packet::PubAck::as_result`] |
//! | [`client::Client::subscribe`] | Receipt of [`packet::SubAck`] | [`packet::SubAck::as_result`] |
//! | [`client::Client::unsubscribe`] | Receipt of [`packet::UnsubAck`] | [`packet::UnsubAck::as_result`] |
//!
//! ```no_run
//! use std::error::Error;
//!
//! use ms_mqtt_client::client::Client;
//! use ms_mqtt_client::packet::PublishProperties;
//! use ms_mqtt_client::topic::TopicName;
//!
//! async fn publish_and_wait(client: &Client) -> Result<(), Box<dyn Error>> {
//!     // Keep the token because this function wants to observe completion.
//!     let ct = client
//!         .publish_qos1(
//!             TopicName::new("example/topic").expect("valid topic"),
//!             "hello".into(),
//!             false,
//!             PublishProperties::default(),
//!         )
//!         .await?; // DetachedError if the operation was not accepted.
//!
//!     let puback = ct.await?; // CompletionError if the operation did not complete.
//!     puback.as_result()?; // OperationFailure if the server rejected the PUBLISH.
//!     Ok(())
//! }
//! ```
//!
//! # Runnable examples
//!
//! The repository examples are the canonical end-to-end patterns:
//!
//! - [Simple client](https://github.com/microsoft/rust-mqtt-client/blob/main/examples/scenario_1_simple.rs): connect, subscribe, publish, receive, acknowledge, and shut down.
//! - [Document updates](https://github.com/microsoft/rust-mqtt-client/blob/main/examples/scenario_2_document_update.rs): reconnect, resubscribe, and keep application state scoped to a connection epoch.
//! - [Message relay](https://github.com/microsoft/rust-mqtt-client/blob/main/examples/scenario_3_relay.rs): coordinate two independently reconnecting clients.

// Low-level modules
// TODO: Revisit the exposed API of these modules, and remove the linting suppressions as necessary
#[cfg(not(any(feature = "__integration", feature = "__fuzzing")))]
pub(crate) mod buffer_pool;
#[cfg(any(feature = "__integration", feature = "__fuzzing"))]
pub mod buffer_pool;

pub(crate) mod io;

#[cfg(not(any(feature = "__integration", feature = "__fuzzing")))]
pub(crate) mod mqtt_proto;
#[cfg(any(feature = "__integration", feature = "__fuzzing"))]
pub mod mqtt_proto;

// High-level modules
pub mod client;
pub mod error;
pub mod packet;
pub mod topic;
pub mod transport;

// NOTE: Any dispatching or connection management would be supplementary components.
// I am in favor of providing them, but they are built on top of these core components and would be optional.
// These core components have been designed to enable the future use of such additional components,
// e.g. PubAckToken is designed to support future usage in complex dispatching scenarios.
