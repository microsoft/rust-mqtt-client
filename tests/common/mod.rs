// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(unused)] // Not all tests use all functions.

use std::pin::pin;

use futures_util::future::{Either, select};
use ms_mqtt_client::client::token::acknowledgement::PubAckToken;
use ms_mqtt_client::client::{ManualAcknowledgement, Receiver};
use ms_mqtt_client::packet::{PubAckProperties, Publish};

pub(crate) async fn run_with_connection<F>(
    connection: impl Future + Unpin,
    f: F,
) -> Option<F::Output>
where
    F: Future + Unpin,
{
    match select(f, connection).await {
        Either::Left((result, _)) => Some(result),
        Either::Right(_) => None,
    }
}

pub(crate) async fn receive_publish(
    connection: impl Future + Unpin,
    receiver: &mut Receiver,
) -> (Publish, ManualAcknowledgement) {
    let f = pin!(receiver.recv());
    match run_with_connection(connection, f).await {
        Some(Some((publish, manual_ack))) => (publish, manual_ack),
        _ => panic!("did not receive expected PUBLISH and ack token"),
    }
}

pub(crate) async fn accept_publish(
    connection: impl Future + Unpin,
    ack_token: PubAckToken,
    properties: PubAckProperties,
) {
    let f = pin!(ack_token.accept(properties));
    match run_with_connection(connection, f).await {
        Some(_) => (),
        _ => panic!("did not manage to ack PUBLISH"),
    }
}
