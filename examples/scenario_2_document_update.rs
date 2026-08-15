// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{error::Error, str, time::Duration};

use futures_util::FutureExt as _;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use ms_mqtt_client::client::{
    Client, ClientOptions, ConnectHandle, ConnectResult, Connection, DisconnectedEvent,
    KeepAliveConfig, ManualAcknowledgement, Receiver, new_client,
};
use ms_mqtt_client::packet::{
    ConnectProperties, DisconnectProperties, PubAckProperties, PubRejectReason, Publish, QoS,
    RetainOptions, SubscribeProperties,
};
use ms_mqtt_client::topic::TopicFilter;
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};

const CLIENT_ID: &str = "my_client";
const HOSTNAME: &str = "localhost";
const PORT: u16 = 1883;
const BASE_DOCUMENT_FILTER: &str = "watchlist/get";
const UPDATE_FILTER: &str = "watchlist/update";

type ExampleResult = Result<(), Box<dyn Error>>;

#[tokio::main]
async fn main() -> ExampleResult {
    // This would be a builder pattern in a real implementation.
    let options = ClientOptions {
        client_id: Some(CLIENT_ID.to_string()),
        ..Default::default()
    };
    let (client, connect_handle, receiver) = new_client(options);

    // Keep the large, long-lived reconnect supervisor out of `main`'s future state.
    Box::pin(run_document_client(connect_handle, client, receiver)).await
}

async fn run_document_client(
    mut connect_handle: ConnectHandle,
    client: Client,
    mut receiver: Receiver,
) -> ExampleResult {
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        println!("Attempting to connect to MQTT server...");
        // Clean start keeps broker subscriptions aligned with the local document state, which is
        // rebuilt after every reconnect.
        let connect_result = tokio::select! {
            result = connect_handle.connect(
                    ConnectionTransportConfig {
                        transport_type: ConnectionTransportType::Tcp {
                            hostname: HOSTNAME.to_string(),
                            port: PORT,
                        },
                        timeout: None,
                        proxy: None,
                        tcp_nodelay: false,
                    },
                    true,
                    KeepAliveConfig::Infinite,
                    None,
                    None,
                    None,
                    ConnectProperties::default(),
                    None,
                ) => result,
            result = &mut shutdown => {
                result?;
                println!("Ctrl+C received; canceling connection attempt and stopping...");
                return Ok(());
            }
        };

        connect_handle = match connect_result {
            ConnectResult::Success(connection, _, disconnect_handle) => {
                println!("Connected to MQTT server");
                let connected_session = run_connected_session(connection, &client, &mut receiver);
                tokio::pin!(connected_session);
                let (connect_handle, event) = tokio::select! {
                    result = &mut connected_session => result?,
                    result = &mut shutdown => {
                        result?;
                        println!("Ctrl+C received; disconnecting from MQTT server...");
                        disconnect_handle.disconnect(&DisconnectProperties::default())?;
                        let (connect_handle, event) = connected_session.await?;
                        println!("Disconnected from MQTT server: {event:?}");
                        drop(connect_handle);
                        return Ok(());
                    }
                };

                println!("Disconnected from MQTT server: {event:?}; reconnecting in 5 seconds...");
                connect_handle
            }
            ConnectResult::Failure(connect_handle, error) => {
                eprintln!("Failed to connect to MQTT server: {error}; retrying in 5 seconds...");
                connect_handle
            }
        };

        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(5)) => {}
            result = &mut shutdown => {
                result?;
                println!("Ctrl+C received; stopping reconnect attempts...");
                return Ok(());
            }
        }
    }
}

async fn run_connected_session(
    connection: Connection,
    client: &Client,
    receiver: &mut Receiver,
) -> Result<(ConnectHandle, DisconnectedEvent), Box<dyn Error>> {
    // The document state and routing queues belong to this connection. Recreate them after every
    // reconnect so messages from a closed connection cannot enter the next workflow.
    let (base_document_tx, mut base_document_rx) = unbounded_channel();
    let (update_tx, mut update_rx) = unbounded_channel();

    // MQTT operations progress only while the connection driver is polled.
    let (connect_handle, event) = tokio::select! {
        result = connection.run_until_disconnect() => result,
        result = maintain_document(client, &mut base_document_rx, &mut update_rx) => {
            result?;
            return Err("Document maintainer exited unexpectedly".into());
        }
        result = dispatch_publishes(receiver, &base_document_tx, &update_tx) => {
            result?;
            return Err("Receiver closed unexpectedly".into());
        }
    };

    // The workflow and dispatcher are now canceled, and the connection driver can no longer add
    // messages to `receiver`. Discard everything queued for the closed connection.
    let discarded_count = discard_queued_messages(receiver, &mut base_document_rx, &mut update_rx);
    if discarded_count > 0 {
        println!("Discarded {discarded_count} queued message(s) from the closed connection");
    }

    Ok((connect_handle, event))
}

async fn dispatch_publishes(
    receiver: &mut Receiver,
    base_document_tx: &UnboundedSender<(Publish, ManualAcknowledgement)>,
    update_tx: &UnboundedSender<(Publish, ManualAcknowledgement)>,
) -> ExampleResult {
    let base_document_filter = TopicFilter::new(BASE_DOCUMENT_FILTER)?;
    let update_filter = TopicFilter::new(UPDATE_FILTER)?;
    while let Some((publish, manual_ack)) = receiver.recv().await {
        // Route the acknowledgement with the publish so the document workflow can acknowledge
        // only after processing succeeds.

        if publish
            .topic_name
            .matches_topic_filter(&base_document_filter)
        {
            base_document_tx.send((publish, manual_ack))?;
        } else if publish.topic_name.matches_topic_filter(&update_filter) {
            update_tx.send((publish, manual_ack))?;
        } else {
            println!(
                "Received publish on unrecognized topic: {:?}",
                publish.topic_name
            );
            // Dropping `manual_ack` attempts auto-acknowledgement with default properties.
        }
    }

    Ok(())
}

fn discard_queued_messages(
    receiver: &mut Receiver,
    base_document_rx: &mut UnboundedReceiver<(Publish, ManualAcknowledgement)>,
    update_rx: &mut UnboundedReceiver<(Publish, ManualAcknowledgement)>,
) -> usize {
    let mut discarded = 0;

    while receiver.recv().now_or_never().flatten().is_some() {
        discarded += 1;
    }
    while base_document_rx.try_recv().is_ok() {
        discarded += 1;
    }
    while update_rx.try_recv().is_ok() {
        discarded += 1;
    }

    discarded
}

async fn reject_invalid_utf8(
    context: &str,
    error: str::Utf8Error,
    manual_ack: ManualAcknowledgement,
) -> ExampleResult {
    match manual_ack {
        ManualAcknowledgement::QoS0 => {
            eprintln!("Discarded {context} with invalid UTF-8: {error}");
        }
        ManualAcknowledgement::QoS1(token) => {
            token
                .reject(
                    PubRejectReason::PayloadFormatInvalid,
                    PubAckProperties::default(),
                )
                .await?
                .await?;
            eprintln!("Rejected {context} with invalid UTF-8: {error}");
        }
        ManualAcknowledgement::QoS2(_) => {
            unreachable!("the subscription requests a maximum of QoS 1")
        }
    }

    Ok(())
}

async fn maintain_document(
    client: &Client,
    base_document_rx: &mut UnboundedReceiver<(Publish, ManualAcknowledgement)>,
    update_rx: &mut UnboundedReceiver<(Publish, ManualAcknowledgement)>,
) -> ExampleResult {
    // Subscribe to the base-document topic.
    // Clean start discards subscriptions, so establish this on every successful connection.
    client
        .subscribe(
            TopicFilter::new(BASE_DOCUMENT_FILTER)?,
            QoS::AtLeastOnce,
            false,
            RetainOptions::default(),
            SubscribeProperties::default(),
        )
        .await?
        .await?
        .as_result()?;
    println!("Subscribed to {BASE_DOCUMENT_FILTER}; waiting for a base document");

    // Establish one valid base document for this connection before accepting updates.
    let mut document = loop {
        let Some((publish, manual_ack)) = base_document_rx.recv().await else {
            return Ok(());
        };

        match str::from_utf8(&publish.payload) {
            Ok(document) => {
                let document = document.to_string();
                println!("Received document: {document}");
                // Dropping the token acknowledges the validated document with default properties.
                drop(manual_ack);
                break document;
            }
            Err(error) => {
                reject_invalid_utf8("base document", error, manual_ack).await?;
            }
        }
    };

    // Subscribe to updates only after a valid base document is available.
    client
        .subscribe(
            TopicFilter::new(UPDATE_FILTER)?,
            QoS::AtLeastOnce,
            false,
            RetainOptions::default(),
            SubscribeProperties::default(),
        )
        .await?
        .await?
        .as_result()?;
    println!("Subscribed to {UPDATE_FILTER}; waiting for document updates");

    while let Some((publish, manual_ack)) = update_rx.recv().await {
        let update = match str::from_utf8(&publish.payload) {
            Ok(update) => update,
            Err(error) => {
                reject_invalid_utf8("document update", error, manual_ack).await?;
                continue;
            }
        };
        println!("Received update: {update}");
        document.push_str(update);
        println!("Current document: {document}");
        // Dropping the token acknowledges the applied update with default properties.
        drop(manual_ack);
    }

    Ok(())
}
