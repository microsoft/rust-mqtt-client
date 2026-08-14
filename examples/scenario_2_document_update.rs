// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{error::Error, pin::pin, str, time::Duration};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use ms_mqtt_client::client::{
    Client, ClientOptions, ConnectHandle, ConnectResult, KeepAliveConfig, ManualAcknowledgement,
    Receiver, new_client,
};
use ms_mqtt_client::packet::{
    ConnectProperties, DisconnectProperties, Publish, QoS, RetainOptions, SubscribeProperties,
};
use ms_mqtt_client::topic::TopicFilter;
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};

const CLIENT_ID: &str = "my_client";
const HOSTNAME: &str = "localhost";
const PORT: u16 = 1883;
const GET_FILTER: &str = "watchlist/get";
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

    let (get_tx, get_rx) = unbounded_channel();
    let (update_tx, update_rx) = unbounded_channel();

    // The reconnect supervisor owns intentional shutdown. Poll it first so the receiver closure
    // caused by that shutdown is not reported as an unexpected failure.
    tokio::select! {
        biased;
        result = run_document_client(connect_handle, client, get_rx, update_rx) => {
            result
        }
        result = mqtt_receive(receiver, get_tx, update_tx) => {
            result?;
            Err("Receiver closed unexpectedly".into())
        }
    }
}

async fn mqtt_receive(
    mut receiver: Receiver,
    get_tx: UnboundedSender<(Publish, ManualAcknowledgement)>,
    update_tx: UnboundedSender<(Publish, ManualAcknowledgement)>,
) -> ExampleResult {
    let get_filter = TopicFilter::new(GET_FILTER)?;
    let update_filter = TopicFilter::new(UPDATE_FILTER)?;
    while let Some((publish, manual_ack)) = receiver.recv().await {
        // NOTE: Explicit acknowledgement is not required. Drop `manual_ack` or leave it unbound by
        // replacing it with `_` in the loop pattern. When acknowledgement is required,
        // auto-acknowledgement is performed with default properties.

        // Keep this dispatcher fast; document processing and acknowledgement happen downstream.

        if publish.topic_name.matches_topic_filter(&get_filter) {
            get_tx.send((publish, manual_ack))?;
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

async fn run_document_client(
    mut connect_handle: ConnectHandle,
    client: Client,
    mut get_rx: UnboundedReceiver<(Publish, ManualAcknowledgement)>,
    mut update_rx: UnboundedReceiver<(Publish, ManualAcknowledgement)>,
) -> ExampleResult {
    // Own the reconnect policy and start one fresh document workflow per successful connection.
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
                // MQTT operations progress only while the connection future is polled, so drive it
                // alongside the connection-scoped document workflow.
                let mut connection_driver = pin!(connection.run_until_disconnect());
                connect_handle = tokio::select! {
                    (connect_handle, event) = &mut connection_driver => {
                        // `select!` has canceled `maintain_document`, dropping its document state.
                        // Discard queued messages from this connection so they cannot be applied
                        // after reconnect.
                        while get_rx.try_recv().is_ok() {}
                        while update_rx.try_recv().is_ok() {}

                        println!(
                            "Disconnected from MQTT server: {event:?}; reconnecting in 5 seconds..."
                        );
                        connect_handle
                    }
                    result = maintain_document(client.clone(), &mut get_rx, &mut update_rx) => {
                        result?;
                        return Err("Document maintainer exited unexpectedly".into());
                    }
                    result = &mut shutdown => {
                        result?;
                        println!("Ctrl+C received; disconnecting from MQTT server...");
                        disconnect_handle.disconnect(&DisconnectProperties::default())?;
                        let (connect_handle, event) = connection_driver.await;
                        println!("Disconnected from MQTT server: {event:?}");
                        drop(connect_handle);
                        return Ok(());
                    }
                };
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

async fn maintain_document(
    client: Client,
    get_rx: &mut UnboundedReceiver<(Publish, ManualAcknowledgement)>,
    update_rx: &mut UnboundedReceiver<(Publish, ManualAcknowledgement)>,
) -> ExampleResult {
    // Subscribe to the get topic.
    // Clean start discards subscriptions, so establish this on every successful connection.
    client
        .subscribe(
            TopicFilter::new(GET_FILTER)?,
            QoS::AtLeastOnce,
            false,
            RetainOptions::default(),
            SubscribeProperties::default(),
        )
        .await?
        .await?
        .as_result()?;

    while let Some((publish, manual_ack)) = get_rx.recv().await {
        // This example expects one base document per connection, followed by updates until
        // disconnect.
        let mut document = str::from_utf8(&publish.payload)?.to_string();
        println!("Received document: {document}");
        // Acknowledge only after the base document has been validated and installed locally.
        drop(manual_ack); // Attempts auto-acknowledgement with default properties.

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

        while let Some((publish, manual_ack)) = update_rx.recv().await {
            let update_str = str::from_utf8(&publish.payload)?;
            println!("Received update: {update_str}");
            document.push_str(update_str);
            println!("Current document: {document}");
            // Acknowledge only after applying the update to local state.
            drop(manual_ack); // Attempts auto-acknowledgement with default properties.
        }
    }

    Ok(())
}
