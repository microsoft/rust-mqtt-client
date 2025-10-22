// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use azure_mqtt::client::{
    AckHandle, Client, ClientOptions, ConnectHandle, ConnectionTransportConfig, Receiver,
    new_client,
};
use azure_mqtt::packet::{ConnectProperties, Publish, QoS, SubscribeProperties};
use azure_mqtt::topic::TopicFilter;

const CLIENT_ID: &str = "my_client";
const HOSTNAME: &str = "localhost";
const PORT: u16 = 1883;
const GET_FILTER: &str = "watchlist/get";
const UPDATE_FILTER: &str = "watchlist/update";

#[tokio::main]
async fn main() {
    // This would be a builder pattern in a real implementation.
    let options = ClientOptions {
        client_id: CLIENT_ID.to_string(),
        queue_size: 10,
    };
    let (client, connect_handle, receiver) = new_client(options);

    let (get_tx, get_rx) = unbounded_channel();
    let (update_tx, update_rx) = unbounded_channel();

    tokio::select! {
        () = mqtt_receive(receiver, get_tx, update_tx) => {
            // Receiver finished
        }
        () = mqtt_run(connect_handle, client, get_rx, update_rx) => {
            // Program finished
        }
    }
}

async fn mqtt_receive(
    mut receiver: Receiver,
    get_tx: UnboundedSender<(Publish, AckHandle)>,
    update_tx: UnboundedSender<(Publish, AckHandle)>,
) {
    let get_filter = TopicFilter::new(GET_FILTER).unwrap();
    let update_filter = TopicFilter::new(UPDATE_FILTER).unwrap();
    loop {
        while let Some((publish, ack_handle)) = receiver.recv().await {
            // NOTE: If you don't want manual ack, simply ignore the ack_token by using a _, and it
            // will be acked automatically on drop.
            // No need for "manual ack" setting on the client.

            if publish.topic_name.matches_topic_filter(&get_filter) {
                get_tx.send((publish, ack_handle)).unwrap();
            } else if publish.topic_name.matches_topic_filter(&update_filter) {
                update_tx.send((publish, ack_handle)).unwrap();
            } else {
                println!(
                    "Received publish on unrecognized topic: {:?}",
                    publish.topic_name
                );
                // Implicitly acks the message on drop
            }
        }
    }
}

async fn mqtt_run(
    mut connect_handle: ConnectHandle,
    client: Client,
    mut get_rx: UnboundedReceiver<(Publish, AckHandle)>,
    mut update_rx: UnboundedReceiver<(Publish, AckHandle)>,
) {
    // Loop so that if we disconnect, we can reconnect.
    loop {
        println!("Attempting to connect to MQTT broker...");
        let (connected, _, _) = connect_handle
            .connect(
                ConnectionTransportConfig::Tcp {
                    hostname: HOSTNAME.to_string(),
                    port: PORT,
                },
                ConnectProperties::default(),
            )
            .await;
        println!("Connected to MQTT broker");

        tokio::select! {
            (connect_handle_, _) = connected.run_until_disconnect() => {
                // Drain the updates channel since we no longer want any of them
                // and we will be reconnecting with clean start true.
                // This will implicitly ack the messages, but again, we are discarding the session.
                while !update_rx.is_empty() {
                    update_rx.try_recv().unwrap();
                }

                connect_handle = connect_handle_;
                println!("Disconnect detected, will reconnect in 5 seconds...");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            () = maintain_document(client.clone(), &mut get_rx, &mut update_rx) => {
                // Maintaining document finished
                return;
            }
        }
    }
}

async fn maintain_document(
    client: Client,
    get_rx: &mut UnboundedReceiver<(Publish, AckHandle)>,
    update_rx: &mut UnboundedReceiver<(Publish, AckHandle)>,
) {
    // Subscribe to the get topic
    client
        .subscribe(
            TopicFilter::new(GET_FILTER).unwrap(),
            QoS::AtLeastOnce,
            SubscribeProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();

    while let Some((publish, ack_handle)) = get_rx.recv().await {
        let mut document = str::from_utf8(&publish.payload).unwrap().to_string();
        println!("Received document: {document}");
        drop(ack_handle); // Implicitly acks the message on drop

        // Subscribe to the update topic
        client
            .subscribe(
                TopicFilter::new(UPDATE_FILTER).unwrap(),
                QoS::AtLeastOnce,
                SubscribeProperties::default(),
            )
            .await
            .unwrap()
            .await
            .unwrap();

        while let Some((publish, ack_handle)) = update_rx.recv().await {
            let update_str = str::from_utf8(&publish.payload).unwrap();
            println!("Received update: {update_str}");
            document.push_str(update_str);
            println!("Current document: {document}");
            drop(ack_handle); // Implicitly acks the message on drop
        }
    }
}
