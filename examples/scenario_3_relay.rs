// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::Arc;

use tokio::sync::Notify;

use azure_mqtt::client::{Client, ClientOptions, Event, EventLoop, Receiver, new_client};
use azure_mqtt::packet::{ConnectProperties, DeliveryQoS, QoS, SubscribeProperties};
use azure_mqtt::topic::TopicFilter;

const DOWNSTREAM_SUB_FILTER: &str = "downstream/#";

#[tokio::main]
async fn main() {
    // Downstream client
    let options = ClientOptions {
        client_id: "downstream_client".to_string(),
        queue_size: 10,
    };
    let (ds_client, ds_event_loop, ds_receiver) = new_client(options);
    let ds_disconnect_notify = Arc::new(Notify::new());

    // Upstream client
    let options = ClientOptions {
        client_id: "upstream_client".to_string(),
        queue_size: 10,
    };
    let (us_client, us_event_loop, _) = new_client(options);
    let us_disconnect_notify = Arc::new(Notify::new());

    tokio::select! {
        () = mqtt_run(ds_event_loop, ds_disconnect_notify.clone()) => {
            println!("Downstream Connection runner unexpectedly failed!");
        }
        () = mqtt_run(us_event_loop, us_disconnect_notify.clone()) => {
            println!("Upstream Connection runner unexpectedly failed!");
        }
        () = maintain_connection(ds_client.clone(), ds_disconnect_notify) => {
            println!("Downstream Connection maintainer unexpectedly failed!");
        }
        () = maintain_connection(us_client.clone(), us_disconnect_notify) => {
            println!("Upstream Connection maintainer unexpectedly failed!");
        }
        () = message_relay(ds_receiver, ds_client, us_client) => {
            println!("Message relay unexpectedly failed!");
        }
    }
}

async fn mqtt_run(mut event_loop: EventLoop, disconnect_notify: Arc<Notify>) {
    loop {
        match event_loop.poll().await {
            Event::Connected => {
                println!("Connected to MQTT broker");
            }
            Event::Disconnected => {
                println!("Disconnected from MQTT broker");
                disconnect_notify.notify_waiters();
            } // Handle other events as needed
        }
    }
}

async fn maintain_connection(client: Client, disconnect_notify: Arc<Notify>) {
    // NOTE: For logging purposes, you want the client ID here.
    loop {
        // Connect to the MQTT broker and wait for the connection to complete
        println!("Attempting to connect to MQTT broker...");
        let connect_properties = ConnectProperties::default(); // Assume clean session = false
        if client
            .connect(connect_properties)
            .await
            .unwrap()
            .await
            .unwrap()
            .is_success()
        {
            disconnect_notify.notified().await;
            println!("Connection lost, will reconnect in 5 seconds...");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        } else {
            eprintln!("Failed to connect to MQTT broker, retrying in 5 seconds...");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }
}

async fn message_relay(mut ds_receiver: Receiver, ds_client: Client, us_client: Client) {
    ds_client
        .subscribe(
            TopicFilter::new(DOWNSTREAM_SUB_FILTER).unwrap(),
            QoS::AtLeastOnce,
            SubscribeProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap()
        .as_result()
        .unwrap();

    while let Some((publish, ack_handle)) = ds_receiver.recv().await {
        // NOTE: Technically we only need to handle QoS1 and QoS0 here since we are subscribing at QoS1,
        // but let's be complete.
        match publish.qos {
            DeliveryQoS::AtMostOnce => {
                let ct = us_client
                    .publish_qos0(publish.topic_name, publish.payload, publish.properties)
                    .await
                    .unwrap();
                match ct.await {
                    Ok(()) => {
                        // Successfully sent the message upstream, acknowledge the downstream message
                        drop(ack_handle);
                    }
                    Err(e) => {
                        eprintln!("Failed to publish message upstream: {e:?}");
                        // Decide how to handle failure to publish upstream
                    }
                }
            }
            DeliveryQoS::AtLeastOnce(_) => {
                let ct = us_client
                    .publish_qos1(publish.topic_name, publish.payload, publish.properties)
                    .await
                    .unwrap();
                match ct.await {
                    Ok(_) => {
                        // Successfully sent the message upstream and received PUBACK, acknowledge the downstream message
                        drop(ack_handle);
                    }
                    Err(e) => {
                        eprintln!("Failed to publish message upstream: {e:?}");
                        // Decide how to handle failure to publish upstream
                    }
                }
            }
            DeliveryQoS::ExactlyOnce(_) => {
                let ct = us_client
                    .publish_qos2(publish.topic_name, publish.payload, publish.properties)
                    .await
                    .unwrap();
                match ct.await {
                    Ok((_, Some(pubrel_token))) => {
                        // Successfully sent the message upstream and received PUBREC
                        // Now send PUBREL and acknowledge the origianl message
                        drop(pubrel_token);
                        drop(ack_handle);
                    }
                    Ok((_, None)) => {
                        eprintln!("Unexpected: PUBREC received with failure code");
                        // Decide how to handle this unexpected case
                    }
                    Err(e) => {
                        eprintln!("Failed to publish message upstream: {e:?}");
                        // Decide how to handle failure to publish upstream
                    }
                }
            }
        }
    }
}
