// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use azure_mqtt::client::{
    Client, ClientOptions, ConnectHandle, ConnectResponse, ConnectionTransportConfig, Receiver,
    new_client,
};
use azure_mqtt::packet::{
    ConnectOptions, ConnectProperties, DeliveryQoS, KeepAlive, QoS, RetainHandling,
    SubscribeProperties,
};
use azure_mqtt::topic::TopicFilter;

const DOWNSTREAM_CLIENT_ID: &str = "downstream_client";
const UPSTREAM_CLIENT_ID: &str = "upstream_client";
const HOSTNAME: &str = "localhost";
const PORT: u16 = 1883;

const DOWNSTREAM_SUB_FILTER: &str = "downstream/#";

#[tokio::main]
async fn main() {
    // Downstream client
    let options = ClientOptions {
        client_id: Some(DOWNSTREAM_CLIENT_ID.to_string()),
        queue_size: 10,
    };
    let (ds_client, ds_connect_handle, ds_receiver) = new_client(options);

    // Upstream client
    let options = ClientOptions {
        client_id: Some(UPSTREAM_CLIENT_ID.to_string()),
        queue_size: 10,
    };
    let (us_client, us_connect_handle, _) = new_client(options);

    tokio::select! {
        () = mqtt_run(ds_connect_handle) => {
            println!("Downstream Connection runner unexpectedly failed!");
        }
        () = mqtt_run(us_connect_handle) => {
            println!("Upstream Connection runner unexpectedly failed!");
        }
        () = message_relay(ds_receiver, ds_client, us_client) => {
            println!("Message relay unexpectedly failed!");
        }
    }
}

async fn mqtt_run(mut connect_handle: ConnectHandle) {
    loop {
        println!("Attempting to connect to MQTT broker...");
        connect_handle = match connect_handle
            .connect(
                ConnectionTransportConfig::Tcp {
                    hostname: HOSTNAME.to_string(),
                    port: PORT,
                },
                false,
                KeepAlive::Infinite,
                ConnectOptions::default(),
                ConnectProperties::default(),
            )
            .await
        {
            ConnectResponse::Success(connected, _, _) => {
                println!("Connected to MQTT broker");
                connect_handle = connected.run_until_disconnect().await.0;
                println!("Disconnected from MQTT broker");
                println!("Connection lost, will reconnect in 5 seconds...");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                connect_handle
            }
            ConnectResponse::Failure(connect_handle, _)
            | ConnectResponse::Timeout(connect_handle) => {
                println!("Failed to connect to MQTT broker, retrying in 5 seconds...");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                connect_handle
            }
        }
    }
}

async fn message_relay(mut ds_receiver: Receiver, ds_client: Client, us_client: Client) {
    ds_client
        .subscribe(
            TopicFilter::new(DOWNSTREAM_SUB_FILTER).unwrap(),
            QoS::AtLeastOnce,
            false,
            false,
            RetainHandling::DoNotSend,
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
                    .publish_qos0(
                        publish.topic_name,
                        publish.payload,
                        false,
                        publish.properties,
                    )
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
                    .publish_qos1(
                        publish.topic_name,
                        publish.payload,
                        false,
                        publish.properties,
                    )
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
                    .publish_qos2(
                        publish.topic_name,
                        publish.payload,
                        false,
                        publish.properties,
                    )
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
