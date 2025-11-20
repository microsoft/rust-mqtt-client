// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use azure_mqtt::client::{
    Client, ClientOptions, ConnectResult, Connection, ConnectionTransportConfig,
    ManualAcknowledgement, Receiver, new_client,
};
use azure_mqtt::packet::{
    ConnectProperties, KeepAlive, PubAckProperties, PubCompProperties, PubRecProperties,
    PublishProperties, QoS, RetainHandling, SubscribeProperties,
};
use azure_mqtt::topic::{TopicFilter, TopicName};

const CLIENT_ID: &str = "my_client";
const HOSTNAME: &str = "localhost";
const PORT: u16 = 1883;

#[tokio::main]
async fn main() {
    // This would be a builder pattern in a real implementation.
    let options = ClientOptions {
        client_id: Some(CLIENT_ID.to_string()),
        ..Default::default()
    };
    let (client, connect_handle, receiver) = new_client(options);

    // Connect to the MQTT broker and wait for the connection to complete
    if let ConnectResult::Success(connection, _, _) = tokio::task::spawn(connect_handle.connect(
        ConnectionTransportConfig::Tcp {
            hostname: HOSTNAME.to_string(),
            port: PORT,
        },
        false,
        KeepAlive::Infinite,
        None,
        None,
        None,
        ConnectProperties::default(),
        None,
    ))
    .await
    .unwrap()
    {
        println!("Connected to MQTT broker");

        tokio::select! {
            () = connection_runner(connection) => {
                // Connection runner finished
            }
            () = receive(receiver) => {
                // Receiver finished
            }
            () = program(client) => {
                // Program finished
            }
        }
    } else {
        println!("Failed to connect to MQTT broker");
    }
}

async fn program(client: Client) {
    // Subscribe to a topic and wait for the subscription to complete
    let subscribe_properties = SubscribeProperties::default();
    let ct = client
        .subscribe(
            TopicFilter::new("test/topic").unwrap(),
            QoS::AtLeastOnce,
            false,
            false,
            RetainHandling::DoNotSend,
            subscribe_properties,
        )
        .await
        .unwrap();
    match ct.await {
        Ok(_) => println!("Subscribed to topic successfully"),
        Err(e) => eprintln!("Failed to subscribe: {e:?}"),
    }

    loop {
        // Publish a message to the topic (with no regard for the acknowledgement)
        let publish_properties = PublishProperties::default();
        client
            .publish_qos1(
                TopicName::new("test/topic").unwrap(),
                "Hello, MQTT!".into(),
                false,
                publish_properties,
            )
            .await
            .unwrap();
        println!("Published message to topic");
        // Sleep for a while before publishing again
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn connection_runner(connection: Connection) {
    _ = connection.run_until_disconnect().await;
    println!("Disconnected from MQTT broker");
}

async fn receive(mut receiver: Receiver) {
    loop {
        while let Some((publish, ack)) = receiver.recv().await {
            // NOTE: If you don't want manual ack, simply ignore it by using a _, and it
            // will be acked automatically on drop.
            // No need for "manual ack" setting on the client.

            // NOTE: Delegate any of this to another task if you like.

            match ack {
                ManualAcknowledgement::QoS0 => {
                    println!("Received publish on QoS 0");
                    println!("Publish does not require acknowledgment (QoS 0)");
                }
                ManualAcknowledgement::QoS1(puback_token) => {
                    println!("Received publish on QoS 1");
                    let ct = puback_token
                        .accept(PubAckProperties::default())
                        .await
                        .unwrap();
                    ct.await.unwrap();
                    println!("Publish acknowledged! (QoS 1)");
                }
                ManualAcknowledgement::QoS2(pubrec_token) => {
                    println!("Received publish on QoS 2");
                    let ct = pubrec_token
                        .accept(PubRecProperties::default())
                        .await
                        .unwrap();
                    let (_pubrel, pubcomp_token) = ct.await.unwrap();
                    let ct = pubcomp_token
                        .confirm(PubCompProperties::default())
                        .await
                        .unwrap();
                    ct.await.unwrap();

                    println!("Publish acknowledged! (QoS 2)");
                }
            }

            println!("Payload: {:?}", String::from_utf8_lossy(&publish.payload));
        }
    }
}
