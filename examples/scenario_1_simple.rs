// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use azure_mqtt::buffer_pool::bytes::BufferPoolImpl;
use azure_mqtt::client::{
    AckHandle, Client, ClientOptions, Event, EventLoop, Receiver, new_client,
};
use azure_mqtt::packet::{
    ConnectProperties, PubAckProperties, PubCompProperties, PubRecProperties, PublishProperties,
    QoS, SubscribeProperties,
};
use azure_mqtt::topic::{TopicFilter, TopicName};

#[tokio::main]
async fn main() {
    // This would be a builder pattern in a real implementation.
    let options = ClientOptions {
        client_id: "my_client".to_string(),
        queue_size: 10,
    };
    let (client, event_loop, receiver) = new_client(options, BufferPoolImpl, BufferPoolImpl);

    tokio::select! {
        () = connection_runner(event_loop) => {
            // Connection runner finished
        }
        () = receive(receiver) => {
            // Receiver finished
        }
        () = program(client) => {
            // Program finished
        }
    }
}

async fn program(client: Client) {
    // Connect to the MQTT broker and wait for the connection to complete
    let connect_properties = ConnectProperties::default();
    client
        .connect(connect_properties)
        .await
        .unwrap()
        .await
        .unwrap()
        .as_result()
        .unwrap();

    // Subscribe to a topic and wait for the subscription to complete
    let subscribe_properties = SubscribeProperties::default();
    let ct = client
        .subscribe(
            TopicFilter::from("test/topic"),
            QoS::AtLeastOnce,
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
                TopicName::from("test/topic"),
                "Hello, MQTT!".into(),
                publish_properties,
            )
            .await
            .unwrap();

        // Sleep for a while before publishing again
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn connection_runner(mut event_loop: EventLoop<BufferPoolImpl>) {
    loop {
        match event_loop.poll().await {
            Event::Connected => {
                println!("Connected to MQTT broker");
            }
            Event::Disconnected => {
                println!("Disconnected from MQTT broker");
            } // Handle other events as needed
        }
    }
}

async fn receive(mut receiver: Receiver) {
    loop {
        while let Some((publish, ack_handle)) = receiver.recv().await {
            // NOTE: If you don't want manual ack, simply ignore the ack_token by using a _, and it
            // will be acked automatically on drop.
            // No need for "manual ack" setting on the client.

            // NOTE: Delegate any of this to another task if you like.

            match ack_handle {
                AckHandle::QoS0 => {
                    println!("Received publish on QoS 0");
                    println!("Publish does not require acknowledgment (QoS 0)");
                }
                AckHandle::QoS1(puback_token) => {
                    println!("Received publish on QoS 1");
                    let ct = puback_token
                        .accept(PubAckProperties::default())
                        .await
                        .unwrap();
                    ct.await.unwrap();
                    println!("Publish acknowledged! (QoS 1)");
                }
                AckHandle::QoS2(pubrec_token) => {
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
