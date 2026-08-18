// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{error::Error, str};

use ms_mqtt_client::client::{
    Client, ClientOptions, ConnectResult, DisconnectedEvent, KeepAliveConfig,
    ManualAcknowledgement, Receiver, new_client,
};
use ms_mqtt_client::packet::{
    ConnectProperties, DisconnectProperties, PubAckProperties, PubRejectReason, PublishProperties,
    QoS, RetainOptions, SubscribeProperties,
};
use ms_mqtt_client::topic::{TopicFilter, TopicName};
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};

const CLIENT_ID: &str = "my_client";
const HOSTNAME: &str = "localhost";
const PORT: u16 = 1883;
const TOPIC: &str = "test/topic";

type ExampleResult = Result<(), Box<dyn Error>>;

#[tokio::main]
async fn main() -> ExampleResult {
    // This would be a builder pattern in a real implementation.
    let options = ClientOptions {
        client_id: Some(CLIENT_ID.to_string()),
        ..Default::default()
    };
    let (client, connect_handle, receiver) = new_client(options);

    // Connect to an MQTT server.
    // Use clean start so this run does not inherit subscriptions from an earlier session.
    let (connection, disconnect_handle) = match connect_handle
        .connect(
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
        )
        .await
    {
        ConnectResult::Success(connection, _, disconnect_handle) => (connection, disconnect_handle),
        ConnectResult::Failure(_, error) => return Err(error.into()),
    };
    println!("Connected to MQTT server");

    // This task only translates Ctrl+C into a disconnect request. MQTT I/O remains driven by
    // `run_until_disconnect` below.
    let _ctrl_c_task = tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                println!("Ctrl+C received; disconnecting from MQTT server...");
                if let Err(error) = disconnect_handle.disconnect(&DisconnectProperties::default()) {
                    eprintln!("Failed to request disconnect: {error}");
                }
            }
            Err(error) => eprintln!("Failed to listen for Ctrl+C: {error}"),
        }
    });

    // MQTT I/O progresses only while the connection future is polled. These workflows are
    // long-lived; when any one returns, `select!` cancels the others and the example exits.
    tokio::select! {
        (connect_handle, event) = connection.run_until_disconnect() => {
            // This example does not reconnect, so discard the returned session handle.
            drop(connect_handle);
            match event {
                DisconnectedEvent::ApplicationDisconnect => {
                    println!("Disconnected from MQTT server: ApplicationDisconnect");
                    Ok(())
                }
                event => Err(format!("Connection ended unexpectedly: {event:?}").into()),
            }
        }
        result = receive(receiver) => {
            result?;
            Err("Receiver closed unexpectedly".into())
        }
        result = program(client) => {
            result?;
            Err("Program exited unexpectedly".into())
        }
    }
}

async fn program(client: Client) -> ExampleResult {
    let topic_filter = TopicFilter::new(TOPIC)?;
    let topic_name = TopicName::new(TOPIC)?;

    // Subscribe to a topic and wait for the subscription to complete
    let ct = client
        .subscribe(
            topic_filter,
            QoS::AtLeastOnce,
            false,
            RetainOptions::default(),
            SubscribeProperties::default(),
        )
        .await?;
    let suback = ct.await?;
    suback.as_result()?;
    println!("Subscribed to topic successfully");

    loop {
        let ct = client
            .publish_qos1(
                topic_name.clone(),
                "Hello, MQTT!".into(),
                false,
                PublishProperties::default(),
            )
            .await?;
        // NOTE: The caller need not await the completion token. Dropping it explicitly or leaving
        // the returned token unbound does not cancel the accepted PUBLISH.
        let puback = ct.await?;
        puback.as_result()?;
        println!("Published message to topic");
        // Sleep for a while before publishing again
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn receive(mut receiver: Receiver) -> ExampleResult {
    while let Some((publish, manual_ack)) = receiver.recv().await {
        // NOTE: Explicit acknowledgement is not required. Drop `manual_ack` or leave it unbound by
        // replacing it with `_` in the loop pattern. When acknowledgement is required,
        // auto-acknowledgement is performed with default properties.

        // NOTE: In real applications, delegate message processing and acknowledgement to managed
        // worker tasks. Slow work here delays subsequent messages and can let the unbounded
        // receive queue grow.

        // QoS 0 requires no acknowledgement. Dropping a QoS 1 `manual_ack`, including on early
        // return or panic, attempts a successful default PUBACK. Keep expected processing failures
        // as values so the QoS 1 path can explicitly accept or reject them below.
        let processing_result = process_payload(&publish.payload);
        match manual_ack {
            ManualAcknowledgement::QoS0 => {
                if let Err(error) = processing_result {
                    eprintln!("Discarded invalid UTF-8 payload: {error}");
                } else {
                    println!("Received publish on QoS 0");
                    println!("Publish does not require acknowledgment (QoS 0)");
                }
            }
            ManualAcknowledgement::QoS1(puback_token) => {
                if let Err(error) = processing_result {
                    let ct = puback_token
                        .reject(
                            PubRejectReason::PayloadFormatInvalid,
                            PubAckProperties::default(),
                        )
                        .await?;
                    ct.await?;
                    eprintln!("Rejected invalid UTF-8 payload: {error}");
                } else {
                    println!("Received publish on QoS 1");
                    // NOTE: Dropping `puback_token` instead performs auto-acknowledgement with
                    // default properties.
                    let ct = puback_token.accept(PubAckProperties::default()).await?;
                    ct.await?;
                    println!("Publish acknowledged! (QoS 1)");
                }
            }
            ManualAcknowledgement::QoS2(_) => unreachable!(
                "the subscription requests a maximum of QoS 1, and QoS 2 is not supported"
            ),
        }
    }

    Ok(())
}

fn process_payload(payload: &[u8]) -> Result<(), str::Utf8Error> {
    let payload = str::from_utf8(payload)?;
    println!("Payload: {payload:?}");
    Ok(())
}
