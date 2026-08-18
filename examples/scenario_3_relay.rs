// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{error::Error, pin::pin, time::Duration};

use tokio::sync::watch;

use ms_mqtt_client::client::{
    Client, ClientOptions, ConnectHandle, ConnectResult, KeepAliveConfig, Receiver, new_client,
};
use ms_mqtt_client::packet::{
    ConnectProperties, DeliveryQoS, DisconnectProperties, QoS, RetainOptions, SubscribeProperties,
};
use ms_mqtt_client::topic::TopicFilter;
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};

const DOWNSTREAM_CLIENT_ID: &str = "downstream_client";
const UPSTREAM_CLIENT_ID: &str = "upstream_client";
// Keep these on separate MQTT servers; sharing one topic space would feed relayed
// `downstream/#` messages back into the relay.
const DOWNSTREAM_HOSTNAME: &str = "localhost";
const DOWNSTREAM_PORT: u16 = 1883;
const UPSTREAM_HOSTNAME: &str = "localhost";
const UPSTREAM_PORT: u16 = 1884;

const DOWNSTREAM_SUB_FILTER: &str = "downstream/#";

type ExampleResult = Result<(), Box<dyn Error>>;

#[tokio::main]
async fn main() -> ExampleResult {
    // Downstream client
    let options = ClientOptions {
        client_id: Some(DOWNSTREAM_CLIENT_ID.to_string()),
        ..Default::default()
    };
    let (ds_client, ds_connect_handle, ds_receiver) = new_client(options);

    // Upstream client
    let options = ClientOptions {
        client_id: Some(UPSTREAM_CLIENT_ID.to_string()),
        ..Default::default()
    };
    let (us_client, us_connect_handle, _) = new_client(options);

    // One shutdown signal coordinates both independently reconnecting clients.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let _ctrl_c_task = tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => println!("Ctrl+C received; stopping relay..."),
            Err(error) => eprintln!("Failed to listen for Ctrl+C: {error}; stopping relay..."),
        }
        shutdown_tx.send_replace(true);
    });

    // Wait for both clients and the relay. Unlike `select!`, this lets both connections complete
    // orderly shutdown and lets the relay drain messages already buffered by `Receiver`.
    tokio::try_join!(
        maintain_connection(
            "Downstream",
            DOWNSTREAM_HOSTNAME,
            DOWNSTREAM_PORT,
            ds_connect_handle,
            Some(ds_client),
            shutdown_rx.clone(),
        ),
        maintain_connection(
            "Upstream",
            UPSTREAM_HOSTNAME,
            UPSTREAM_PORT,
            us_connect_handle,
            None,
            shutdown_rx.clone(),
        ),
        message_relay(ds_receiver, us_client, shutdown_rx),
    )?;
    Ok(())
}

async fn maintain_connection(
    label: &'static str,
    hostname: &'static str,
    port: u16,
    mut connect_handle: ConnectHandle,
    subscription_client: Option<Client>,
    mut shutdown: watch::Receiver<bool>,
) -> ExampleResult {
    loop {
        println!("[{label}] Attempting to connect to MQTT server at {hostname}:{port}...");
        // The best-effort relay does not resume broker session state; every connection starts
        // fresh, and the downstream client subscribes again below.
        let connect_result = tokio::select! {
            result = connect_handle.connect(
                    ConnectionTransportConfig {
                        transport_type: ConnectionTransportType::Tcp {
                            hostname: hostname.to_string(),
                            port,
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
            () = wait_for_shutdown(&mut shutdown) => {
                println!("[{label}] Canceling connection attempt...");
                return Ok(());
            }
        };

        connect_handle = match connect_result {
            ConnectResult::Success(connection, _, disconnect_handle) => {
                println!("[{label}] Connected to MQTT server at {hostname}:{port}");
                let mut connection_driver = pin!(connection.run_until_disconnect());
                // Only the downstream connection has setup work. Keep polling the connection
                // driver while its subscription is submitted and completed.
                let mut subscribed = subscription_client.is_none();
                let (connect_handle, event) = loop {
                    tokio::select! {
                        result = &mut connection_driver => break result,
                        result = async {
                            if let Some(client) = &subscription_client {
                                subscribe_downstream(client).await
                            } else {
                                Ok(())
                            }
                        }, if !subscribed => {
                            result?;
                            subscribed = true;
                        }
                        () = wait_for_shutdown(&mut shutdown) => {
                            println!("[{label}] Disconnecting from MQTT server...");
                            // Requesting disconnect does not drive I/O; keep polling the connection
                            // driver so it can send DISCONNECT and finish shutdown.
                            disconnect_handle.disconnect(&DisconnectProperties::default())?;
                            let (connect_handle, event) = connection_driver.await;
                            println!("[{label}] Disconnected from MQTT server: {event:?}");
                            drop(connect_handle);
                            return Ok(());
                        }
                    }
                };
                println!(
                    "[{label}] Disconnected from MQTT server: {event:?}; reconnecting in 5 seconds..."
                );
                connect_handle
            }
            ConnectResult::Failure(connect_handle, error) => {
                eprintln!(
                    "[{label}] Failed to connect to MQTT server: {error}; retrying in 5 seconds..."
                );
                connect_handle
            }
        };

        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(5)) => {}
            () = wait_for_shutdown(&mut shutdown) => {
                println!("[{label}] Stopping reconnect attempts...");
                return Ok(());
            }
        }
    }
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    // A losing `select!` branch can be canceled after the value changes, so check the retained
    // state before waiting for another change.
    let requested = *shutdown.borrow();
    if !requested {
        let _ = shutdown.changed().await;
    }
}

async fn subscribe_downstream(client: &Client) -> ExampleResult {
    client
        .subscribe(
            TopicFilter::new(DOWNSTREAM_SUB_FILTER)?,
            QoS::AtLeastOnce,
            false,
            RetainOptions::default(),
            SubscribeProperties::default(),
        )
        .await?
        .await?
        .as_result()?;
    println!("[Downstream] Subscribed to downstream messages");
    Ok(())
}

async fn message_relay(
    mut ds_receiver: Receiver,
    us_client: Client,
    shutdown: watch::Receiver<bool>,
) -> ExampleResult {
    // This example relays one message at a time so acknowledgement follows one complete upstream
    // attempt. A production relay can use managed workers, but must bound queued work explicitly.
    while let Some((publish, manual_ack)) = ds_receiver.recv().await {
        // Republish as non-retained; forwarding an incoming RETAIN flag could replace retained
        // state on the upstream server.
        match publish.qos {
            DeliveryQoS::AtMostOnce => {
                let ct = match us_client
                    .publish_qos0(
                        publish.topic_name,
                        publish.payload,
                        false,
                        publish.properties,
                    )
                    .await
                {
                    Ok(completion) => completion,
                    Err(error) => {
                        eprintln!("Failed to submit upstream QoS 0 PUBLISH: {error}");
                        drop(manual_ack);
                        continue;
                    }
                };
                if let Err(error) = ct.await {
                    eprintln!("Upstream QoS 0 PUBLISH did not complete: {error}");
                }

                // QoS 0 completion means release for transmission, not server receipt.
                // The downstream QoS 0 delivery itself requires no acknowledgement.
                drop(manual_ack);
            }
            DeliveryQoS::AtLeastOnce(_) => {
                let ct = match us_client
                    .publish_qos1(
                        publish.topic_name,
                        publish.payload,
                        false,
                        publish.properties,
                    )
                    .await
                {
                    Ok(completion) => completion,
                    Err(error) => {
                        eprintln!("Failed to submit upstream QoS 1 PUBLISH: {error}");
                        drop(manual_ack);
                        continue;
                    }
                };
                match ct.await {
                    Ok(puback) => {
                        if let Err(error) = puback.as_result() {
                            eprintln!("Upstream server rejected the PUBLISH: {error}");
                        }
                    }
                    Err(error) => {
                        eprintln!("Upstream QoS 1 PUBLISH did not complete: {error}");
                    }
                }

                // This relay is best effort: after one upstream attempt, dropping the downstream
                // token attempts acknowledgement with default properties even if upstream failed.
                drop(manual_ack);
            }
            DeliveryQoS::ExactlyOnce(_) => {
                unreachable!("the downstream subscription requests a maximum of QoS 1");
            }
        }
    }

    if *shutdown.borrow() {
        Ok(())
    } else {
        Err("Downstream receiver closed unexpectedly".into())
    }
}
