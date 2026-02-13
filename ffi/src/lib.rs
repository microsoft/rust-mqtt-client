// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{
    ffi::c_char,
};

use bytes::Bytes;

use azure_mqtt as rust;

pub type PacketIdentifier = u16;

#[repr(C)]
pub struct ClientOptions {
    pub client_id: *const c_char,
    pub max_packet_identifier: u16,
    pub publish_qos0_queue_size: usize,
    pub publish_qos1_qos2_queue_size: usize,
}

pub type ClientPtr = *mut rust::client::Client;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn client_free(client: ClientPtr) {
    let client = unsafe { Box::from_raw(client) };
    drop(client);
}

pub type ConnectHandlePtr = *mut rust::client::ConnectHandle;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn connect_handle_free(connect_handle: ConnectHandlePtr) {
    let connect_handle = unsafe { Box::from_raw(connect_handle) };
    drop(connect_handle);
}

pub type ReceiverPtr = *mut rust::client::Receiver;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn receiver_free(receiver: ReceiverPtr) {
    let receiver = unsafe { Box::from_raw(receiver) };
    drop(receiver);
}

#[repr(C)]
pub struct NewClient {
    pub client: ClientPtr,
    pub connect_handle: ConnectHandlePtr,
    pub receiver: ReceiverPtr,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn new_client(options: ClientOptions) -> NewClient {
    let client_id =
        if options.client_id.is_null() {
            None
        }
        else {
            let client_id = unsafe { std::ffi::CStr::from_ptr(options.client_id) };
            Some(client_id.to_string_lossy().into_owned())
        };
    let max_packet_identifier = rust::packet::PacketIdentifier::new(options.max_packet_identifier).unwrap_or(rust::packet::PacketIdentifier::MAX);
    let options = rust::client::ClientOptions {
        client_id,
        max_packet_identifier,
        publish_qos0_queue_size: options.publish_qos0_queue_size,
        publish_qos1_qos2_queue_size: options.publish_qos1_qos2_queue_size,
    };

    let (client, connect_handle, receiver) = rust::client::new_client(options);
    let client = Box::into_raw(Box::new(client));
    println!("created client at 0x{:016x}", client.addr());
    let connect_handle = Box::into_raw(Box::new(connect_handle));
    let receiver = Box::into_raw(Box::new(receiver));

    NewClient {
        client,
        connect_handle,
        receiver,
    }
}

#[repr(C)]
pub struct ConnectionTransportConfigTcp {
    pub hostname: *const c_char,
    port: u16,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn connect_handle_connect_tcp(connect_handle: ConnectHandlePtr, connection_transport: ConnectionTransportConfigTcp) -> bool {
    let connect_handle = unsafe { std::ptr::read(connect_handle) };
    let connection_transport = {
        let hostname = unsafe { std::ffi::CStr::from_ptr(connection_transport.hostname) };
        let hostname = hostname.to_string_lossy().into_owned();

        rust::client::ConnectionTransportConfig {
            transport_type: rust::client::ConnectionTransportType::Tcp {
                hostname,
                port: connection_transport.port,
            },
            timeout: None,
        }
    };

    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let result = runtime.block_on(async {
            let result = connect_handle.connect(
                connection_transport,
                false,
                rust::client::KeepAliveConfig::Infinite,
                None,
                None,
                None,
                rust::packet::ConnectProperties::default(),
                None,
            ).await;
            match result {
                rust::client::ConnectResult::Success(connection, _, _) => {
                    println!("Success");
                    tx.send(true);
                    _ = connection.run_until_disconnect().await;
                    // TODO: return .await result
                    Ok(())
                },
                rust::client::ConnectResult::Failure(_, err) => {
                    println!("Failure: {err}");
                    tx.send(false);
                    // TODO: return actual err
                    Err(())
                },
            }
        });
        println!("bg thread ended with {result:?}");
    });

    rx.recv().unwrap()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn client_publish_qos0(client: *const rust::client::Client, topic_name: *const c_char, payload: *const u8, payload_len: usize) {
    let client = unsafe { &*client };
    let topic_name = {
        let topic_name = unsafe { std::ffi::CStr::from_ptr(topic_name) };
        let topic_name = topic_name.to_string_lossy().into_owned();
        rust::topic::TopicName::new(topic_name).unwrap()
    };
    let payload = {
        let payload = unsafe { std::slice::from_raw_parts(payload, payload_len) };
        Bytes::copy_from_slice(payload)
    };
    println!("received client at 0x{:016x}", <*const rust::client::Client>::addr(client));
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let result = runtime.block_on(client.publish_qos0(topic_name, payload, false, Default::default()));
    println!("publish result: {result:?}");
}
