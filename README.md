# Microsoft MQTT client

[![Build](https://img.shields.io/github/actions/workflow/status/microsoft/rust-mqtt-client/pr.yaml?branch=main&label=build)](https://github.com/microsoft/rust-mqtt-client/actions/workflows/pr.yaml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.88-blue.svg)](Cargo.toml)

<!-- TODO: add the following badges once the crate is published to crates.io
     (they render grey/404 until then), and switch the MSRV badge from the
     static one above to the dynamic endpoint that reads `rust-version`:
[![crates.io](https://img.shields.io/crates/v/ms-mqtt-client.svg)](https://crates.io/crates/ms-mqtt-client)
[![docs.rs](https://docs.rs/ms-mqtt-client/badge.svg)](https://docs.rs/ms-mqtt-client)
[![MSRV](https://img.shields.io/crates/msrv/ms-mqtt-client.svg)](Cargo.toml)
-->

A low-level MQTT 5 client library that prioritizes **correctness** — upholding the protocol's ordering, delivery, and state rules — and **control** — giving the application, rather than the library, the final say over connection lifecycle, task structure, and message handling.

It is built to facilitate demanding, long-lived applications such as edge and IoT services, message relays, and higher-level SDKs — systems that need many concurrent components to share a single reliable connection and to reason precisely about the fate of every operation.

The client is designed for use with any standards-compliant MQTT 5 servers, such as Mosquitto.

## Implementation status

QoS 0 and QoS 1 are supported. The QoS 2 types and methods reserve the intended public API, but end-to-end QoS 2 publishing and receiving are not yet implemented.

See [MQTT 5 feature support](doc/mqtt-support.md) for the detailed protocol and client feature matrix.


## Design

- **Three independent components.** `new_client()` returns a `Client` (outgoing operations), a `ConnectHandle`/`Connection` (connection lifecycle and I/O), and a `Receiver` (incoming publishes). Each can be owned by a different task, so concerns stay cleanly separated.
- **Cloneable connection actor.** The `Client` is a cheap, cloneable handle to a single connection "actor". Because the internal channels are multi-producer, many tasks or threads can multiplex their operations over one shared connection, and the connection task serializes them to preserve protocol ordering and flow control.
- **Lifecycle enforced by the type system.** Connect, run, and reconnect are expressed through ownership (`ConnectHandle` → `Connection` → `ConnectHandle`), so illegal states such as connecting twice or running a disconnected connection are compile errors rather than runtime faults.
- **Tiered result reporting.** The API separately reports the stages applicable to each operation: acceptance by the client, operation-specific completion, and, when provided by the protocol, the server's verdict through an MQTT reason code.
- **Explicit QoS and acknowledgement.** Publishing uses QoS-specific methods, and incoming PUBLISHes expose the acknowledgement control appropriate to their QoS. Applications can handle acknowledgement flows explicitly, while dropping an unused control attempts the default successful response where one is required.
  - **QoS 2 is not yet implemented**
- **You drive the connection.** The library does not drive the MQTT connection in the background; the application chooses its own task topology, reconnect policy, and message dispatch.

## Getting started

### Requirements

- A Tokio runtime
- OpenSSL development libraries discoverable by `pkg-config`

Install the OpenSSL build dependencies for your platform:

```shell
# Debian or Ubuntu
sudo apt-get update
sudo apt-get install pkg-config libssl-dev

# Fedora or RHEL
sudo dnf install pkgconf-pkg-config openssl-devel

# macOS with Homebrew
brew install pkg-config openssl@3
```

TCP and TLS transports are available by default. WebSocket transports are available through the `websockets` feature.

### Simple Connect and Publish

```rust
use std::error::Error;

use ms_mqtt_client::client::{
  ClientOptions, ConnectResult, KeepAliveConfig, new_client,
};
use ms_mqtt_client::packet::{
  ConnectProperties, DisconnectProperties, PublishProperties,
};
use ms_mqtt_client::topic::TopicName;
use ms_mqtt_client::transport::{
  ConnectionTransportConfig, ConnectionTransportType,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  let (client, connect_handle, _receiver) = new_client(ClientOptions::default());
  let result = connect_handle
    .connect(
      ConnectionTransportConfig {
        transport_type: ConnectionTransportType::Tcp {
          hostname: "localhost".into(),
          port: 1883,
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
    .await;

  let (connection, disconnect_handle) = match result {
    ConnectResult::Success(connection, _, disconnect_handle) => {
      (connection, disconnect_handle)
    }
    ConnectResult::Failure(_, error) => return Err(error.into()),
  };
  let connection_task = tokio::spawn(connection.run_until_disconnect());

  let publish_result: Result<(), Box<dyn Error>> = async {
    let puback = client
      .publish_qos1(
        TopicName::new("example/topic")?,
        "hello".into(),
        false,
        PublishProperties::default(),
      )
      .await?
      .await?;
    puback.as_result()?;
    Ok(())
  }
  .await;

  let _ = disconnect_handle.disconnect(&DisconnectProperties::default());
  let _ = connection_task.await?;
  publish_result
}
```

## Canonical usage patterns

The crate documentation and runnable examples are the canonical references for application code and coding assistants. Start from the pattern that matches the intended task:

| Canonical pattern | Reference |
| --- | --- |
| Single-client lifecycle: connect, subscribe, publish, receive, acknowledge, and shut down | [Simple-client example](examples/scenario_1_simple.rs) |
| Reconnect supervisor: retry, resubscribe, and rebuild connection-scoped state | [Document-update example](examples/scenario_2_document_update.rs) |
| Multiple-client supervision: independently reconnect clients and coordinate shutdown | [Message-relay example](examples/scenario_3_relay.rs) |

The [examples guide](examples/README.md) explains how to configure and run these references against an MQTT server.

Code built from these patterns must preserve four invariants:

1. Continuously poll `Connection::run_until_disconnect()` while using the client or receiver; no background task drives MQTT I/O.
2. Distinguish three phases: successful completion of a `Client` operation future means submission, awaiting its completion token reports operation-specific completion, and `as_result()` on an acknowledgement reports the MQTT server's verdict.
3. For an orderly shutdown, call `DisconnectHandle::disconnect()` and keep driving the connection until it returns.
4. Use QoS 0 or QoS 1 only. QoS 2 types and methods reserve a future API and are not implemented end to end.

## Contributing and support

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines and the project's code of conduct. For help, see [SUPPORT.md](SUPPORT.md). Report security vulnerabilities according to [SECURITY.md](SECURITY.md).

## License

See [LICENSE](LICENSE) for details.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
