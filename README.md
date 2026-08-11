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

The client id designed to be compatible with any MQTT broker, such as Mosquitto MQTT Broker or Azure Event Grid MQTT Broker.

## Design

- **Three independent components.** `new_client()` returns a `Client` (outgoing operations), a `ConnectHandle`/`Connection` (connection lifecycle and I/O), and a `Receiver` (incoming publishes). Each can be owned by a different task, so concerns stay cleanly separated.
- **Cloneable connection actor.** The `Client` is a cheap, cloneable handle to a single connection "actor". Because the internal channels are multi-producer, many tasks or threads can multiplex their operations over one shared connection, and the connection task serializes them to preserve protocol ordering and flow control.
- **Lifecycle enforced by the type system.** Connect, run, and reconnect are expressed through ownership (`ConnectHandle` → `Connection` → `ConnectHandle`), so illegal states such as connecting twice or running a disconnected connection are compile errors rather than runtime faults.
- **Tiered result reporting.** Every operation distinguishes acceptance by the client, completion on the network, and the broker's own verdict (MQTT reason codes) — each surfaced at the moment it becomes known, via awaitable completion tokens.
- **Explicit QoS and acknowledgement.** QoS 0/1/2 have dedicated methods and token types that make each packet flow explicit, and incoming messages are acknowledged automatically on drop while still allowing full manual control.
- **You own the runtime.** The library spawns no background tasks; the application drives the connection and chooses its own task topology, reconnect policy, and message dispatch.

## License

See [LICENSE](LICENSE) for details.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
