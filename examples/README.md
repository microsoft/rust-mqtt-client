# Examples

These runnable examples demonstrate the library's canonical end-to-end patterns: a complete
single-client lifecycle, reconnect supervision with connection-scoped state, and coordinated
supervision of multiple clients. Start with the simple-client example, then use the document-update
example for reconnect supervision or the message-relay example for multiple-client supervision.

Run these examples from the repository root. They work with any standards-compliant MQTT 5 server; Mosquitto is a convenient option, not a requirement. The first two examples default to an unauthenticated TCP server on `localhost:1883`. To use another server, update `HOSTNAME`, `PORT`, and any credentials near the top of the example. The relay defaults to separate downstream and upstream servers on `localhost:1883` and `localhost:1884`.

## Install Mosquitto locally

The local walkthroughs use the Mosquitto server and its `mosquitto_pub` command-line client.

On Debian or Ubuntu:

```shell
sudo apt-get update
sudo apt-get install mosquitto mosquitto-clients
```

On Fedora, or RHEL with [EPEL](https://docs.fedoraproject.org/en-US/epel/) enabled:

```shell
sudo dnf install mosquitto
```

On macOS with Homebrew:

```shell
brew install mosquitto
```

If the installation did not start Mosquitto as a service, start it in a separate terminal:

```shell
mosquitto -p 1883
```

If a local Mosquitto service is already listening on port 1883, use that service instead.

## Use the public test server

To run the first two examples without a local server, set `HOSTNAME` to `test.mosquitto.org` and leave `PORT` set to `1883`. This is a shared, unauthenticated, and unencrypted test service: choose unique topic names, do not publish sensitive data, and do not rely on it for availability or performance. Change each example's `CLIENT_ID` to a unique value as well; the server permits only one active connection for a given client ID, so another user running the unchanged example could disconnect your client.

## Simple client

[scenario_1_simple.rs](scenario_1_simple.rs) connects one client, subscribes to `test/topic` at QoS 1, and publishes a message to that topic every five seconds. Received messages are acknowledged manually.

```shell
cargo run --example scenario_1_simple
```

The example prints each publish and its echoed payload. Stop it with `Ctrl-C`.

## Document updates

[scenario_2_document_update.rs](scenario_2_document_update.rs) separates connection management from incoming-message dispatch. It reconnects after connection loss, receives a base document on `watchlist/get`, and appends subsequent UTF-8 payloads from `watchlist/update`.

```shell
cargo run --example scenario_2_document_update
```

After the example reports `Subscribed to watchlist/get`, publish a base document with any MQTT client. For example:

```shell
mosquitto_pub -V mqttv5 -q 1 -t watchlist/get -m "base"
```

Wait for the example to report `Subscribed to watchlist/update`, then publish an update:

```shell
mosquitto_pub -V mqttv5 -q 1 -t watchlist/update -m "+update"
```

The example prints `base+update` as the current document.

## Message relay

[scenario_3_relay.rs](scenario_3_relay.rs) demonstrates a best-effort relay for QoS 0 and QoS 1 messages between two MQTT servers. Each message gets one upstream publish operation, and failures are logged without retrying. QoS 0 has no acknowledgement; its completion token reports when the upstream session releases the PUBLISH for transmission. For QoS 1, the relay waits for the upstream result before acknowledging downstream, but it acknowledges downstream even if the upstream operation fails or the server rejects it.

The server on port 1883 from the local setup above is the downstream server. Start a second Mosquitto process as the upstream server in another terminal:

```shell
mosquitto -p 1884
```

These must be separate server processes. Different listeners on one server still share a topic space and would feed the relayed `downstream/#` messages back into the relay.

Start an upstream subscriber so the relayed message is visible:

```shell
mosquitto_sub -V mqttv5 -p 1884 -t 'downstream/#' -v
```

Run the relay:

```shell
cargo run --example scenario_3_relay
```

After the downstream client reports that it is subscribed and the upstream client reports that it is connected, publish a message to the downstream server:

```shell
mosquitto_pub -V mqttv5 -p 1883 -q 1 -t downstream/example -m "relayed message"
```

The subscriber connected to port 1884 prints `downstream/example relayed message`.