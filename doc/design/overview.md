# MQTT Client Overview

The MQTT crate has three components which collectively provide client functionality.
They are created together by the `new_client` factory function:

```rust
let (client, connect_handle, receiver) = new_client(options);
```

- `ConnectHandle` / `Connection` — establishes and drives the network connection
- `Client` — sends outgoing operations (publish/subscribe/unsubscribe)
- `Receiver` — receives incoming Publishes

## ConnectHandle / Connection

The connection lifecycle is expressed through ownership rather than an event-polling loop.
A `ConnectHandle` establishes a connection, and the resulting `Connection` is a future that
must be actively driven to send and receive packets.

```rust
match connect_handle.connect(/* transport, clean_start, keep_alive, ... */).await {
    ConnectResult::Success(connection, connack, disconnect_handle) => {
        // `connection` must be driven to keep the client running.
        // Returns a fresh `ConnectHandle` (for reconnecting) and the reason for disconnect.
        let (connect_handle, event) = connection.run_until_disconnect().await;
    }
    ConnectResult::Failure(connect_handle, err) => {
        // `connect_handle` is returned so the connection can be re-attempted.
    }
}
```

`connect` consumes the `ConnectHandle`, and `run_until_disconnect` consumes the `Connection` and
hands back a `ConnectHandle`. This makes illegal states — such as connecting while already
connected — unrepresentable. An application-initiated disconnect is triggered via the
`DisconnectHandle` returned by `connect`. Enhanced authentication uses `connect_enhanced_auth`,
which additionally returns a `ReauthHandle` for later re-authentication.

## Client

Used for sending outgoing data:
- Publish
- Subscribe
- Unsubscribe

The `Client` is cheap and cloneable, so it can be shared across many tasks or threads that
multiplex their operations over the single shared connection. Re-authentication is handled
separately via the `ReauthHandle` (see above), since an AUTH packet is only valid on a
connection established with a matching authentication method.

### Simple
```rust
// Publish a message to the topic (with no regard for the acknowledgement)
client.publish_qos1(
    TopicName::new("test/topic").unwrap(),  // Topic
    "Hello, MQTT!".into(),                  // Payload (bytes)
    false,                                  // Retain
    PublishProperties::default()            // Properties
).await.unwrap();
```

### Result Reporting + Completion Tokens
Result reporting uses a tiered approach that looks like this:

### QoS1
```rust
let client_result: Result<PublishQoS1CompletionToken, DetachedError> = client.publish_qos1(
    TopicName::new("test/topic").unwrap(),  // Topic
    "Hello, MQTT!".into(),                  // Payload (bytes)
    false,                                  // Retain
    PublishProperties::default()            // Properties
).await;
let ct = client_result.unwrap();
let completion_result: Result<PubAck, CompletionError> = ct.await;
let pub_ack = completion_result.unwrap();

// Inspect the PUBACK for details about the operation.
if pub_ack.is_success() {
    println!("Publish succeeded!");
} else {
    println!("Publish failed: {:?}", pub_ack.reason);
}

// Alternatively, convert into a Result for better ergonomics (especially with the ? operator).
match pub_ack.as_result() {
    Ok(_) => println!("Publish succeeded!"),
    Err(e) => println!("Publish failed: {e}"),
}
```

### QoS2
```rust
let client_result: Result<PublishQoS2CompletionToken, DetachedError> = client.publish_qos2(
    TopicName::new("test/topic").unwrap(),  // Topic
    "Hello, MQTT!".into(),                  // Payload (bytes)
    false,                                  // Retain
    PublishProperties::default()            // Properties
).await;
let ct = client_result.unwrap();
let completion_result: Result<(PubRec, Option<PubRelToken>), CompletionError> = ct.await;
let (pub_rec, pubrel_token) = completion_result.unwrap();

// PUBREC (or any packet) can then be inspected for details about the operation
if pub_rec.is_success() {
    println!("Publish succeeded!");

    // Manually acknowledge the PUBREC, or could simply drop the pubrel_token
    if let Some(pubrel_token) = pubrel_token {
        let ct = pubrel_token.confirm(PubRelProperties::default()).await.unwrap();
        let pubcomp = ct.await.unwrap();
    }
} else {
    println!("Publish failed: {:?}", pub_rec.reason);
    // pubrel_token will be None, there is no need to use it
}
```

QoS 2 packet exchange, duplicate suppression, and session-present recovery are implemented. The
connection-level Receive Maximum and Maximum QoS controls tracked in
[`questions.md`](questions.md#mqtt-5-flow-control-and-capability-limits) remain deferred.

Broadly, the idea is that there are distinct failure types that are able to be reported at different times:
1. Failure of the client to accept the operation because its `ConnectHandle` or `Connection` was dropped (`DetachedError`).
2. Failure of an accepted operation to complete, such as an in-flight SUBSCRIBE canceled by connection loss (`CompletionError`).
3. Failure reported by the server, such as a rejection reason in a PUBACK (`OperationFailure`).

## Receiver
Incoming channel/stream that receives incoming Publishes, along with an optional acknowledgement mechanism. The receiver receives **all** messages, and it is up to the application to send them where they need to go (see "dispatching" section)

### Automatic acknowledgement
```rust
while let Some((publish, _)) = receiver.recv().await {
    println!("Received publish");
}
```

### Manual acknowledgement
```rust
while let Some((publish, ack_handle)) = receiver.recv().await {
    println!("Received publish");

    match ack_handle {
        ManualAcknowledgement::QoS0 => {
            println!("Publish does not require acknowledgment (QoS 0)");
        }
        ManualAcknowledgement::QoS1(puback_token) => {
            let ct = puback_token.accept(PubAckProperties::default()).await.unwrap();
            ct.await.unwrap();
            println!("Publish acknowledged! (QoS 1)");
        }
        ManualAcknowledgement::QoS2(pubrec_token) => {
            let ct = pubrec_token.accept(PubRecProperties::default()).await.unwrap();
            let (pubrel, pubcomp_token) = ct.await.unwrap();
            let ct = pubcomp_token.confirm(PubCompProperties::default()).await.unwrap();
            ct.await.unwrap();
            println!("Publish acknowledged! (QoS 2)");
        }
    }
}
```

If dropped without explicit acknowledgement by the user, the `ManualAcknowledgement` will trigger the acknowledgement process on drop in order to not break ordering rules and respect the MQTT specification requirement of acknowledging all received messages.

### Redelivery / Connection epoch

In order to be maximally compatible with MQTT servers (and in particular, the AIO MQ broker), a `PubAckToken` (used with QoS1) is valid only for the connection epoch in which it was received. Once the connection epoch changes, the token can no longer acknowledge its PUBLISH: a PUBACK from the old epoch is not transmitted on the new connection, and any still-pending completion token resolves with an error. Completion tokens that resolved before the epoch changed are unaffected.

QoS 2 controls are scoped to the MQTT session rather than one connection. They remain valid across
a reconnect when CONNACK reports Session Present, and are canceled when that session expires.

### Dispatching

There is, of course, a need for messages to be able to be dispatched to other locations for processing, as you will not want to block the receive loop. This is a common pattern, and one we will need for our own SDKs, as will presumably many consumers of this library. There would need to be an additional optional component for dispatching messages, although it would need to operate independently of the three components of the "client" and thus is not part of this current architecture, but it is being kept in mind.