# MQTT Client Overview

In-progress MQTT crate has three components which collectively provide client functionality:
    - `EventLoop` / `Connection` (semantics pending)
    - `Client`
    - `Receiver`

## EventLoop / Connection

Basically, a "do work" loop that keeps the client connection running.

We are still discussing the exact granularity and final form of event reporting as we assess implementation needs.

```rust
loop {
    match event_loop.poll().await {
        Event::Connected => {
            //stuff
        }
        Event::DesiredDisconnect => {
            // stuff
        }
        Event::KeepAliveExpired => {
            //stuff
        }
        ...
    }
}
```

## "Client"

Used for sending outgoing data:
- Publish
- Subscribe
- Unsubscribe
- Reauthorize

Currently also triggers connect/disconnect, but this functionality may be moved to EventLoop/Connection

### Simple
```rust
// Publish a message to the topic (with no regard for the acknowledgement)
client.publish_qos1(
    TopicName::new("test/topic").unwrap(),  // Topic
    "Hello, MQTT!".into(),                  // Payload (bytes)
    PublishProperties::default()            // Properties
).await.unwrap();
```

### Result Reporting + Completion Tokens
Result reporting will have a tiered approach that looks something like this:

### QoS1
```rust
let client_result: Result<CompletionToken<PubAck>, ClientError> = client.publish_qos1(
    TopicName::from("test/topic").unwrap(), // Topic
    "Hello, MQTT!".into(),                  // Payload (bytes)
    PublishProperties::default()            // Properties
).await;
let completion_token = client_result.unwrap();
let completion_result: Result<PubAck, CompletionError> = completion_token.await;
let pub_ack = completion_result.unwrap();

// PubAck (or any packet) can then inspected for details about the operation
if pub_ack.is_success() {
    println!("Publish succeeded!");
} else {
    println!("Publish failed: {}", pub_ack.reason);
}

// Alternatively, can convert into a Result for better ergonomics (especially with the ? operator)
match pub_ack.as_result() {
    Ok(_) => println!("Publish succeeded!")
    Err(e) => println!("Publish failed: {e}")
}
```

### QoS2
```rust
let client_result: Result<CompletionToken<PubRec>, ClientError> = client.publish_qos2(
    TopicName::new("test/topic").unwrap(), // Topic
    "Hello, MQTT!".into(),                  // Payload (bytes)
    PublishProperties::default()            // Properties
).await;
let completion_token = client_result.unwrap();
let completion_result: Result<(PubRec, Option<PubRelToken>), CompletionError> = completion_token.await;
let (pub_rec, pubrel_token) = completion_result.unwrap();

// PubRec (or any packet) can then inspected for details about the operation
if pub_rec.is_success() {
    println!("Publish succeeded!");
    
    // Manually acknowledge the PUBREC, or could simply drop the pubrel_token
    if let Some(pubrel_token) = pubrel_token {
        let completion_token = pubrel_token.confirm().await.unwrap();
        let pubcomp = completion_token.await.unwrap();
    }
} else {
    println!("Publish failed: {}", pub_ack.reason);
    // pubrel_token will be None, there is no need to use it
}
```

Broadly, the idea is that there are distinct failure types that are able to be reported at different times:
1) Failure of the client to accept the message (i.e. the connection struct was dropped and not running)
2) Failure of the message to be delivered once accepted into the client (e.g. in-flight subscribe cancelled due to connection loss)
3) Failure reported to the client by the broker (i.e. MQTT error codes)

## Receiver
Incoming channel/stream that receives incoming Publishes, along with an optional acknowledgement mechanism. The receiver receives **all** messages, and it is up to the application to send them where they need to go (see "dispatching" section)

### Automatic acknowledgement
```rust
loop {
    while let Some((publish, _)) = receiver.recv().await {
        println!("Received publish");
    }
}
```

### Manual acknowledgement
```rust
loop {
    while let Some((publish, ack_handle)) = receiver.recv().await {
        println!("Received publish");

        match ack_handle {
            AckHandle::QoS0 => {
                println!("Publish does not require acknowledgment (QoS 0)");
            }
            AckHandle::QoS1(puback_token) => {
                let ct = puback_token.accept(PubAckProperties::default()).await.unwrap();
                ct.await.unwrap();
                println!("Publish acknowledged! (QoS 1)");
            }
            AckHandle::QoS2(pubrec_token) => {
                let ct = pubrec_token.accept(PubRecProperties::default()).await.unwrap();
                let (pubrel, pubcomp_token) = ct.await.unwrap();
                let ct = pubcomp_token.confirm(PubCompProperties::default()).await.unwrap();
                ct.await.unwrap();
                println!("Publish acknowledged! (QoS 2)");
            }
        }
    }
}
```

If dropped without explicit acknowledgement by the user, the `AckHandle` will trigger acknowledgement process on drop in order to not break ordering rules and respect the MQTT specification requirement of acknowledging all received messages.

### Redelivery / connection epoch

In order to be maximally compatible with generic brokers (and in partiular, our MQ broker), a `PubAckToken` (used with QoS1) is only valid for a given connection epoch - that is to say that if the connection is lost, any `CompletionToken` returned by the acknowledgement of an `PubAckToken` will return an error indicating cancellation. The associated publish will be redelivered upon next connect (assuming it was not expired by the broker in the meantime).

If using QoS2, `PubRecToken` and `PubCompToken` are not subject to this limitation as all brokers are required to redeliver.

### Dispatching

There is, of course, a need for messages to be able to be dispatched to other locations for processing, as you will not want to block the receive loop. This is a common pattern, and one we will need for our own SDKs, as will presumably many consumers of this library. There would need to be an additional optional component for dispatching messages, although it would need to operate independently of the three components of the "client" and thus is not part of this current architecture, but it is being kept in mind.