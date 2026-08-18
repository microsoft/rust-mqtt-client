# Queueing Considerations

The queue architecture and packet-specific connection-loss behavior are summarized in [Connection-Loss Edge Cases](edge_case_behavior.md). This document expands on the tradeoffs of the unbounded acknowledgement and incoming PUBLISH channels.

## Acknowledgement Backpressure

The acknowledgement channel is deliberately unbounded so acknowledgement tokens can synchronously submit their default response from `Drop`; acknowledgement ordering remains governed by the internal session state.

- There is no producer backpressure. Once polled, explicit acknowledgement methods enqueue without waiting for capacity, and token `Drop` enqueues immediately. Submission fails only after the session-side receiver has been dropped.
- Each MQTT direction has 65,535 packet identifiers, which bounds the number of concurrently active QoS 1 and QoS 2 transactions in that direction. It does not bound the number or size of acknowledgement requests retained over time. An incoming QoS 1 transaction produces a PUBACK request; a QoS 2 transaction can produce PUBREC and PUBCOMP requests on the incoming path or a PUBREL request on the outgoing path. Each request retains its packet and completion state, and an explicit request can also retain heap-backed reason strings and user properties until it is drained.
- A QoS 1 PUBACK request is valid only for its connection epoch. The intended QoS 2 PUBREC, PUBREL, and PUBCOMP flows are scoped to the MQTT session and may continue across resumed connections. These lifetimes determine whether a request is still valid, but do not remove it from the channel: old-epoch or expired-session requests cannot be discarded until the session drains and validates them.
- Only an actively driven `Connection` drains the channel. Requests remain allocated while the connection driver is stalled or absent, and retained tokens can enqueue stale requests from earlier connection epochs or MQTT sessions. Total queue memory is therefore bounded by available process memory rather than by the packet identifier space.

Applications should continuously drive the connection and avoid retaining large numbers of acknowledgement tokens or attaching unnecessarily large properties. QoS 2 implementations must account for every phase of the handshake rather than treating the packet identifier limit as a global request or memory bound.

## Incoming PUBLISH Backpressure

The incoming PUBLISH channel is unbounded so the `Connection` can continue processing network traffic independently of how quickly the application drains its `Receiver`.

- There is no producer backpressure between the `Connection` and `Receiver`. A slow or stalled application can retain queued PUBLISH packets, including their topics, payloads, properties, and acknowledgement state, until they are received or the channel is dropped.
- MQTT Receive Maximum and packet identifiers constrain the number of concurrently active incoming QoS 1 and QoS 2 transactions. QoS 0 has neither mechanism, so a server can continue sending QoS 0 PUBLISH packets throughout a connection without a protocol-level bound on the number retained by the client.
- Total queue memory is therefore bounded by available process memory when QoS 0 traffic arrives faster than the application consumes it. Applying backpressure by simply stopping socket reads would also prevent the `Connection` from processing acknowledgements, keep-alive traffic, and other control packets.

Applications should continuously drain the `Receiver` and dispatch slow processing elsewhere. A possible limit for queued QoS 0 messages is tracked in [issue #105](https://github.com/microsoft/rust-mqtt-client/issues/105); any implementation must preserve packet ordering and allow the connection to keep processing protocol traffic.