# Open Questions

## Monitoring
- Should connection state / client operations be separate from network traffic?
- Is user monitoring of ACK exchange, pings, etc. even worth providing vs just logging internally?
- The event-loop concept has since been implemented as `Connection` (driven via `run_until_disconnect`), which reports the reason for disconnection via `DisconnectedEvent`. Finer-grained event reporting is still open.

## Component naming
- See inline comments in source code for naming discussions
- In particular, the naming of `Client` (the outgoing-operations handle) is still under discussion.

## Queueing
- See `edge_case_behavior.md` for more discussion on queueing

## MQTT 5 flow control and capability limits

QoS 2 packet exchange, duplicate suppression, and session recovery are implemented. The following
connection-level controls are required for complete MQTT 5 limit enforcement but are deliberately
deferred:

- **Outbound CONNACK Receive Maximum:** Maintain one connection-scoped quota shared by QoS 1 and
	QoS 2 PUBLISH packets. Consume quota only when a PUBLISH is emitted, replenish it on PUBACK,
	PUBCOMP, or a failed PUBREC, and reset it for every connection.
- **CONNACK Maximum QoS:** Prevent unsupported new and replayed QoS 2 PUBLISH packets while still
	allowing a transaction that has already reached PUBREL to complete.
- **Inbound CONNECT Receive Maximum:** Count distinct incoming QoS 1 and QoS 2 PUBLISH transactions
	per connection, release each count at its terminal acknowledgement, and send DISCONNECT reason
	`0x93` when the server exceeds the negotiated limit.