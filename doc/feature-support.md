# MQTT 5 client feature support

This document describes the MQTT features supported by `ms-mqtt-client`. **The client supports
MQTT 5.0 only.** This is a client capability summary, not a broker compatibility matrix or a claim
of complete MQTT 5 conformance.

MQTT deliberately divides behavior between clients and servers. For example, this client can
request a retained publication, a persistent session, or a shared subscription, but the server
stores and routes the corresponding messages. Those features are listed as supported when the
client implements the required MQTT flow; server availability is called out separately.

## Status definitions

- **✓**: the public client API and session state machine implement the feature end to
  end.
- **Partial**: the public API or wire representation exists, but some required state management,
  enforcement, or convenience behavior is not implemented.
- **✗**: applications must not use the feature. A public packet or token type alone
  does not imply support.

## Protocol Features

These are behaviors and capabilities defined by MQTT 5.0 that the client implements when
communicating with a server.

| Feature | Status | Notes |
| --- | --- | --- |
| QoS 0 publish and receive | ✓ | At-most-once delivery; there is no server acknowledgement. |
| QoS 1 publish and receive | ✓ | At-least-once delivery is supported in both directions. |
| QoS 2 publish and receive | ✗ | Public types reserve a future API, but QoS 2 methods and receive paths are not implemented and can panic. |
| Publication acknowledgement outcomes | ✓ | Supported acknowledgement flows allow received publications to be accepted or rejected using MQTT 5 reason codes and properties. |
| In-flight retransmission after session resume | ✓ | Supported QoS flows retransmit unacknowledged PUBLISH and PUBREL packets with their original packet identifiers when the server resumes the MQTT session, setting DUP on retransmitted PUBLISH packets. |
| Packet ordering | ✓ | The session preserves original PUBLISH order during retransmission and emits PUBACK packets in incoming PUBLISH order, even when the application acknowledges publications out of order. |
| Message Expiry Interval | ✓ | The property is sent and received. The server expires queued copies and reduces the interval by time spent waiting before onward delivery; MQTT does not require publishing clients to age locally queued or retransmitted messages. |
| Payload metadata | ✓ | Payload Format Indicator and Content Type are exposed on PUBLISH and Will messages. |
| Request/response metadata | ✓ | Response Topic, Correlation Data, and Response Information are exposed; request correlation and dispatch remain application concerns. |
| Subscribe and unsubscribe | ✓ | One topic filter per client operation, at maximum QoS 0 or QoS 1. |
| Multiple filters per SUBSCRIBE or UNSUBSCRIBE | ✗ | MQTT permits multiple topic filters in one packet, but each public client operation accepts only one. |
| Wildcard and shared subscriptions | ✓ | Single-level (`+`), multi-level (`#`), and shared (`$share/<group>/<filter>`) filters are supported when available on the server. |
| Subscription Identifiers | ✓ | An identifier can be sent in SUBSCRIBE and is exposed on matching incoming PUBLISH packets when supported by the server. |
| Subscription options | ✓ | No Local, Retain As Published, and all three Retain Handling values are exposed. |
| Retained messages | ✓ | The client exposes the PUBLISH retain flag and MQTT 5 retained-message subscription options. Storage and delivery are server behavior. |
| Will messages | ✓ | Includes Will Delay Interval and the other MQTT 5 Will properties. Publication after an ungraceful disconnect is server behavior. |
| Disconnect with Will Message | ✗ | The public disconnect API always sends Normal Disconnection (`0x00`) and cannot request publication of the configured Will with reason code `0x04`. |
| Keep alive | ✓ | Automatic PINGREQ, PINGRESP timeout detection, and the Server Keep Alive override are implemented. |
| Manual PINGREQ | ✗ | MQTT 5 permits PINGREQ at any time, irrespective of Keep Alive, but there is no public operation to send one on demand. |
| Server-assigned Client Identifier | ✓ | With Clean Start enabled, setting `ClientOptions::client_id` to `None` requests a server-assigned identifier, which is exposed from CONNACK as `ConnAckProperties::assigned_client_identifier`. |
| Session negotiation and continuation | ✓ | Clean Start, Session Expiry Interval, and Session Present are supported. The server owns persisted session state. |
| Orderly and server-initiated disconnect | ✓ | Normal client DISCONNECT and incoming server DISCONNECT reason codes, properties, and connection outcomes are exposed. |
| Server redirection | ✓ | Use Another Server, Server Moved, and Server Reference are exposed from rejected CONNACK and server DISCONNECT packets. Selecting and connecting to another endpoint is application-managed. |
| Username and password | ✓ | MQTT CONNECT username and password fields are exposed. Authorization policy belongs to the server. |
| Enhanced authentication and reauthentication | ✓ | Multi-step AUTH exchanges are application-driven through dedicated handles and tokens. |
| Detailed response packets | ✓ | CONNACK, PUBACK, SUBACK, UNSUBACK, DISCONNECT, and AUTH are exposed as structured packet values, preserving reason codes and supported properties such as Reason String and User Properties. |
| User Properties | ✓ | CONNECT, CONNACK, PUBLISH, PUBACK, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, DISCONNECT, AUTH and Will all support arbitrary user properties |
| Topic aliases | ✗ | Alias fields are exposed, but the client does not maintain per-connection mappings or support alias-only PUBLISH packets in either direction. Leave `ConnectProperties::topic_alias_maximum` at `0` and `PublishProperties::topic_alias` unset. |
| Protocol validation and constraint enforcement | Partial | Malformed encodings, topic and filter syntax, and several packet-local constraints are validated. Some direction-, cross-field-, and session-dependent rules are not enforced; see [Limitations](limitations.md#protocol-validation). |
| Server-advertised capability enforcement | ✗ | Maximum QoS, Retain Available, and wildcard/shared/subscription-identifier availability are exposed, but client operations do not prevent prohibited PUBLISH or SUBSCRIBE packets. Applications must inspect CONNACK and avoid prohibited operations. |
| Peer-advertised limit enforcement | ✗ | Receive Maximum, Maximum Packet Size, and Topic Alias Maximum are encoded, decoded, and exposed, but their directional limits are not enforced. Applications must enforce outgoing limits and must not rely on the client to reject incoming violations. |

## Client Features

These are library-specific APIs, runtime behaviors, and transport capabilities provided by
`ms-mqtt-client`; they are not defined by the MQTT protocol.

| Feature | Status | Notes |
| --- | --- | --- |
| Independently owned components | ✓ | Outgoing operations, connection lifecycle and I/O, and incoming publications can be owned and driven by separate tasks. |
| Concurrent operation submission | ✓ | `Client` is cloneable, and operations from multiple tasks or threads are serialized onto one MQTT connection. |
| Ownership-enforced connection lifecycle | ✓ | `ConnectHandle`, `Connection`, and the returned `ConnectHandle` represent the connect, run, and reconnect states. |
| Application-driven connection I/O | ✓ | The Tokio-based library starts no background connection task; the application chooses task topology and must drive `Connection::run_until_disconnect`. |
| MQTT over TCP | ✓ | Plain TCP with configurable `TCP_NODELAY`. |
| MQTT over TLS | ✓ | OpenSSL TLS 1.2 or later, with custom CA trust and optional client certificates. |
| MQTT over WebSocket | ✓ | WebSocket (`ws`) and secure WebSocket (`wss`) require the `websockets` Cargo feature. |
| Application-supplied transport streams | ✗ | The public API cannot use a caller-provided Tokio `AsyncRead + AsyncWrite` stream; Unix-domain sockets, QUIC, and other custom transports require library changes. |
| HTTP and HTTPS CONNECT proxies | Partial | Explicitly configured CONNECT tunnels work with every supported transport. Unauthenticated proxies and preemptive Basic authentication (`Proxy-Authorization`) are supported, but `407 Proxy Authentication Required` challenges and authentication-scheme negotiation are not handled. |
| SOCKS5 proxies | ✗ | SOCKS5 proxying, including its separate authentication-method negotiation, is not implemented. |
| Transport establishment timeout | ✓ | Covers TCP, proxy, TLS, and WebSocket establishment separately from the MQTT CONNECT response timeout. |
| In-memory queueing while disconnected | ✓ | PUBLISH, SUBSCRIBE, and UNSUBSCRIBE requests can wait in bounded channels until a connection is driven. PUBLISH queue capacities are configurable, and submission waits when a queue is full. |
| Tiered operation results | ✓ | The API distinguishes client acceptance, operation completion, and the server's MQTT reason code. |
| Application-controlled acknowledgement timing | ✓ | Incoming QoS 1 publications include a control token so the application can delay PUBACK until the desired processing point. Dropping an unused control attempts a successful default acknowledgement. |
| Topic validation and local matching | ✓ | `TopicName` and `TopicFilter` validate MQTT syntax and can match names against filters locally. |
| Automatic reconnect and resubscription | ✗ | Reconnect timing, retry policy, endpoint selection, and resubscription are application responsibilities. |
| Durable offline persistence | ✗ | Queues and session state are in memory and do not survive a process restart. |
| Built-in publication dispatch | ✗ | A single `Receiver` yields all incoming publications; routing them to handlers is an application responsibility. |