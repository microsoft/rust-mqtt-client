# Limitations

This document describes known limitations that need more detail than the
[feature support](feature-support.md) matrix. MQTT requirements below refer to the
[OASIS MQTT 5.0 specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).

## Protocol validation

Protocol validation is partially implemented. The client validates topic names and filters,
MQTT UTF-8 strings, malformed wire encodings, and many packet-local constraints. Examples of the
latter include rejecting a zero Subscription Identifier while decoding and rejecting No Local on
a shared subscription while decoding SUBSCRIBE.

Some rules cannot be checked from one encoded property or packet alone. They depend on packet
direction, relationships between fields, the original CONNECT, or locally retained session state.
The following contextual checks are not currently enforced.

### Subscription Identifier on an outgoing PUBLISH

MQTT section 3.3.4, [MQTT-3.3.4-6], says that a client-to-server PUBLISH MUST NOT contain a
Subscription Identifier. A server adds Subscription Identifiers only when forwarding a
publication that matches subscriptions.

[`PublishProperties::subscription_identifiers`](../src/packet.rs) is available on the public
packet type, and the publish APIs pass it to the wire packet without checking packet direction.
The client can therefore send an invalid PUBLISH. A server that detects the Protocol Error must
close the network connection and should first send DISCONNECT with reason code `0x82`.

Applications must leave `subscription_identifiers` empty on outgoing publications.

### No Local on an outgoing shared subscription

MQTT section 3.8.3.1, [MQTT-3.8.3-4], defines `No Local = 1` on a shared subscription as a
Protocol Error.

The SUBSCRIBE decoder rejects this combination, but [`Client::subscribe`](../src/client.rs)
accepts a shared `TopicFilter` with `no_local` set to `true`, and the encoder writes that
combination unchanged. A server that detects it must close the network connection and should
first send DISCONNECT with reason code `0x82`.

Applications must set `no_local` to `false` for shared subscriptions.

### Authentication Method continuity

MQTT section 4.12 requires the Authentication Method in every AUTH packet and successful CONNACK
to match the method supplied in CONNECT. This is specified by [MQTT-4.12.0-5].
[MQTT-4.12.0-6] and [MQTT-4.12.0-7] also prohibit AUTH when CONNECT did not contain an
Authentication Method. Reauthentication has the same continuity requirement in
[MQTT-4.12.1-1].

Initial outgoing AUTH responses retain the configured method. Incoming AUTH packets and a
successful CONNACK are not compared with the CONNECT method, however. During reauthentication,
the continuation token also copies the method supplied by the server, so the client can echo a
changed method instead of rejecting it. These paths are handled in the session state machine and
[`Reauth`](../src/client/token/reauth.rs).

Applications performing enhanced authentication must compare the method in each exchange with
the method selected for CONNECT and stop the connection when it changes or appears unexpectedly.

### Session Present consistency

MQTT section 3.2.2.2 defines several relationships between CONNACK Session Present, CONNECT Clean
Start, and the client's local session state:

- Clean Start `1` requires Session Present `0` ([MQTT-3.2.2-2]).
- Session Present `1` when the client has no local Session State requires the client to close the
  network connection ([MQTT-3.2.2-4]).
- Session Present `0` when the client has local Session State requires the client to discard that
  state before continuing ([MQTT-3.2.2-5]).
- An unsuccessful CONNACK requires Session Present `0` ([MQTT-3.2.2-6]).

The session state machine implements the discard behavior when Session Present is `0`. It accepts
Session Present `1` without checking Clean Start or whether resumable local state exists. The
client can therefore continue a connection for which [MQTT-3.2.2-4] requires it to close.

Applications should reject Session Present `1` after Clean Start or when they know there is no
local session to resume.

### Session Expiry Interval on DISCONNECT

MQTT section 3.14.2.2.2 says that omitting Session Expiry Interval from DISCONNECT leaves the
CONNECT value in force. It also defines two directional and cross-packet constraints: a server
MUST NOT include the property in DISCONNECT ([MQTT-3.14.2-2]), and a client that set Session
Expiry Interval to zero in CONNECT cannot change it to a nonzero value in DISCONNECT.

The public disconnect properties accept any interval, and [`Client::disconnect`](../src/client.rs)
does not compare it with the CONNECT value. The client can therefore make the prohibited
zero-to-nonzero transition. The decoder also accepts the property from a server, although the
session state machine does not apply it.

Applications must omit the DISCONNECT property or keep it at zero when CONNECT used a zero
Session Expiry Interval.