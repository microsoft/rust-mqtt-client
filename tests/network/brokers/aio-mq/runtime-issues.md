# AIO MQ 1.6.0 runtime instability during transport tests

## Summary

After the AIO MQ 1.6.0 broker is successfully deployed with MQTT, MQTT/TLS,
WebSocket, and WSS listeners, two runtime behaviors make transport-level client
testing unreliable:

1. Deliberately failed TLS handshakes can leave secure endpoints or their
   `kubectl port-forward` process unusable for later valid connections.
2. Running all transport I/O profiles concurrently against the one-worker
   `Tiny` fixture intermittently drops otherwise valid connections. The same
   tests pass when transport tests are serialized.

These issues are distinct from the deployment/reconciliation problems described
in `deployment-issue.md`. That document concerns reaching a correctly configured
running frontend. This document concerns behavior after that frontend is ready.

## Environment

- Chart: `oci://mqbuilds.azurecr.io/helm/aio-broker:1.6.0`
- Broker runtime reported: `Build 1.6.0 (20260803-2-release-mq 23e7f20f9c7036d1bfd7fba012fb65bd6f62c8d9)`
- Host: WSL2, Linux `6.18.33.2-microsoft-standard-WSL2`, x86_64
- k3d: `v5.9.0`
- k3s: `v1.35.5-k3s1`
- kubectl client: `v1.33.1`
- Helm: `v3.21.3`
- Frontend: one replica, one worker
- Backend: one chain, one partition, one worker
- Memory profile: `Tiny`
- Client authentication/authorization: disabled except TLS client-certificate
  tests where noted
- Host exposure: one `kubectl port-forward` process per listener

## Expected behavior

A failed client connection should affect only that connection. In particular:

- Rejecting an untrusted server certificate or hostname mismatch should not
  prevent a later valid TLS client from connecting.
- Multiple independent clients using TCP, TLS, WS, and WSS concurrently should
  not cause unrelated connections to be dropped.

## Issue 1: failed TLS handshakes affect later secure connections

### Reproduction

With the broker frontend ready and MQTT/TLS listening on port 8883:

1. Connect a TLS client that does not trust the server CA, or connect using a
   hostname that does not match the server certificate.
2. Confirm that the client rejects the handshake, as expected.
3. Connect a correctly configured client using the trusted CA and matching
   hostname.
4. Exchange MQTT traffic.

### Observed behavior

The initial failure is returned as an I/O/TLS error, as expected. However, later
valid MQTT/TLS or WSS tests intermittently fail with `unexpected EOF`.

When the endpoint is exposed using `kubectl port-forward`, the forwarding
process can terminate after the server resets the failed secure connection:

```text
error forwarding port 8883 to pod ...:
writeto tcp4 127.0.0.1:<port>->127.0.0.1:8883:
read: connection reset by peer
error: lost connection to pod
```

A forwarding process that carries several ports exits entirely when one secure
stream is reset. Running one port-forward process per listener isolates the
other transports, but the affected secure endpoint still cannot be relied on for
subsequent positive tests in the same fixture run.

### Current workaround

The network suite does not run deliberate TLS-failure cases against the AIO MQ
fixture. Positive MQTT/TLS and WSS data-path tests still run.

In code this is represented as
`FixtureQuirk::FailedTlsHandshakeDestabilizesServer`: Mosquitto, EMQX, and HiveMQ
run these tests; AIO MQ skips them.

### Evidence limitations

The observed forwarding-process exit is definitely triggered by a connection
reset from inside the pod network namespace. We have not yet isolated whether
later connection failures originate in:

- AIO MQ frontend TLS state,
- the Kubernetes port-forward implementation,
- an interaction between the two, or
- certificate/connection teardown behavior in this specific test environment.

A direct in-cluster reproducer that avoids `kubectl port-forward` would help
separate these causes.

## Issue 2: concurrent transport tests intermittently drop connections

### Reproduction

Run positive client I/O tests concurrently over all four listeners:

- TCP/1883
- MQTT over TLS/8883
- WebSocket/8083
- WSS/8084

Each profile opens clients, subscribes, publishes QoS 1 traffic, receives the
routed publication, acknowledges it, and disconnects cleanly. Some tests also
hold connections idle across keepalive traffic or send sustained bursts.

### Observed behavior

Under normal libtest parallel execution, one or more profiles intermittently
fail with errors such as:

```text
DetachedError
unexpected EOF
Connection refused (os error 111)
```

The failures were not consistently tied to one transport. In different runs,
TCP, TLS, WS, or WSS could fail while other profiles passed.

Running the same transport tests serially against the same deployed fixture
made the failures disappear. The full positive TCP/TLS/WS/WSS suite then passed.

### Current workaround

Only transport-related tests are serialized when `MQTT_SERVER=aio-mq`. Other
server fixtures continue to run those tests in parallel.

In code this is represented as
`FixtureQuirk::RequiresSerialTransportTests`.

The fixture currently uses:

```text
frontend.replicas = 1
frontend.workers = 1
memoryProfile = Tiny
```

### Evidence limitations

We have not proven that this is an AIO MQ concurrency defect. Plausible causes
include:

- the one-worker `Tiny` frontend configuration,
- simultaneous TLS and WebSocket pressure,
- health-probe traffic,
- Kubernetes port-forward behavior, or
- a broker runtime defect.

Increasing frontend workers/replicas and rerunning without serialization would
be the first useful isolation step.

## Repository workarounds

The test fixture currently encodes these mitigations:

- Skip intentional TLS-failure tests when
   `FixtureQuirk::FailedTlsHandshakeDestabilizesServer` is present.
- Serialize transport tests when `FixtureQuirk::RequiresSerialTransportTests`
   is present.
- Run one `kubectl port-forward` process per listener so a reset on one port does
  not terminate forwarding for all transports.

These workarounds are separate from the staged deployment workaround documented
in `deployment-issue.md`.

By contrast, `FixtureCapability::MutualTls` describes a positively provisioned
test endpoint rather than a workaround or defect.

## Suggested investigation

1. Run the TLS failure sequence from a pod inside the cluster, connecting
   directly to the service, to remove port-forward from the path.
2. Check frontend logs and process health immediately after each failed TLS
   handshake.
3. Repeat concurrent tests with more frontend workers and/or replicas.
4. Disable health probes temporarily to determine whether probe traffic affects
   the result.
5. Run each transport concurrently through direct in-cluster connections rather
   than host forwards.
6. Compare behavior with a newer broker build when one becomes available.

## Questions for maintainers

1. Should a failed TLS handshake ever reset or restart the frontend listener in
   AIO MQ 1.6.0?
2. Is the frontend expected to tolerate immediate valid connections following a
   certificate or hostname-validation failure?
3. Are there known limitations for concurrent TCP/TLS/WS/WSS clients with one
   frontend worker and the `Tiny` memory profile?
4. What frontend worker/replica configuration is recommended for transport
   interoperability testing?
5. Which logs or metrics best distinguish TLS-listener failure from capacity or
   health-probe pressure?
