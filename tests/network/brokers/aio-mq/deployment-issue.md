# AIO MQ 1.6.0 multi-transport deployment and reconciliation failures

## Summary

A clean AIO MQ 1.6.0 deployment does not reliably reach a usable state when one
`BrokerListener` initially defines MQTT, MQTT/TLS, WebSocket, and WSS ports.

We found a reliable but complex workaround:

1. Create the Broker with plaintext MQTT only.
2. Wait for the Broker to reach `Running`.
3. Add MQTT/TLS and plaintext WebSocket.
4. Wait separately for the generated TLS endpoint configuration and the TLS Secret volume on the frontend StatefulSet.
5. Add WSS.
6. Delete the frontend pod so it reloads the generated endpoint configuration.

This appears to expose at least two operator reconciliation problems. We can split
them into separate issues if preferred.

## Environment

- Chart: `oci://mqbuilds.azurecr.io/helm/aio-broker:1.6.0`
- Chart/app version: `1.6.0`
- Broker runtime reported: `Build 1.6.0 (20260803-2-release-mq 23e7f20f9c7036d1bfd7fba012fb65bd6f62c8d9)`
- Host: WSL2, Linux `6.18.33.2-microsoft-standard-WSL2`, x86_64
- k3d: `v5.9.0`
- k3s: `v1.35.5-k3s1`
- kubectl client: `v1.33.1`
- Helm: `v3.21.3`
- Broker cardinality: one frontend replica/worker and one backend chain with one partition/worker
- Memory profile: `Tiny`
- Internal traffic encryption: disabled
- Client authentication/authorization: disabled

The standalone `aio-broker` OCI repository was enumerated on 2026-08-07. Version
`1.6.0` was the highest stable semantic-version tag among 5,009 tags.

## Expected behavior

Creating the TLS Secret followed by one `BrokerListener` containing all supported
ports and one `Broker` should converge to a `Running` broker with a ready frontend:

- 1883: MQTT
- 8883: MQTT over TLS
- 8083: MQTT over WebSocket
- 8084: MQTT over secure WebSocket

The operator should mount the referenced TLS Secret, generate the endpoint
configuration, and roll the frontend when that configuration changes.

## Actual behavior 1: initial multi-transport deployment never reaches Running

On repeated clean k3d clusters, applying all four ports before the Broker reached
`Running` left the Broker in `Starting` for at least five minutes.

Observed status:

```text
runtimeStatus:
  status: Starting
  description: Waiting for all replicas to report ready
```

The backend pod remained `0/1` with this startup-probe failure:

```text
startup-probe: store not ready for worker 0
```

No ready frontend was available. Reducing the existing listener back to TCP-only
did not recover that deployment.

Creating a new cluster with the same chart and the same Broker settings, but with
only plaintext port 1883 initially, reached `Running` in roughly 30-60 seconds.

## Minimal initial-deployment reproduction

Install the chart and CRDs, then create a standard Kubernetes TLS Secret:

```bash
kubectl create secret tls network-server-tls \
  --cert=server.crt \
  --key=server.key
```

Apply this listener before or together with the Broker:

```yaml
apiVersion: mqttbroker.iotoperations.azure.com/v1
kind: BrokerListener
metadata:
  name: listener
  namespace: default
spec:
  brokerRef: broker
  serviceName: aio-broker
  serviceType: LoadBalancer
  ports:
    - port: 1883
      protocol: Mqtt
    - port: 8883
      protocol: Mqtt
      tls:
        mode: Manual
        manual:
          secretRef: network-server-tls
    - port: 8083
      protocol: WebSockets
    - port: 8084
      protocol: WebSockets
      tls:
        mode: Manual
        manual:
          secretRef: network-server-tls
---
apiVersion: mqttbroker.iotoperations.azure.com/v1
kind: Broker
metadata:
  name: broker
  namespace: default
spec:
  advanced:
    encryptInternalTraffic: Disabled
  cardinality:
    frontend:
      replicas: 1
      workers: 1
    backendChain:
      redundancyFactor: 1
      partitions: 1
      workers: 1
  generateResourceLimits:
    cpu: Disabled
  memoryProfile: Tiny
```

Then observe:

```bash
kubectl get broker broker -w
kubectl get pods -w
kubectl describe pod aio-broker-backend-1-0
```

## Actual behavior 2: listener updates can produce config without a TLS mount

After bootstrapping TCP-only and reaching `Running`, updating the listener with
TLS, WS, and WSS could produce the expected endpoint ConfigMap entries while the
frontend StatefulSet still had no volume or mount for `network-server-tls`.

Generated endpoint configuration included:

```toml
[[endpoints]]
addresses = ["0.0.0.0:8883", "[::]:8883"]
protocol = "Mqtt"

[endpoints.tls]
source = "K8sMounted"
certDir = "/server/network-server-tls"
```

However, the frontend pod had no `/server/network-server-tls` mount. It logged
that TLS was enabled and accepted TCP connections, but reset TLS handshakes with
an unexpected EOF.

The reliable sequence was:

1. Update the listener with 1883, 8883, and 8083 only.
2. Wait until the frontend StatefulSet references Secret `network-server-tls`.
3. Update the listener again to add 8084/WSS.
4. Delete `aio-broker-frontend-0` and wait for the StatefulSet rollout.

After that sequence, startup logs listed all four endpoints and MQTT client tests
passed over TCP, TLS, WS, and WSS.

## Actual behavior 3: listener configuration changes do not reliably restart the frontend

The operator updated `aio-broker-frontendbroker-config`, but an existing frontend
continued running with its old listener set. For example, the Service and ConfigMap
contained port 8084 while the running frontend did not bind 8084.

Deleting the frontend pod caused the replacement pod to load the configuration
and bind all four ports:

```text
frontend worker 0 will listen on 0.0.0.0:1883, TLS enabled: false
frontend worker 0 will listen on 0.0.0.0:8883, TLS enabled: true
frontend worker 0 will listen on 0.0.0.0:8083, TLS enabled: false
frontend worker 0 will listen on 0.0.0.0:8084, TLS enabled: true
```

## Repository workaround

The test fixture currently encodes the staged sequence in:

- `broker.yaml`: TCP-only bootstrap
- `broker-transports.yaml`: add MQTT/TLS and WS
- `broker-wss.yaml`: add WSS after the TLS Secret mount exists
- `up.sh`: wait for each reconciliation stage and restart the frontend

The workaround should be removable when a single declarative listener:

1. reaches `Running` from a clean deployment,
2. creates the TLS Secret mount on the frontend StatefulSet,
3. rolls the frontend when endpoint configuration changes, and
4. accepts MQTT clients on all four transports without manual pod deletion.

## Separate k3d observation

Direct k3d host-port publishing worked for plaintext traffic but produced EOFs
for TLS traffic in this local setup. Kubernetes `kubectl port-forward` preserved
TLS correctly, so the fixture uses one port-forward process per listener.

This may be a k3d/networking issue rather than an AIO MQ issue and is included
only to describe the complete reproduction environment.

## Questions for maintainers

1. Is configuring TLS/WSS on the initial `BrokerListener` before the Broker is
   `Running` expected to work in chart 1.6.0?
2. Should a listener update that references a TLS Secret update the frontend
   StatefulSet volume and trigger a rollout automatically?
3. Is sharing one Kubernetes TLS Secret between MQTT/TLS and WSS supported?
4. Are there operator/controller logs or status conditions that should surface
   the missing Secret mount or stale frontend configuration?
5. Are any of these reconciliation issues already fixed in an unpublished or
   preview broker chart?
