# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import abc
import cffi
import contextlib
import dataclasses
import typing

PacketIdentifier = int
PACKET_IDENTIFIER_MAX = 65535

@dataclasses.dataclass(init=True, repr=True, frozen=True)
class ClientOptions:
    client_id: str | None = None
    max_packet_identifier: PacketIdentifier = 65535
    publish_qos0_queue_size: int = 100
    publish_qos1_qos2_queue_size: int = 100

@dataclasses.dataclass(init=True, repr=True, frozen=True)
class ConnectionTransportConfigTcp:
    hostname: str
    port: int

class Client(contextlib.AbstractContextManager):
    def __init__(self, c):
        self._c = c

    def __exit__(self, _exc_type: typing.Any, _exc_value: typing.Any, _traceback: typing.Any):
        if self._c != ffi.NULL:
            lib.client_free(self._c)

    def subscribe(self, topic_filter: str, qos: int):
        topic_filter_c = ffi.new("char[]", topic_filter.encode())
        return
        lib.client_subscribe(topic_filter_c, qos)

    def publish_qos0(self, topic: str, payload: bytes):
        topic_c = ffi.new("char[]", topic.encode())
        payload_c = ffi.new("uint8_t[]", payload)
        lib.client_publish_qos0(self._c, topic_c, payload_c, len(payload))

class ConnectHandle(contextlib.AbstractContextManager):
    def __init__(self, c):
        self._c = c

    def __exit__(self, _exc_type: typing.Any, _exc_value: typing.Any, _traceback: typing.Any):
        if self._c != ffi.NULL:
            lib.connect_handle_free(self._c)

    def connect_tcp(self, connection_transport: ConnectionTransportConfigTcp):
        connection_transport_c = ffi.new("struct ConnectionTransportConfigTcp *")

        hostname_c = ffi.new("char[]", connection_transport.hostname.encode())
        connection_transport_c.hostname = hostname_c

        connection_transport_c.port = connection_transport.port

        c = self._c
        self._c = ffi.NULL

        lib.connect_handle_connect_tcp(c, connection_transport_c[0])

class Receiver(contextlib.AbstractContextManager):
    def __init__(self, c):
        self._c = c

    def __exit__(self, _exc_type: typing.Any, _exc_value: typing.Any, _traceback: typing.Any):
        if self._c != ffi.NULL:
            lib.receiver_free(self._c)

def new_client(options: ClientOptions) -> tuple[Client, ConnectHandle, Receiver] :
    options_c = ffi.new("struct ClientOptions *")

    if options.client_id is None:
        options_c.client_id = ffi.NULL
    else:
        client_id_c = ffi.new("char[]", options.client_id.encode())
        options_c.client_id = client_id_c

    options_c.max_packet_identifier = options.max_packet_identifier
    options_c.publish_qos0_queue_size = options.publish_qos0_queue_size
    options_c.publish_qos1_qos2_queue_size = options.publish_qos1_qos2_queue_size

    new_client = lib.new_client(options_c[0])
    client = Client(new_client.client)
    connect_handle = ConnectHandle(new_client.connect_handle)
    receiver = Receiver(new_client.receiver)

    return [client, connect_handle, receiver]

ffi = cffi.FFI()
with open("../azure_mqtt_ffi.h") as f:
    header = f.read()
    ffi.cdef(header)
lib = ffi.dlopen("../../target/debug/libazure_mqtt_ffi.so")
