# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import time

import azure_mqtt

def main():
    [client, connect_handle, receiver] = azure_mqtt.new_client(azure_mqtt.ClientOptions())
    with client, connect_handle, receiver:
        print(client, connect_handle, receiver)

        connect_handle.connect_tcp(azure_mqtt.ConnectionTransportConfigTcp("localhost", 1883))

        # client.subscribe("test/topic", 1)
        client.publish_qos0("foo", b"asdf")

    while True:
        print("hello from python")
        time.sleep(1)

if __name__ == "__main__":
    main()
