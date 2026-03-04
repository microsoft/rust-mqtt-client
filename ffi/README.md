# libazure_mqtt_ffi

This is high-level documentation of the FFI library implementation of the Azure MQTT client, its design rationale and future plan.


## What's FFI?

"Foreign Function Interface" is the technique of calling functions written in another programming language from your program. The caller needs to understand the callee's types, calling convention, etc. The easiest and most common way to do this is to use the C ABI, namely the callee exposes an ABI that is identical to the C ABI for that platform, and the caller calls the function as if it's a C function.


## Why FFI?

We want customers using C, Python, Golang, etc to be able to use a Microsoft-maintained MQTT client. At the same time we prefer having one implementation that is known to be correct (follows the MQTT spec) and high-performance (optimized for network IO latency and throughput, memory copies, etc).

While native libraries for each programming language we want to support are usually easier to use, we believe the tradeoff is worth it.

The other alternative is to use language-specific integrations, eg writing a CPython library for Python that exposes Rust objects as Python objects and vice versa. However this is not necessarily possible for every language, and requires a lot more language-specific work than just writing thunks (as described in the next section), so we do not want to do this approach.

## How do we do FFI?

Every language we care about has FFI compatibility via a C ABI, so that is what we do here.

First we wrap the Rust API in a C API and compile it to a shared object. This is the header `azure_mqtt_ffi.h` and the `azure_mqtt_ffi` crate in this directory.

Next we write a language-specific package that does C FFI to the shared object, using the header if necessary. Eg for Python this is the `python/azure_mqtt.py` file.

This means that there end up being multiple reimplementations of the same types and functions:

1. The original Rust client version, eg `fn azure_mqtt::client::new_client(options: ClientOptions) -> (Client, ConnectHandle, Receiver) { ... }` / `struct azure_mqtt::client::ClientOptions { .. }`

2. The FFI library version, eg `fn azure_mqtt_ffi::new_client(options: ClientOptions) -> NewClient { ... }` / `struct azure_mqtt_ffi::ClientOptions { ... }`

3. The C header version, eg `struct NewClient new_client(struct ClientOptions options);` / `struct ClientOptions { ... };`.

4. The language wrapper version, eg `def new_client(client_options: ClientOptions)` / `class ClientOptions: ...`

While all these reimplementations seem counter-productive to the original goal of having only one implementation, it is worth noting that these reimplementations are just thunks to convert values from one system to another (eg Python strings <-> C strings <-> Rust strings), and the "business logic" of the client as a whole has a single implementation in the Rust client code.

Because the Rust implementation is entirely async and requires running in a tokio runtime, the idea is that every call to create a client spawns a background thread that will run a tokio runtime and drive that one specific client. This means languages do not need to be concerned with handling Rust async semantics themselves, and Rust doesn't need to be concerned with interoperating with the language's ideas of threads.

An example of the latter is that Python will not want the client library to block until it receives an incoming packet because it won't be able to run the user's Python code meanwhile. Only when the user code wants to block and wait for an incoming packet should it do so. But the Rust client will need to run (sending PINGREQs and PINGRESPs) even if the Python code has not yielded to it. This is not a problem when the Rust code is running in its own dedicated background thread.

This background thread approach has its own disadvantages but we believe the tradeoff is worth it:

- A large number of clients leads to a large number of background threads. However we do not expect there to be a large number of clients.

- Communicating with a background thread requires the overhead of message passing using channels. However this overhead is already there because even with the Rust client library there are channels used to communicate between the `Client` / `Receiver` and the `Session`. So we use the same channels for this cross-thread communication also.


## Future plans

- Eventually the `azure_mqtt_ffi.py` script should be published as a Python package.

- It is an open question of how the FFI library `libazure_mqtt_ffi.so` should be shipped. Taking the specific case of the Python package, the ideal case would be to ship the Rust source code inside the Python wheel and then wire it up to be compiled at `pip install` time, but the downside of this is that it requires the user to have C compiler AND Rust compiler AND devel packages of openssl etc.

  The alternative is to ship the precompiled library, but the downside of this is that the precompiled library will be specific to one libc and one openssl and so on. We would have to ship multiple packages for each combination of libc and openssl etc that we care about, or at least pick combinations that represent the distros we care to support.

  For the short term the plan is to do the latter, and limit ourselves to just whatever works on Azure Linux or Ubuntu, ie the combination of their glibc version + openssl version.

- The C header is currently hand-written. It could be auto-generated from the Rust source using a tool like `cbindgen`. However this might create problems for some languages, eg Python's `cffi` library supports only a subset of C that the current hand-written header caters to (no unions, no `#include`s, etc), so it might not be feasible.

- Some functions take `Foo *` and do not consume it, so the FFI caller is allowed to use the `Foo` for future API calls. Other functions take `Foo *` and consume it, so the FFI caller must not reuse the `Foo` or release it. FFI values are opaque so the C interface has to put them behind pointers, which then creates this ambiguity in the function signature. There doesn't seem to be a way around this other than documenting the behavior for each function in the C header and then being careful to follow it in every language wrapper.

  Related to that point, languages other than Rust generally cannot express that a value is consumed (move semantics) in a way that prevents user code from trying to reuse the value after it's been consumed. The best they can do is mark the value as invalid and check for validity on every function call. It is not clear if there's a better way on the language side.

- The C API is a least-denominator API in that every operation is blocking or has a timeout. It would be nice to integrate with language-specific features for async operations, eg asyncio in Python. However every language does async in its own way (callbacks vs events vs blocking on another thread) so it is hard to find a common ground that `libazure_mqtt_ffi` could expose.

- The wrapped API is currently around the "simple" client API that deals with Rust `String`s and `Byte`s. Therefore the C API's `char *` and `uint8_t *` need to be memcpy'd into the Rust versions so that they can then be given to the client API. The client API then does another copy for `String` -> `ByteStr<Bytes>`. It would be nice to avoid this by using the "complex" client API that deals with explicit `BufferPool` impls, and then use an FFI-specific `BufferPool` impl that can use `char *` and `uint8_t *` as `Shared`s directly.

  This is not necessarily feasible though, because it depends on the language making it possible to "leak" the `char *` at the time the FFI function is called, and then knowing that it can be released only at some later time when the Rust side is done with it. Thus the FFI BufferPool would need a way to signal to the language side when the Rust side is done with a buffer permanently.

  Also the "complex" API doesn't exist yet. But it is likely to be a copy-paste of the "simple" API for the most part. In fact some of the "simple" API is a wrapper around the "complex" API already, just the "complex" API isn't publicly exported.

  An alternative is to expose `BufferPool::take_owned` in the FFI interface, so that the language side can allocate a buffer from the Rust side, write its string / payload into it, and then hand it off to the FFI API. This way the Rust side would own the buffer and thus it would be automatically be released when it's used up. This approach can work with the default `BufferPool` so it wouldn't require an FFI-specific `BufferPool`.

- Currently the `Client` / `Receiver` communication uses tokio channels, which means functions like `client_publish_qos0` need to spin up a singe-use tokio runtime to send the request over the channel. It could be useful to use unbounded channels here since the unbounded sender does not require a tokio runtime. It comes with the obvious disadvantage of being unbounded and thus not applying backpressure, but it is possible to implement backpressure manually by counting the number of elements in the channel queue (MQ does this for example) so it's not a concern.
