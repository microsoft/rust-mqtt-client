use crate::client::pkid::PkidPool;
use crate::buffer_pool::Shared;
use crate::mqtt_proto::{PacketIdentifier, Disconnect, Connect, Publish, ConnAck, PubAck, SubAck, UnsubAck};
use crate::topic::TopicName;
use crate::token::{ConnectCompletionNotifier, PublishQoS1CompletionNotifier, SubscribeCompletionNotifier, UnsubscribeCompletionNotifier};
use bytes::Bytes;
use std::collections::VecDeque;

// Scenarios:
// - Assign pkid
// - Register inflight operation
// - Complete inflight operation
// - Set connected state
// - Enqueue/Dequeue offline operation
// - Validate packets against session rules
// - track outstanding received messages
// - Ack ordering


// --- OPTION 1 ---
// Data only. No smart stuff.
// - Directly use pkid pool for getting pkids
// - Directly use inflight hashmaps for registering/finishing operations
// - Directly set the connected state
// - Directly use offline queue
// - Direct use ack ordere / unacked messages for ack ordering
//      - maybe some kind of listening task, maybe just comparisons
// - Call a validator function before registering an inflight operation
//
// Pros:
// - Simple
// Cons:
// - Lots of direct access to internal state, easy to mess up and make invalid
// - More boilerplate / repeitive code for the EventLoop (especially around leasing pkids)
// - Harder to unit-test session logic in isolation as it's mixed with I/O code in the EventLoop 

pub struct SessionState1 <S:Shared> {
    pub pkid_pool: PkidPool,
    pub offline_queue: VecDeque<InflightOperation<S>>,
    pub inflight_tracker: InflightTracker<S>,
    pub unacked_messages: VecDeque<PacketIdentifier>,
    pub ack_order: AckOrderer,
    pub connected: bool,
}


// --- OPTION 2 ---
// More of a state machine / data hybrid
// - Register an inflight operation, and pkid will automatically be assigned internally as necessary
// - Complete an inflight operation, and pkid will automatically be released internally as necessary
// - Completing an operation will also trigger the notifier (or should it?)
// - Changing the connection state will automatically clean up / cancel outstanding operations as necessary
// - Offline queue could happen automatically based on state when trying to insert / register an operation
//     - although this has some problematic semantics with a queue not being "inflight"
// - Validation (and resulting cancellation) happens automatically when registering an operation
// - Event Loop caller simply inserts new operations/requests, triggers completions, triggers state changes,
//      and pulls on some kind of "next" stream for the next packet to send on the wire
//
// Pros: 
// Cons:
// - Feels like a bit of a confused middleground, there are strange separation of responsibilities that emerge,
//    e.g. who is responsible for triggering a completion?
// - offline queue semantics are a bit strange - how do you get/fetch from it? the session state is aware of connection,
//   but if you manaully add to the queue, you can do it at times that don't make sense (e.g. while connected)

pub struct SessionState2 <S:Shared> {
    pkid_pool: PkidPool,
    offline_queue: VecDeque<InflightOperation<S>>,
    inflight_tracker: InflightTracker<S>,
    ack_order: AckOrderer,
    connected: bool,
}

impl <S:Shared> SessionState2 <S> {
    pub fn register_inflight_connect(&mut self, completion: ConnectCompletionNotifier) -> Connect<S>{
        unimplemented!()
    }

    pub fn register_inflight_publish_qos1(&mut self, topic: TopicName, payload: Bytes, completion: PublishQoS1CompletionNotifier) -> Publish<S>{
        unimplemented!()
    }

    pub fn complete_inflight(&mut self, operation: CompletedOperation<S>) {
        unimplemented!()
    }

}


// --- OPTION 3 ---
// Conceptually similar to the above, but more magic - the insertion is unnecessary as the Session
// receives packets directly from the clients. Basically, a big filter on all incoming data before it
// gets to the event loop. Perhaps is better described as a "Session Manager"
// - Registering inflight operations happens automatically based on requests,
// - Completing an operation will trigger the notifier
// - Changing the connection state will automatically clean up / cancel outstanding operations as necessary
// - Offline queue is automatic based on state
// - Validation is automatic when registering an operation.
// - Event Loop caller simply pulls on some kind of "next" stream for the next packet to send on the wire
// - Incoming publishes are automatically registered for acking before being dispatched to the Receiver
// Pros:
// - Fully encapsulates all session logic
// - Removes basically all boilerplate from the Event Loop
// - Easy to unit-test session logic in isolation as it's not mixed with I/O code in
// - Symmetry with I/O
// Cons:
// - resetting the session needs to either happen in place, or transfer the client receivers into a new struct
// - may be timing issues with handling the channels internally (although maybe this suggests we should just use Arc'd ref to state?)
// - too much magic?

// TODO: ping logic in here too
// TODO: offline queue is the Request enum i.e. not the real packet yet b/c pkid not yet assigned
pub struct SessionManager <S:Shared> {
    client_rx: tokio::sync::mpsc::Receiver<Request>,
    pkid_pool: PkidPool,
    offline_queue: VecDeque<InflightOperation<S>>,  // not actually related to conn state - could be used if out of pkids or inflight queue filled
    inflight_tracker: InflightTracker<S>,
    ack_order: AckOrderer,
    connected: bool,
}

impl <S:Shared> SessionManager <S> {
    pub async fn next_outgoing_packet(&mut self) -> Option<InflightOperation<S>> {
        unimplemented!()
    }

    pub fn incoming_publish(&mut self, publish: Publish<S>) {
        unimplemented!()
    }

    pub fn transition_connected(&mut self, connack: ConnAck<S>) {
        unimplemented!()
    }

    pub fn transition_disconnected(&mut self, disconnect: Disconnect<S>) {
        unimplemented!()
    }

    pub fn complete_inflight(&mut self, operation: CompletedOperation<S>) {
        unimplemented!()
    }
}


pub enum Packet<S:Shared> {
    Connect(Connect<S>),
    Publish(Publish<S>),
}

pub enum InflightOperation <S>
where S: Shared
{
    Connect(ConnectCompletionNotifier),
    Subscribe(PacketIdentifier, SubscribeCompletionNotifier),
    Unsubscribe(PacketIdentifier, UnsubscribeCompletionNotifier),
    PublishQoS1(Publish<S>, PublishQoS1CompletionNotifier),
}

pub enum CompletedOperation <S>
where S: Shared
{
    Connect(ConnAck<S>),
    PublishQoS1(PubAck<S>),
    Subscribe(SubAck<S>),
    Unsubscribe(UnsubAck<S>),
    // TODO: QoS 2 publish, pubrec, pubrel
}

// Stubs
pub struct InflightTracker<S:Shared> {
    _marker: std::marker::PhantomData<S>,
}


pub enum Request {
    Connect(ConnectCompletionNotifier),
    Publish(PublishQoS1CompletionNotifier)
    // and so on...
}
pub struct AckOrderer {}