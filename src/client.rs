use bytes::Bytes;

use crate::packet::{AuthProperties, ConnAck, ConnectProperties, DisconnectProperties, PubAck, PubRec, Publish, PublishProperties, QoS, SubAck, SubscribeProperties, UnsubAck, UnsubscribeProperties};
use crate::token::{CompletionToken, PubAckToken, PubRecToken, PubRelToken};
use crate::error::ClientError;
use crate::topic::{TopicName, TopicFilter};


/// DISCUSS: What should this module and factory function be called?
/// The three components are the client collectively - so what should the outbound struct (currently called the Client) be?
/// Should it be MqttSender or something? Or are we fine with the duplicate semantic?
/// Alternatively, maybe we break up connect/disconnect/auth into a separate fourth component?

/// Creates the three components needed to run the MQTT client
pub fn new_client(options: ClientOptions) -> (Client, EventLoop, Receiver) {
    unimplemented!()
}

// NOTE: Use a builder pattern
pub struct ClientOptions {
    pub client_id: String,
    pub queue_size: usize,
    // Any other options can be added here, but there really ought not be many.
    // DISCUSS: Are there any other options anyway? Should we just forgo the struct and have a function that takes a queue size?

}

// TODO: I don't like the naming of this as Client.
// MQTTHandle? Sender? 

/// Sends outgoing data.
#[derive(Clone)]
pub struct Client {
}

impl Client {
    /// Sends a CONNECT packet to the broker.
    /// 
    /// Returns a token that can be awaited to receive the CONNACK response packet.
    pub async fn connect(&self, properties: ConnectProperties) -> Result<CompletionToken<ConnAck>, ClientError> {
        unimplemented!()
    }

    /// Sends a DISCONNECT packet to the broker.
    /// 
    /// Returns a token that can be awaited for notification of the completion of the disconnect.
    pub async fn disconnect(&self, properties: DisconnectProperties) -> Result<CompletionToken<()>, ClientError> {
        unimplemented!()
    }

    /// Sends an AUTH packet to the broker with reason code 0x19 (Reauthenticate).
    /// 
    /// Returns a token that can be awaited for notification of the completion of the AUTH.
    /// 
    /// TODO: How to preven this from being used illegally? Only valid to reauthenticate if the original CONNECT
    /// contained an authentication method. Perhaps this should not be a method, and instead some kind of AuthToken.
    pub async fn reauthenticate(&self, properties: AuthProperties) -> Result<CompletionToken<()>, ClientError> {
        unimplemented!()
    }

    /// Sends a PUBLISH packet to the broker at QoS 0.
    /// 
    /// Returns a token that can be awaited for confirmation of the PUBLISH being sent.
    pub async fn publish_qos0(&self, topic_name: TopicName, payload: Bytes, properties: PublishProperties) -> Result<CompletionToken<()>, ClientError> {
        unimplemented!()
    }

    /// Sends a PUBLISH packet to the broker at QoS 1
    /// 
    /// Returns a token that can be awaited to receive the PUBACK response packet.
    pub async fn publish_qos1(&self, topic_name: TopicName, payload: Bytes, properties: PublishProperties) -> Result<CompletionToken<PubAck>, ClientError> {
        unimplemented!()
    }

    /// Sends a PUBLISH packet to the broker at QoS 2
    /// 
    /// Returns a token that can be awaited to receive the PUBREC response packet and optionally a
    /// `PubRelToken` for sending a PUBREL packet if the PUBREC response indicates a success.
    pub async fn publish_qos2(&self, topic_name: TopicName, payload: Bytes, properties: PublishProperties) -> Result<CompletionToken<(PubRec, Option<PubRelToken>)>, ClientError> {
        unimplemented!()
    }

    /// Send a SUBSCRIBE packet to the broker.
    /// 
    /// Returns a token that can be awaited to receive the SUBACK response packet.
    pub async fn subscribe(&self, topic_filter: TopicFilter, qos: QoS, properties: SubscribeProperties) -> Result<CompletionToken<SubAck>, ClientError> {
        unimplemented!()
    }

    /// Send an UNSUBSCRIBE packet to the broker.
    ///
    /// Returns a token that can be awaited to receive the UNSUBACK response packet.
    pub async fn unsubscribe(&self, topic_filter: TopicFilter, properties: UnsubscribeProperties) -> Result<CompletionToken<UnsubAck>, ClientError> {
        unimplemented!()
    }
}


/// DISCUSS: Should this be called Connection instead? I think that's semantically clearer, but, it precludes us
/// from providing Events for certain things that aren't connection related, e.g. outgoing publish, etc, although
/// it's unclear if those things are even valuable.
/// ConnectionEventLoop?
pub struct EventLoop {}

impl EventLoop {
    /// Polls for an event from the event loop.
    /// As long as the event loop is being polled, the MQTT client will continue to run.
    pub async fn poll(&mut self) -> Event {
        unimplemented!()
    }
}

// DISCUSS: How should disconnect be handled? DesiredDisconnect vs UnexpectedDisconnect? Or leave it up to the user to stitch that
// together based on incoming data. Lean towards the latter so we don't project semantics.
// Should it be packet based, e.g. CONNACK to keep it as simple as possible?

pub enum Event {
    Connected,  // NOTE: These enums will need to have values in them where appropriate, e.g. CONNACK
    Disconnected,
    // other stuff
}

pub struct Receiver {}
impl Receiver {
    /// Receive an incoming Publish, and any AckToken that may be associated with it.
    /// 
    /// AckToken will only be present if the Publish has a QoS of 1 or 2.
    /// 
    /// Receiving None indicates that the client has been dropped, and no more messages will be received.
    pub async fn recv(&mut self) -> Option<(Publish, AckHandle)> {
        unimplemented!()
    }
}


// TODO: this has to be clonable
pub enum AckHandle {
    QoS0,
    QoS1(PubAckToken),
    QoS2(PubRecToken),
}
