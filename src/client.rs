// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Structs and types that together provide the MQTT client functionality.

// TODO: Remove when possible.
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(clippy::unused_async)]

mod inflight;

use bytes::Bytes;

use crate::error::ClientError;
use crate::packet::{
    AuthProperties, ConnAck, ConnectProperties, DisconnectProperties, PubAck, PubRec, Publish,
    PublishProperties, QoS, SubAck, SubscribeProperties, UnsubAck, UnsubscribeProperties,
};
use crate::token::{
    CompletionToken, CompletionTransmitter, PubAckToken, PubRecToken, PubRelToken, completion_pair,
};
use crate::topic::{TopicFilter, TopicName};

// TODO: What should this module and factory function be called?
// The three components are the client collectively - so what should the outbound struct (currently called the Client) be?
// Should it be MqttSender or something? Or are we fine with the duplicate semantic?
// Alternatively, maybe we break up connect/disconnect/auth into a separate fourth component?

/// Creates the three components needed to run the MQTT client
#[allow(clippy::needless_pass_by_value)] // TODO: Remove when implemented
pub fn new_client(options: ClientOptions) -> (Client, EventLoop, Receiver) {
    // NOTE: We use size 1 channels for outgoing data to avoid buffering packets that are not yet
    // owned by the internal session state. If this becomes a performance bottleneck, revisit.
    let (o_pub_tx, o_pub_rx) = tokio::sync::mpsc::channel(1);
    let (o_ctrl_tx, o_ctrl_rx) = tokio::sync::mpsc::channel(1);
    // TODO: How should the size of the incoming application message channel be determined?
    // For now, it's arbitrarily set to 100.
    let (i_pub_tx, i_pub_rx) = tokio::sync::mpsc::channel(100);
    let client = Client {
        pub_tx: o_pub_tx,
        ctrl_tx: o_ctrl_tx,
    };
    let event_loop = EventLoop {
        o_pub_rx,
        o_ctrl_rx,
        i_pub_tx,
    };
    let receiver = Receiver { rx: i_pub_rx };
    (client, event_loop, receiver)
}

/// Options for configuring the MQTT client
pub struct ClientOptions {
    /// MQTT Client Identifier
    pub client_id: String,
    /// Maximum size of the outgoing message queue
    pub queue_size: usize,
    // Any other options can be added here, but there really ought not be many.
    // TODO: Use a builder pattern?
    // TODO: How to represent authentication options?
}

// TODO: I don't like the naming of this as Client.
// MQTTHandle? Sender? OperationsInterface? Outgoing?

/// Sends outgoing data.
#[derive(Clone)]
pub struct Client {
    // NOTE: We use different channels for publishes vs. control packets to allow for
    // prioritization of operations by the receiver.
    /// Channel that transmits outgoing PUBLISH requests
    pub_tx: tokio::sync::mpsc::Sender<PublishRequest>,
    /// Channel that transmits all other outgoing control packet requests
    ctrl_tx: tokio::sync::mpsc::Sender<ControlRequest>,
}

impl Client {
    /// Sends a CONNECT packet to the broker.
    ///
    /// Returns a token that can be awaited to receive the CONNACK response packet.
    pub async fn connect(
        &self,
        properties: ConnectProperties,
    ) -> Result<CompletionToken<ConnAck>, ClientError> {
        let (transmitter, token) = completion_pair();
        self.ctrl_tx
            .send(ControlRequest::Connect(transmitter, properties))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }

    /// Sends a DISCONNECT packet to the broker.
    ///
    /// Returns a token that can be awaited for notification of the completion of the disconnect.
    pub async fn disconnect(
        &self,
        properties: DisconnectProperties,
    ) -> Result<CompletionToken<()>, ClientError> {
        let (transmitter, token) = completion_pair();
        self.ctrl_tx
            .send(ControlRequest::Disconnect(transmitter, properties))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }

    /// Sends an AUTH packet to the broker with reason code 0x19 (Reauthenticate).
    ///
    /// Returns a token that can be awaited for notification of the completion of the AUTH.
    pub async fn reauthenticate(
        &self,
        properties: AuthProperties,
    ) -> Result<CompletionToken<()>, ClientError> {
        // TODO: How to preven this from being used illegally? Only valid to reauthenticate if the original CONNECT
        // contained an authentication method. Perhaps this should not be a method, and instead some kind of AuthToken.
        unimplemented!()
    }

    /// Sends a PUBLISH packet to the broker at QoS 0.
    ///
    /// Returns a token that can be awaited for confirmation of the PUBLISH being sent.
    pub async fn publish_qos0(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        properties: PublishProperties,
    ) -> Result<CompletionToken<()>, ClientError> {
        let (transmitter, token) = completion_pair();
        self.pub_tx
            .send(PublishRequest::PublishQoS0(
                transmitter,
                topic_name,
                payload,
                properties,
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }

    /// Sends a PUBLISH packet to the broker at QoS 1
    ///
    /// Returns a token that can be awaited to receive the PUBACK response packet.
    pub async fn publish_qos1(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        properties: PublishProperties,
    ) -> Result<CompletionToken<PubAck>, ClientError> {
        let (transmitter, token) = completion_pair();
        self.pub_tx
            .send(PublishRequest::PublishQoS1(
                transmitter,
                topic_name,
                payload,
                properties,
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }

    /// Sends a PUBLISH packet to the broker at QoS 2
    ///
    /// Returns a token that can be awaited to receive the PUBREC response packet and optionally a
    /// `PubRelToken` for sending a PUBREL packet if the PUBREC response indicates a success.
    pub async fn publish_qos2(
        &self,
        topic_name: TopicName,
        payload: Bytes,
        properties: PublishProperties,
    ) -> Result<CompletionToken<(PubRec, Option<PubRelToken>)>, ClientError> {
        let (transmitter, token) = completion_pair();
        self.pub_tx
            .send(PublishRequest::PublishQoS2(
                transmitter,
                topic_name,
                payload,
                properties,
            ))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }

    /// Send a SUBSCRIBE packet to the broker.
    ///
    /// Returns a token that can be awaited to receive the SUBACK response packet.
    pub async fn subscribe(
        &self,
        topic_filter: TopicFilter,
        qos: QoS,
        properties: SubscribeProperties,
    ) -> Result<CompletionToken<SubAck>, ClientError> {
        let (transmitter, token) = completion_pair();
        self.ctrl_tx
            .send(ControlRequest::Subscribe(transmitter, qos, properties))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }

    /// Send an UNSUBSCRIBE packet to the broker.
    ///
    /// Returns a token that can be awaited to receive the UNSUBACK response packet.
    pub async fn unsubscribe(
        &self,
        topic_filter: TopicFilter,
        properties: UnsubscribeProperties,
    ) -> Result<CompletionToken<UnsubAck>, ClientError> {
        let (transmitter, token) = completion_pair();
        self.ctrl_tx
            .send(ControlRequest::Unsubscribe(transmitter, properties))
            .await
            .map_err(|_| ClientError::DetachedClient)?;
        Ok(token)
    }
}

/// Receives incoming Application Messages as `Publish`es.
pub struct Receiver {
    /// Channel for receiving incoming PUBLISH packets
    rx: tokio::sync::mpsc::Receiver<(Publish, AckHandle)>,
}
impl Receiver {
    /// Receive an incoming `Publish`, and any `AckToken` that may be associated with it.
    ///
    /// `AckToken` will only be present if the Publish has a QoS of 1 or 2.
    ///
    /// Receiving None indicates that the client has been dropped, and no more messages will be received.
    pub async fn recv(&mut self) -> Option<(Publish, AckHandle)> {
        self.rx.recv().await
    }
}

/// Runs the MQTT client event loop, keeping the client operational.
pub struct EventLoop {
    // TODO: Should this be called Connection instead? I think that's semantically clearer, but, it precludes us
    // from providing Events for certain things that aren't connection related, e.g. outgoing publish, etc, although
    // it's unclear if those things are even valuable.
    // Furthermore, it's somewhat confusing if the actual connect method is not on the Connection.
    // ConnectionEventLoop?
    // It'll really come down to what events get provided here I guess.

    // NOTE: We use different channels for publishes vs. control packets to allow for
    // prioritization of operations we want to process here.
    /// Channel for receiving outgoing PUBLISH requests from a `Client`
    o_pub_rx: tokio::sync::mpsc::Receiver<PublishRequest>,
    /// Channel for receiving all other outgoing control packet requests from a `Client`
    o_ctrl_rx: tokio::sync::mpsc::Receiver<ControlRequest>,
    /// Channel for sending incoming PUBLISH packets to the `Receiver`
    i_pub_tx: tokio::sync::mpsc::Sender<(Publish, AckHandle)>,
}

impl EventLoop {
    /// Polls for an event from the event loop.
    /// As long as the event loop is being polled, the MQTT client will continue to run.
    pub async fn poll(&mut self) -> Event {
        unimplemented!()
    }
}

// TODO: How should disconnect be handled? DesiredDisconnect vs UnexpectedDisconnect? Or leave it up to the user to stitch that
// together based on incoming data. Lean towards the latter so we don't project semantics.
// Should it be packet based, e.g. DISCONNECT vs ConnectionLost vs SocketClosed?

pub enum Event {
    Connected, // NOTE: These enums will need to have values in them where appropriate, e.g. CONNACK
    Disconnected,
    // other stuff
}

// TODO: this has to be clonable
pub enum AckHandle {
    QoS0,
    QoS1(PubAckToken),
    QoS2(PubRecToken),
}

/// Request to send a PUBLISH packet.
enum PublishRequest {
    PublishQoS0(
        CompletionTransmitter<()>,
        TopicName,
        Bytes,
        PublishProperties,
    ),
    PublishQoS1(
        CompletionTransmitter<PubAck>,
        TopicName,
        Bytes,
        PublishProperties,
    ),
    PublishQoS2(
        CompletionTransmitter<(PubRec, Option<PubRelToken>)>,
        TopicName,
        Bytes,
        PublishProperties,
    ),
}

/// Request to send a non-PUBLISH control packet.
enum ControlRequest {
    // NOTE: A PUBLISH *is* a control packet, but it is not included here as it has a dedicated
    // channel and enum to allow for prioritization.
    Connect(CompletionTransmitter<ConnAck>, ConnectProperties),
    Disconnect(CompletionTransmitter<()>, DisconnectProperties),
    Reauthenticate(CompletionTransmitter<()>),
    Subscribe(CompletionTransmitter<SubAck>, QoS, SubscribeProperties),
    Unsubscribe(CompletionTransmitter<UnsubAck>, UnsubscribeProperties),
}
