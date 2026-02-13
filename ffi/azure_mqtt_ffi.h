/**
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

void start_client(void);

// ---
//
// TODO: Annotate Option fields as nullable to distinguish from non-nullable String etc.

// Base types

typedef uint16_t KeepAlive;
const KeepAlive KeepAlive_Infinite = 0;

typedef uint16_t PacketIdentifier;

typedef uint8_t QoS;
const QoS QoS_AtMostOnce = 0;
const QoS QoS_AtLeastOnce = 1;
const QoS QoS_ExactlyOnce = 2;

typedef uint16_t SessionExpiryInterval;
const SessionExpiryInterval SessionExpiryInterval_Infinite = 0xFFFFFFFF;

typedef uint8_t PayloadFormatIndicator;
const PayloadFormatIndicator PayloadFormatIndicator_Unspecified = 0;
const PayloadFormatIndicator PayloadFormatIndicator_UTF8 = 1;

typedef uint8_t RetainHandling;
const RetainHandling RetainHandling_Send = 0;
const RetainHandling RetainHandling_SendOnlyIfSubscriptionDoesNotCurrentlyExist = 1;
const RetainHandling RetainHandling_DoNotSend = 2;

struct RetainOptions {
    bool retain_as_published;
    RetainHandling retain_handling;
};

// Will

struct WillProperties {
    uint32_t delay_interval;
    PayloadFormatIndicator payload_format_indicator;
    bool expires;
    uint32_t message_expiry_interval;
    char *content_type;
    char *response_topic;
    uint8_t *correlation_data;
    char **user_properties;
};

struct Will {
    char *topic_name;
    QoS qos;
    bool retain;
    uint8_t *payload;
    struct WillProperties properties;
};

// CONNECT

struct ConnectProperties {
    SessionExpiryInterval session_expiry_interval;
    uint16_t receive_maximum;
    uint32_t maximum_packet_size;
    uint16_t topic_alias_maximum;
    bool request_response_information;
    bool request_problem_information;
    char **user_properties;
};

// CONNACK

typedef uint8_t ConnAckReason;
const ConnAckReason ConnAckReason_Success = 0x00;
const ConnAckReason ConnAckReason_UnspecifiedError = 0x80;
const ConnAckReason ConnAckReason_MalformedPacket = 0x81;
const ConnAckReason ConnAckReason_ProtocolError = 0x82;
const ConnAckReason ConnAckReason_ImplementationSpecificError = 0x83;
const ConnAckReason ConnAckReason_UnsupportedProtocolVersion = 0x84;
const ConnAckReason ConnAckReason_ClientIdentifierNotValid = 0x85;
const ConnAckReason ConnAckReason_BadUserNameOrPassword = 0x86;
const ConnAckReason ConnAckReason_NotAuthorized = 0x87;
const ConnAckReason ConnAckReason_ServerUnavailable = 0x88;
const ConnAckReason ConnAckReason_ServerBusy = 0x89;
const ConnAckReason ConnAckReason_Banned = 0x8A;
const ConnAckReason ConnAckReason_BadAuthenticationMethod = 0x8C;
const ConnAckReason ConnAckReason_TopicNameInvalid = 0x90;
const ConnAckReason ConnAckReason_PacketTooLarge = 0x95;
const ConnAckReason ConnAckReason_QuotaExceeded = 0x97;
const ConnAckReason ConnAckReason_PayloadFormatInvalid = 0x99;
const ConnAckReason ConnAckReason_RetainNotSupported = 0x9A;
const ConnAckReason ConnAckReason_QoSNotSupported = 0x9B;
const ConnAckReason ConnAckReason_UseAnotherServer = 0x9C;
const ConnAckReason ConnAckReason_ServerMoved = 0x9D;
const ConnAckReason ConnAckReason_ConnectionRateExceeded = 0x9F;

struct ConnAckProperties {
    bool expires;
    SessionExpiryInterval session_expiry_interval;
    uint16_t receive_maximum;
    QoS maximum_qos;
    bool retain_available;
    uint32_t maximum_packet_size;
    char *assigned_client_identifier;
    uint16_t topic_alias_maximum;
    char *reason_string;
    char **user_properties;
    bool wildcard_subscription_available;
    bool subscription_identifiers_available;
    bool shared_subscription_available;
    bool has_server_keep_alive;
    KeepAlive server_keep_alive;
    char *response_information;
    char *server_reference;
};

struct ConnAck {
    bool session_present;
    ConnAckReason reaosn;
    struct ConnAckProperties properties;
};

// PUBLISH

struct DeliveryInfo {
    bool dup;
    PacketIdentifier packet_identifier;
};

struct DeliveryQoS {
    QoS qos;
    // Only meaningful for QoS 1 or 2.
    struct DeliveryInfo delivery_info;
};

struct PublishProperties {
    PayloadFormatIndicator payload_format_indicator;
    bool expires;
    uint32_t message_expiry_interval;
    uint16_t topic_alias;
    char *response_topic;
    uint8_t *correlation_data;
    char **user_properties;
    uint32_t *subscription_identifiers;
    char *content_type;
};

struct Publish {
    uint8_t *payload;
    struct DeliveryQoS qos;
    bool retain;
    char *topic_name;
    struct PublishProperties properties;
};

// PUBACK

typedef uint8_t PubAckReason;
const PubAckReason PubAckReason_Success = 0x00;
const PubAckReason PubAckReason_NoMatchingSubscribers = 0x10;
const PubAckReason PubAckReason_UnspecifiedError = 0x80;
const PubAckReason PubAckReason_ImplementationSpecificError = 0x83;
const PubAckReason PubAckReason_NotAuthorized = 0x87;
const PubAckReason PubAckReason_TopicNameInvalid = 0x90;
const PubAckReason PubAckReason_PacketIdentifierInUse = 0x91;
const PubAckReason PubAckReason_QuotaExceeded = 0x97;
const PubAckReason PubAckReason_PayloadFormatInvalid = 0x99;

struct PubAckProperties {
    char *reason_string;
    char **user_properties;
};

struct PubAck {
    PacketIdentifier packet_identifier;
    PubAckReason reason;
    struct PubAckProperties properties;
};

// PUBREC

typedef uint8_t PubRecReason;
const PubRecReason PubRecReason_Success = 0x00;
const PubRecReason PubRecReason_NoMatchingSubscribers = 0x10;
const PubRecReason PubRecReason_UnspecifiedError = 0x80;
const PubRecReason PubRecReason_ImplementationSpecificError = 0x83;
const PubRecReason PubRecReason_NotAuthorized = 0x87;
const PubRecReason PubRecReason_TopicNameInvalid = 0x90;
const PubRecReason PubRecReason_PacketIdentifierInUse = 0x91;
const PubRecReason PubRecReason_QuotaExceeded = 0x97;
const PubRecReason PubRecReason_PayloadFormatInvalid = 0x99;

struct PubRecProperties {
    char *reason_string;
    char **user_properties;
};

struct PubRec {
    PacketIdentifier packet_identifier;
    PubRecReason reason;
    struct PubRecProperties properties;
};

// PUBREL

typedef uint8_t PubRelReason;
const PubRelReason PubRelReason_Success = 0x00;
const PubRelReason PubRelReason_PacketIdentifierNotFound = 0x92;

struct PubRelProperties {
    char *reason_string;
    char **user_properties;
};

struct PubRel {
    PacketIdentifier packet_identifier;
    PubRelReason reason;
    struct PubRelProperties properties;
};

// PUBCOMP

typedef uint8_t PubCompReason;
const PubCompReason PubCompReason_Success = 0x00;
const PubCompReason PubCompReason_PacketIdentifierNotFound = 0x92;

struct PubCompProperties {
    char *reason_string;
    char **user_properties;
};

struct PubComp {
    PacketIdentifier packet_identifier;
    PubCompReason reason;
    struct PubCompProperties properties;
};

// SUBSCRIBE

struct SubscribeProperties {
    uint32_t subscription_identifier;
    char **user_properties;
};

// SUBACK

typedef uint8_t SubAckReason;
const SubAckReason SubAckReason_GrantedQoS0 = 0x00;
const SubAckReason SubAckReason_GrantedQoS1 = 0x01;
const SubAckReason SubAckReason_GrantedQoS2 = 0x02;
const SubAckReason SubAckReason_UnspecifiedError = 0x80;
const SubAckReason SubAckReason_ImplementationSpecificError = 0x83;
const SubAckReason SubAckReason_NotAuthorized = 0x87;
const SubAckReason SubAckReason_TopicFilterInvalid = 0x8F;
const SubAckReason SubAckReason_PacketIdentifierInUse = 0x91;
const SubAckReason SubAckReason_QuotaExceeded = 0x97;
const SubAckReason SubAckReason_SharedSubscriptionsNotSupported = 0x9A;
const SubAckReason SubAckReason_SubscriptionIdentifiersNotSupported = 0xA1;
const SubAckReason SubAckReason_WildcardSubscriptionsNotSupported = 0xA2;

struct SubAckProperties {
    char *reason_string;
    char **user_properties;
};

struct SubAck {
    PacketIdentifier packet_identifier;
    SubAckReason *reasons;
    struct SubAckProperties properties;
};

// UNSUBSCRIBE

struct UnsubscribeProperties {
    char **user_properties;
};

// UNSUBACK

typedef uint8_t UnsubAckReason;
const UnsubAckReason UnsubAckReason_Success = 0x00;
const UnsubAckReason UnsubAckReason_NoSubscriptionExisted = 0x11;
const UnsubAckReason UnsubAckReason_UnspecifiedError = 0x80;
const UnsubAckReason UnsubAckReason_ImplementationSpecificError = 0x83;
const UnsubAckReason UnsubAckReason_NotAuthorized = 0x87;
const UnsubAckReason UnsubAckReason_TopicFilterInvalid = 0x8F;
const UnsubAckReason UnsubAckReason_PacketIdentifierInUse = 0x91;

struct UnsubAckProperties {
    char *reason_string;
    char **user_properties;
};

struct UnsubAck {
    PacketIdentifier packet_identifier;
    UnsubAckReason *reasons;
    struct UnsubAckProperties properties;
};

// DISCONNECT

typedef uint8_t DisconnectReason;
const DisconnectReason DisconnectReason_NormalDisconnection = 0x00;
const DisconnectReason DisconnectReason_DisconnectWithWillMessage = 0x04;
const DisconnectReason DisconnectReason_UnspecifiedError = 0x80;
const DisconnectReason DisconnectReason_MalformedPacket = 0x81;
const DisconnectReason DisconnectReason_ProtocolError = 0x82;
const DisconnectReason DisconnectReason_ImplementationSpecificError = 0x83;
const DisconnectReason DisconnectReason_NotAuthorized = 0x87;
const DisconnectReason DisconnectReason_ServerBusy = 0x89;
const DisconnectReason DisconnectReason_ServerShuttingDown = 0x8B;
const DisconnectReason DisconnectReason_KeepAliveTimeout = 0x8D;
const DisconnectReason DisconnectReason_SessionTakenOver = 0x8E;
const DisconnectReason DisconnectReason_TopicFilterInvalid = 0x8F;
const DisconnectReason DisconnectReason_TopicNameInvalid = 0x90;
const DisconnectReason DisconnectReason_ReceiveMaximumExceeded = 0x93;
const DisconnectReason DisconnectReason_TopicAliasInvalid = 0x94;
const DisconnectReason DisconnectReason_PacketTooLarge = 0x95;
const DisconnectReason DisconnectReason_MessageRateTooHigh = 0x96;
const DisconnectReason DisconnectReason_QuotaExceeded = 0x97;
const DisconnectReason DisconnectReason_AdministrativeAction = 0x98;
const DisconnectReason DisconnectReason_PayloadFormatInvalid = 0x99;
const DisconnectReason DisconnectReason_RetainNotSupported = 0x9A;
const DisconnectReason DisconnectReason_QoSNotSupported = 0x9B;
const DisconnectReason DisconnectReason_UseAnotherServer = 0x9C;
const DisconnectReason DisconnectReason_ServerMoved = 0x9D;
const DisconnectReason DisconnectReason_SharedSubscriptionsNotSupported = 0x9E;
const DisconnectReason DisconnectReason_ConnectionRateExceeded = 0x9F;
const DisconnectReason DisconnectReason_MaximumConnectTime = 0xA0;
const DisconnectReason DisconnectReason_SubscriptionIdentifiersNotSupported = 0xA1;
const DisconnectReason DisconnectReason_WildcardSubscriptionsNotSupported = 0xA2;

struct DisconnectProperties {
    bool has_session_expiry_interval;
    SessionExpiryInterval session_expiry_interval;
    char *reason_string;
    char **user_properties;
    char *server_reference;
};

struct Disconnect {
    DisconnectReason *reasons;
    struct DisconnectProperties properties;
};

// AUTH

typedef uint8_t AuthReason;
const AuthReason AuthReason_Success = 0x00;
const AuthReason AuthReason_ContinueAuthentication = 0x18;
const AuthReason AuthReason_Reauthenticate = 0x19;

struct AuthenticationInfo {
    char *method;
    uint8_t *data;
};

struct AuthProperties {
    char *reason_string;
    char **user_properties;
};

struct Auth {
    AuthReason *reasons;
    bool has_authentication_info;
    struct AuthenticationInfo authentication_info;
    struct AuthProperties properties;
};

// Functions

struct ClientOptions {
    const char *client_id;
    PacketIdentifier max_packet_identifier;
    size_t publish_qos0_queue_size;
    size_t publish_qos1_qos2_queue_size;
};

struct Client;

void client_free(struct Client *client);

struct ConnectHandle;

void connect_handle_free(struct ConnectHandle *connect_handle);

struct Receiver;

void receiver_free(struct Receiver *receiver);

struct NewClient {
    struct Client *client;
    struct ConnectHandle *connect_handle;
    struct Receiver *receiver;
};

struct NewClient new_client(struct ClientOptions options);

struct ConnectionTransportConfigTcp {
    char* hostname;
    // TODO: cffi doesn't like `#include <stdint.h>`, but C would require it. Maybe take int? But even sockaddr_in takes uint16_t...
    uint16_t port;
};

_Bool connect_handle_connect_tcp(struct ConnectHandle *connect_handle, struct ConnectionTransportConfigTcp connection_transport);

void client_publish_qos0(struct Client *client, char *topic, uint8_t *payload, size_t payload_len);
