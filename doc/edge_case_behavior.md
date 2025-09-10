
| Packet        | Not Connected | Connection loss in channel  | Conneciton loss in message queue | Connection loss in ordering queue | Connection Loss in flight |
| ------------- | ------------- | --------------------------- | --------------------------------| ---- | ---
| QoS 0 PUBLISH | Add to session message queue | Add to session message queue | Stays in session message queue | N/A | N/A (Already Complete)
| QoS 1 PUBLISH | Add to session message queue | Add to session message queue | Stays in session message queue | N/A | Redelivered on reconnect
| QoS 2 PUBLISH | Add to session message queue | Add to session message queue | Stays in session message queue | N/A | Redelivered on reconnect
| PUBACK        | Issue CompletionError | Issue CompletionError | N/A | Issue CompletionError | Considered complete
| PUBREC        | Add to session ordering queue | Add to session ordering queue | N/A | Stays in session ordering queue | Wait for reconnect and either:<br>- Redeliver upon receiving redelivered PUBLISH<br>- Complete upon receiving PUBREL 
| PUBREL        | Add to session ordering queue | Add to session ordering queue | N/A | Stays in session ordering queue | Redelivered on reconnect
| PUBCOMP       | Enter session, wait for reconnect and<br>deliver upon PUBREL redelivery | Enter session, wait for reconnect and<br>deliver upon PUBREL redelivery | N/A | N/A | Considered complete
| SUBSCRIBE     | Add to session message queue | Add to session message queue | Stays in session message queue | N/A | Issue CompletionError
| UNSUBSCRIBE   | Add to session message queue | Add to session message queue | Stays in session message queue | N/A | Issue CompletionError
| CONNECT       | ? | N/A | N/A | N/A | N/A
| DISCONNECT    | ? |



### Clarifications
* All packets issue CompletionError if the Session ends, this table assumes the Session does not end
* CONNACK, SUBACK, UNSUBACK are not in this table, because they are never explicitly issued by the application
* CompletionToken can be provided as soon as the request goes into the channel

### Questions
- What happens if you connect while already connected? Where does that fail?
- What happens if you disconnect while already disconnected? Does it matter what state you're in?
- How to handle combo order queueing of PUBACK/PUBREC/PUBREL?

### Implementation Considerations
- Can the channel function as the session message queue?
    - This would require multiple channels and might be a little weird in terms of PKID assignment, but potentially more efficient
    - Would need a distinct channels for:
        - QoS 0 PUBLISH (no PKID, not subject to broker receive maximum)
        - QoS 1 PUBLISH + QoS 2 PUBLISH + SUBSCRIBE + UNSUBSCRIBE (shared PKID ordering, subject to broker receive maximum)
        - PUBACK + PUBREC + PUBREL + PUBCOMP (need immediately for PKID tracking, ordering [if applicable] is non-linear)
        - Unclear how CONNECT/DISCONNECT would be handled (maybe the PUBACK/PUBREC/PUBREL/PUBCOMP immediate channel)
    - If not, channel should probably be size 1 to minimize strange behavior and size, since the real queues are inside the EventLoop
    - The way the EventLoop pulls on the various channels would need to be biased to prevent starvation
- Session message queue size
    - Can it be configured to be smaller than the broker receive maximum?
        - Should be allowed - receive maximum only governs the number of in-flight messages
    - Can it be configured to be larger than the broker receive maximum?
        - Should be allowed - receive maximum only governs the number of in-flight messages
    - What if the message queue is full? What is the expected behavior?
        - Does the corresponding client method, e.g. `client.publish_qos1()` hang? Does that mean in the case of session ending, we would still have to process it eventually just to return a CompletionToken that can issue a CompletionError?