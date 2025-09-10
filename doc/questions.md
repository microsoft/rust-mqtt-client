# Open Questions

## Monitoring
- Should connection state / client operations be separate from network traffic?
- Is user monitoring of ACK exchange, pings, etc. even worth providing vs just logging internally?
- Implication - if we only care about client-level, `EventLoop` should instead be a `Connection`
- If we're sending the results to the caller (i.e. the response packets) then 

## Component naming
- See inline comments in source code for naming discussions

## Queueing
- See `edge_case_behavior.md` for more discussion on queueing

## Reauthentication
- Should this be done by token rather than client API? After all, a reauth is only valid when connected using a particular auth method on the CONNECT packet. Issuing an AUTH packet in other contexts is a spec violation that a client is not supposed to do.
- Perhaps the `CompletionToken` returned by `.connect()` could be `CompletionToken<ConnAck, Option<ReauthToken>>` instead

## Connect / Disconnect
- It is a spec violation to send a CONNECT while already connected.
- Similarly, it doesn't make sense to try to send a DISCONNECT while already disconnected.
- Can the API be improved around these cases?