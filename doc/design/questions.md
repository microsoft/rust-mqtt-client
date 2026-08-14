# Open Questions

## Monitoring
- Should connection state / client operations be separate from network traffic?
- Is user monitoring of ACK exchange, pings, etc. even worth providing vs just logging internally?
- The event-loop concept has since been implemented as `Connection` (driven via `run_until_disconnect`), which reports the reason for disconnection via `DisconnectedEvent`. Finer-grained event reporting is still open.

## Component naming
- See inline comments in source code for naming discussions
- In particular, the naming of `Client` (the outgoing-operations handle) is still under discussion.

## Queueing
- See `edge_case_behavior.md` for more discussion on queueing