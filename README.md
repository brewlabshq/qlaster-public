# Qlaster

Shared-memory data streaming for colocated Solana services.

## Consumer

Connects to the sender over a Unix domain socket, receives a per-slot shared-memory ring plus eventfd, and drains account or transaction updates from that ring.

## Sender

Binds the Unix domain socket control plane, provisions shared-memory rings for consumers, tracks subscriptions, and publishes matching account updates or requested transaction updates into each consumer ring.

## Workflow

The sender receives account updates and optional transaction updates from broadcast channels. Consumers subscribe over the control socket, then receive data on the shared-memory ring. Account updates are filtered by subscribed account pubkeys or owners; transaction updates are delivered only after the consumer calls the transaction subscription API.
