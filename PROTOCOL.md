# Kueue client-server model and protocol

Kueue is designed in a client-server model. There are two different types of
clients: The actual "client" who sends job requests to the server, and the
"worker" client, who registers as a worker and waits for jobs to be assigned to
them for processing.

All communication takes place over a single TCP socket connection. The clients
connect to the server and send an initial "hello" message to distinguish between
client and worker. All possible messages are defined in
[src/messages/mod.rs](src/messages/mod.rs) as structs. For a transfer over the
network, a message struct is serialized to JSON on the sender and deserialized
from JSON back to a struct from the receiver. The text-based transfer in the
JSON format should make it possible to also implement clients in other
languages, e.g., in Python using the `socket` and `json` packages.

Authentication is currently very simple and no encryption is considered. To
ensure that no secrets are leaked, authentification is implemented in a simple
[challege-response](https://en.wikipedia.org/wiki/Challenge%E2%80%93response_authentication)
protocol with a shared secret that all trusted parties know. If authentication
is required for a specific task, the required challege-response protocol needs
to be conducted beforehand. For client connections, the client initiates the
authetication using the `AuthRequest` message. Workers always need to be
autheticated to process jobs, so the authetication is part of the initial
hand-shake with the server. 

TODO: continue documentation...

## Welcome hand-shake with the server

## Authentication challege-response protocol

## Client communication

## Worker communication
