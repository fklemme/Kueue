# Kueue client-server model and protocol

Kueue is designed in a client-server model. There are two different types of
clients: The actual "client" who sends job requests to the server, and the
"worker" client, who registers as a worker and waits for jobs to be assigned to
them for processing.

All communication takes place over a single TCP socket connection. The clients
connect to the server and send an initial "hello" message to distinguish between
client and worker. All possible messages are defined in
[src/messages/mod.rs](src/messages/mod.rs) as structs. For the transfer over the
network, a message struct is serialized into JSON on the sender's side and
deserialized from JSON back into a struct on the receiver's side. The text-based
transfer in the JSON format should make it possible to also implement clients in
other languages, e.g., in Python using the `socket` and `json` packages.

Authentication is currently very simple and no encryption is considered. To
ensure that no secrets are leaked, authentication is implemented in a simple
[challenge-response](https://en.wikipedia.org/wiki/Challenge%E2%80%93response_authentication)
protocol with a shared secret that all trusted parties know. If authentication
is required for a specific task, the required challenge-response protocol needs
to be conducted beforehand. For client connections, the client initiates the
authentication using the `AuthRequest` message. Workers always need to be
authenticated to process jobs, so the authentication is part of the initial
hand-shake with the server. 

TODO: continue documentation...

## Welcome hand-shake with the server

## Challenge-response authentication



## Client communication

### Connect to the server

| Client          |    | Server        |
|-----------------|----|---------------|
| HelloFromClient | -> |               |
|                 | <- | WelcomeClient |

### Escalate privileges through authentication

Most non-changing request to the server can be performed without authentication.
However, state-changing requests such as issuing new jobs or removing an
existing jobs from the server requires prior authentication. This must be
requested from the client beforehand. If the client attempts to send a request
that requires authentication before performing the following authentication
protocol, the connection will be closed by the server.

| Client                                      |    | Server              |
|---------------------------------------------|----|---------------------|
| AuthRequest                                 | -> |                     |
|                                             | <- | AuthChallenge(salt) |
| AuthResponse(base64(sha256(secret + salt))) | -> |                     |
|                                             | <- | AuthAccepted(bool)  |

## Worker communication

### Connect and authenticate

The worker-server hand-shake is similar to the one with the client. However, for
workers, authentication is mandatory and must be handled imediately after the
welcome message. Therefore, authentication is not requested by the worker but
the `AuthChallenge` is sent immediately after the `WelcomeWorker` message.

| Worker                                      |    | Server              |
|---------------------------------------------|----|---------------------|
| HelloFromWorker(worker_name)                | -> |                     |
|                                             | <- | WelcomeWorker       |
|                                             | <- | AuthChallenge(salt) |
| AuthResponse(base64(sha256(secret + salt))) | -> |                     |
|                                             | <- | AuthAccepted(bool)  |
