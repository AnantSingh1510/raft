# Raft

Raft is an open source real-time collaboration engine: a Rust CRDT core, a Go sync server, and a TypeScript SDK packaged as one developer-friendly stack.

The first milestone is a narrow but working vertical slice:

- Rust core for document operations, state vectors, and binary updates
- Go WebSocket server for rooms, presence, and operation broadcasting
- TypeScript SDK for browser and Node clients
- Persistence adapters after the operation protocol settles

## Repository Layout

```text
crates/raft-core/       Rust CRDT core
server/                 Go WebSocket sync server
packages/raft-sdk/      TypeScript client SDK
docs/                   Architecture and protocol notes
examples/               Small runnable examples
```

## Current Status

Raft is now a working v0.1 collaboration MVP. It includes:

- A Rust `TextDocument` that can insert, delete, encode state, and apply binary updates
- A language-neutral wire protocol, operation protocol, and state-vector protocol
- A Go WebSocket sync server with room history replay and optional file persistence
- A TypeScript `RaftTextDocument` and reconnect-capable `RaftClient` SDK with presence callbacks
- Unit tests across Rust, Go, and TypeScript

This is not yet the final production YATA engine. The current text model is intentionally small and auditable so the transport, protocol, and SDK can evolve against something real.

## Getting Started

Run the Rust core tests:

```sh
cargo test
```

Run the Go server tests:

```sh
cd server
go test ./...
```

Build the TypeScript SDK:

```sh
npm install
npm run build --workspace @raft/sdk
```

Run every test suite:

```sh
npm run test:all
```

Start the sync server:

```sh
cd server
go run ./cmd/raft-server
```

The server listens on `:8080` and accepts WebSocket connections at `/rooms/{roomID}`.

Persist room logs to disk:

```sh
cd server
RAFT_DATA_DIR=.raft-data go run ./cmd/raft-server
```

Build a server container:

```sh
cd server
docker build -t raft-server .
```

## TypeScript Example

```ts
import { RaftClient, RaftTextDocument } from "@raft/sdk";

const doc = new RaftTextDocument(1);
const client = new RaftClient({
  url: "ws://localhost:8080",
  roomId: "notes",
  autoReconnect: true,
});

client.onUpdate((update) => doc.applyUpdate(update));
client.onPresence((presence) => console.log("presence", presence));
client.connect();
client.sync(doc.encodeStateVector());

const update = doc.insert(0, "Hello Raft");
client.send(update);
client.sendPresence({ user: "Ada", cursor: 10 });

const stateVector = doc.encodeStateVector();
const missingUpdate = doc.diffFromEncodedStateVector(stateVector);
```

## Rust Example

```rust
use raft_core::TextDocument;

let mut alice = TextDocument::new("notes", 1)?;
let mut bob = TextDocument::new("notes", 2)?;

let update = alice.insert(0, "Hello")?;
bob.apply_update(&update)?;

assert_eq!(alice.text(), bob.text());
# Ok::<(), Box<dyn std::error::Error>>(())
```
