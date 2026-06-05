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

Start the sync server:

```sh
cd server
go run ./cmd/raft-server
```

The server listens on `:8080` and accepts WebSocket connections at `/rooms/{roomID}`.

