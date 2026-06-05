# Raft Architecture

Raft is split into three layers:

1. `raft-core`: owns CRDT operation IDs, state vectors, document integration, binary encoding, and deterministic merge semantics.
2. `raft-server`: owns transport, room membership, presence, authentication hooks, and persistence integration.
3. `@raft/sdk`: owns the client-facing API for browser and Node apps.

## Operation Protocol

Document updates are carried inside a Raft wire message. The operation protocol is intentionally compact and language-neutral. Rust and TypeScript both encode and decode this frame format:

```text
[op_count: u32]
  repeated op:
    [client_id: u64]
    [clock: u64]
    [origin_left_client: u64]
    [origin_left_clock: u64]
    [origin_right_client: u64]
    [origin_right_clock: u64]
    [deleted: u8]
    [content_type: u8]
    [content_len: u32]
    [content: bytes]
```

An origin client value of `0` means the origin is absent. Real client IDs must be non-zero.

Content types:

| Type | Meaning |
|---:|---|
| `1` | UTF-8 text insert |
| `2` | Opaque bytes |
| `3` | Delete tombstone; `origin_left` points at the deleted text op |

## State-Vector Protocol

Rust and TypeScript also share a compact state-vector frame:

```text
[entry_count: u32]
  repeated entry:
    [client_id: u64]
    [clock: u64]
```

State vectors let one peer ask another for only the operations it has not seen yet. The current SDK exposes this through `encodeStateVector()` and `diffFromEncodedStateVector(...)`.

## Wire Protocol

All modern WebSocket messages use an envelope:

```text
[magic: "RF"][version: u8][type: u8][payload_len: u32][payload]
```

Message types:

| Type | Name | Payload |
|---:|---|---|
| `1` | `sync` | Encoded state vector. Server responds with missing update messages. |
| `2` | `state-vector` | Encoded state vector. Reserved for peer negotiation. |
| `3` | `update` | Encoded operation update. Persisted and broadcast. |
| `4` | `presence` | JSON presence payload. Broadcast only, not persisted. |
| `5` | `error` | UTF-8 error string. |

The Go server still accepts legacy raw update frames and treats them as update payloads. This keeps older clients from breaking during the protocol transition.

## Text Model

The v0.1 text document stores immutable operations and renders visible text by ordering text inserts between their left and right origins. Delete operations do not remove historical inserts; they tombstone their target operation. This keeps encoded state replayable and gives the server permission to stay protocol-agnostic.

## Server Model

The Go server owns WebSocket rooms. It accepts update messages from any client, appends each update payload to the room history, broadcasts the update envelope to other connected clients, and replays history to late joiners. When `RAFT_DATA_DIR` is set, each room history is also stored as an append-only length-prefixed log on disk.

For `sync` messages, the server parses the encoded state vector and scans stored operation updates. It sends back any update whose operation clock is newer than the requesting client's state vector. The server still keeps persistence simple by storing the original binary update payloads.

## SDK Model

The TypeScript SDK contains two independent pieces:

- `RaftTextDocument` for local text operations, binary update generation, and remote update application.
- `RaftClient` for WebSocket room connection, binary send, state-vector sync requests, presence messages, offline queueing, optional reconnects, update callbacks, presence callbacks, and connection-state callbacks.

## Release Surface

The repository includes a GitHub Actions CI workflow and a Dockerfile for the Go sync server. The local verification command is:

```sh
npm run test:all
```
