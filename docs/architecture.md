# Raft Architecture

Raft is split into three layers:

1. `raft-core`: owns CRDT operation IDs, state vectors, document integration, binary encoding, and deterministic merge semantics.
2. `raft-server`: owns transport, room membership, presence, authentication hooks, and persistence integration.
3. `@raft/sdk`: owns the client-facing API for browser and Node apps.

## Operation Protocol

The operation protocol is intentionally compact and language-neutral. Rust and TypeScript both encode and decode this frame format:

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

## Text Model

The v0.1 text document stores immutable operations and renders visible text by ordering text inserts between their left and right origins. Delete operations do not remove historical inserts; they tombstone their target operation. This keeps encoded state replayable and gives the server permission to stay protocol-agnostic.

## Server Model

The Go server owns WebSocket rooms. It accepts binary updates from any client, appends each update to the room history, broadcasts the update to other connected clients, and replays history to late joiners. The server does not inspect CRDT operations yet; persistence adapters can later store the same binary history without understanding document internals.

## SDK Model

The TypeScript SDK contains two independent pieces:

- `RaftTextDocument` for local text operations, binary update generation, and remote update application.
- `RaftClient` for WebSocket room connection, binary send, update callbacks, and connection-state callbacks.

