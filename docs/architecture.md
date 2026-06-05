# Raft Architecture

Raft is split into three layers:

1. `raft-core`: owns CRDT operation IDs, state vectors, document integration, binary encoding, and deterministic merge semantics.
2. `raft-server`: owns transport, room membership, presence, authentication hooks, and persistence integration.
3. `@raft/sdk`: owns the client-facing API for browser and Node apps.

The operation protocol is intentionally compact and language-neutral. The current binary frame format is:

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

