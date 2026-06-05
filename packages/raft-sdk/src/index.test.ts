import assert from "node:assert/strict";
import test from "node:test";

import {
  RaftClient,
  RaftTextDocument,
  WireMessageType,
  decodeWireMessage,
  decodeOperations,
  decodeStateVector,
  encodeOperations,
  encodeStateVector,
  encodeWireMessage,
  joinRoomUrl,
  type Operation,
} from "./index.js";

test("encodes and decodes operations", () => {
  const op: Operation = {
    id: { client: 7, clock: 1 },
    originLeft: undefined,
    originRight: undefined,
    content: { type: "text", value: "hello" },
    deleted: false,
  };

  assert.deepEqual(decodeOperations(encodeOperations([op])), [op]);
});

test("text documents converge with binary updates", () => {
  const alice = new RaftTextDocument(1);
  const bob = new RaftTextDocument(2);

  bob.applyUpdate(alice.insert(0, "Ra"));
  alice.applyUpdate(bob.insert(2, "ft"));
  bob.applyUpdate(alice.delete(1, 1));

  assert.equal(alice.text, "Rft");
  assert.equal(bob.text, alice.text);
});

test("text documents order concurrent siblings deterministically", () => {
  const alice = new RaftTextDocument(1);
  const bob = new RaftTextDocument(2);

  bob.applyUpdate(alice.insert(0, "a"));
  const left = alice.insert(1, "x");
  const right = bob.insert(1, "y");

  alice.applyUpdate(right);
  bob.applyUpdate(left);

  assert.equal(alice.text, bob.text);
});

test("text document restores encoded state", () => {
  const alice = new RaftTextDocument(1);
  alice.insert(0, "hello");
  alice.delete(1, 1);

  const restored = new RaftTextDocument(2);
  restored.applyUpdate(alice.encodeState());

  assert.equal(restored.text, "hllo");
});

test("text document compacts stable unreferenced tombstones", () => {
  const doc = new RaftTextDocument(1);
  doc.insert(0, "ab");
  doc.delete(1, 1);

  const before = decodeOperations(doc.encodeState()).length;
  const removed = doc.compactTombstones(new Map([[1, 3]]));

  assert.equal(removed, 2);
  assert.equal(doc.text, "a");
  assert.equal(decodeOperations(doc.encodeState()).length, before - 2);
});

test("text document keeps deleted anchors while referenced", () => {
  const doc = new RaftTextDocument(1);
  doc.insert(0, "abc");
  doc.delete(1, 1);

  assert.equal(doc.compactTombstones(new Map([[1, 4]])), 0);
  assert.equal(doc.text, "ac");
});

test("encodes state vectors and diffs from them", () => {
  const alice = new RaftTextDocument(1);
  const bob = new RaftTextDocument(2);

  bob.applyUpdate(alice.insert(0, "Ra"));
  assert.deepEqual([...decodeStateVector(bob.encodeStateVector())], [[1, 2]]);

  const update = alice.insert(2, "ft");
  const diff = alice.diffFromEncodedStateVector(bob.encodeStateVector());

  assert.deepEqual(decodeOperations(diff), decodeOperations(update));
  bob.applyUpdate(diff);
  assert.equal(bob.text, "Raft");
});

test("round trips explicit state vectors", () => {
  const vector = new Map([
    [1, 3],
    [2, 7],
  ]);

  assert.deepEqual([...decodeStateVector(encodeStateVector(vector))], [...vector]);
});

test("joins room URLs", () => {
  assert.equal(joinRoomUrl("ws://localhost:8080", "notes"), "ws://localhost:8080/rooms/notes");
  assert.equal(joinRoomUrl("ws://localhost:8080/api/", "a b"), "ws://localhost:8080/api/rooms/a%20b");
});

test("encodes and decodes wire messages", () => {
  const message = {
    type: WireMessageType.Update,
    payload: new Uint8Array([1, 2, 3]),
  };

  assert.deepEqual(decodeWireMessage(encodeWireMessage(message)), message);
});

test("client queues updates while offline and flushes on open", () => {
  FakeWebSocket.instances = [];
  const client = new RaftClient({
    url: "ws://localhost:8080",
    roomId: "notes",
    WebSocketImpl: FakeWebSocket as unknown as typeof WebSocket,
  });
  const update = new Uint8Array([1, 2, 3]);

  client.send(update);
  client.connect();

  const socket = FakeWebSocket.instances[0]!;
  assert.equal(socket.sent.length, 0);
  socket.open();

  assert.deepEqual(decodeWireMessage(socket.sent[0]!), {
    type: WireMessageType.Update,
    payload: update,
  });
});

test("client emits presence messages", () => {
  FakeWebSocket.instances = [];
  const client = new RaftClient({
    url: "ws://localhost:8080",
    roomId: "notes",
    WebSocketImpl: FakeWebSocket as unknown as typeof WebSocket,
  });
  const seen: unknown[] = [];
  client.onPresence((presence) => seen.push(presence));

  client.connect();
  const socket = FakeWebSocket.instances[0]!;
  socket.message(
    encodeWireMessage({
      type: WireMessageType.Presence,
      payload: new TextEncoder().encode(JSON.stringify({ user: "Ada" })),
    }),
  );

  assert.deepEqual(seen, [{ user: "Ada" }]);
});

test("client auto reconnects after unexpected close", async () => {
  FakeWebSocket.instances = [];
  const client = new RaftClient({
    url: "ws://localhost:8080",
    roomId: "notes",
    WebSocketImpl: FakeWebSocket as unknown as typeof WebSocket,
    autoReconnect: true,
    reconnectDelayMs: 1,
  });

  client.connect();
  FakeWebSocket.instances[0]!.close();
  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.equal(FakeWebSocket.instances.length, 2);
  client.disconnect();
});

class FakeWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  static instances: FakeWebSocket[] = [];

  readonly sent: Uint8Array[] = [];
  readyState = FakeWebSocket.CONNECTING;
  binaryType: BinaryType = "arraybuffer";
  private listeners = new Map<string, Array<(event: any) => void>>();

  constructor(readonly url: string, readonly protocols?: string | string[]) {
    FakeWebSocket.instances.push(this);
  }

  addEventListener(type: string, listener: (event: any) => void): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    if (data instanceof Uint8Array) {
      this.sent.push(data);
      return;
    }
    throw new Error("fake socket only accepts Uint8Array test payloads");
  }

  close(): void {
    this.readyState = FakeWebSocket.CLOSED;
    this.emit("close", {});
  }

  open(): void {
    this.readyState = FakeWebSocket.OPEN;
    this.emit("open", {});
  }

  message(data: Uint8Array): void {
    this.emit("message", { data: data.buffer });
  }

  private emit(type: string, event: any): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event);
    }
  }
}
