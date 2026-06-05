import assert from "node:assert/strict";
import test from "node:test";

import { RaftTextDocument, decodeOperations, encodeOperations, joinRoomUrl, type Operation } from "./index.js";

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

test("text document restores encoded state", () => {
  const alice = new RaftTextDocument(1);
  alice.insert(0, "hello");
  alice.delete(1, 1);

  const restored = new RaftTextDocument(2);
  restored.applyUpdate(alice.encodeState());

  assert.equal(restored.text, "hllo");
});

test("joins room URLs", () => {
  assert.equal(joinRoomUrl("ws://localhost:8080", "notes"), "ws://localhost:8080/rooms/notes");
  assert.equal(joinRoomUrl("ws://localhost:8080/api/", "a b"), "ws://localhost:8080/api/rooms/a%20b");
});
