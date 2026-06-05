import { RaftTextDocument } from "../packages/raft-sdk/dist/index.js";

const alice = new RaftTextDocument(1);
const bob = new RaftTextDocument(2);

bob.applyUpdate(alice.insert(0, "Ra"));
alice.applyUpdate(bob.insert(2, "ft"));
bob.applyUpdate(alice.delete(1, 1));

console.log({ alice: alice.text, bob: bob.text });
