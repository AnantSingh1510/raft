export type ConnectionState = "idle" | "connecting" | "open" | "closed";

const WIRE_MAGIC_0 = 0x52;
const WIRE_MAGIC_1 = 0x46;
const WIRE_VERSION = 1;

export enum WireMessageType {
  Sync = 1,
  StateVector = 2,
  Update = 3,
  Presence = 4,
  Error = 5,
}

export type WireMessage =
  | { type: WireMessageType.Sync; payload: Uint8Array }
  | { type: WireMessageType.StateVector; payload: Uint8Array }
  | { type: WireMessageType.Update; payload: Uint8Array }
  | { type: WireMessageType.Presence; payload: Uint8Array }
  | { type: WireMessageType.Error; payload: Uint8Array };

export interface OpId {
  client: number;
  clock: number;
}

export type OpContent =
  | { type: "text"; value: string }
  | { type: "bytes"; value: Uint8Array }
  | { type: "delete" };

export interface Operation {
  id: OpId;
  originLeft?: OpId;
  originRight?: OpId;
  content: OpContent;
  deleted: boolean;
}

export type StateVector = Map<number, number>;

export interface RaftClientOptions {
  roomId: string;
  url: string;
  protocols?: string | string[];
  WebSocketImpl?: typeof WebSocket;
  autoReconnect?: boolean;
  reconnectDelayMs?: number;
  queueWhileOffline?: boolean;
  maxQueuedUpdates?: number;
}

export type UpdateHandler = (update: Uint8Array) => void;
export type StateHandler = (state: ConnectionState) => void;
export type PresenceHandler = (presence: unknown) => void;

export class RaftTextDocument {
  private readonly clientId: number;
  private nextClock = 1;
  private operations = new Map<string, Operation>();
  private pending: Operation[] = [];

  constructor(clientId: number) {
    assertValidInteger(clientId, "clientId");
    if (clientId === 0) {
      throw new Error("clientId must be non-zero");
    }

    this.clientId = clientId;
  }

  get text(): string {
    return this.visibleTextItems()
      .map((item) => item.text)
      .join("");
  }

  insert(index: number, value: string): Uint8Array {
    const items = this.visibleTextItems();
    if (index < 0 || index > items.length) {
      throw new RangeError("insert index is out of bounds");
    }

    const ops: Operation[] = [];
    let originLeft = index > 0 ? items[index - 1]?.id : undefined;
    const originRight = items[index]?.id;

    for (const char of Array.from(value)) {
      const id = this.nextId();
      const op: Operation = {
        id,
        originLeft,
        originRight,
        content: { type: "text", value: char },
        deleted: false,
      };
      this.integrateOperation(op);
      ops.push(op);
      originLeft = id;
    }

    return encodeOperations(ops);
  }

  delete(index: number, length: number): Uint8Array {
    const items = this.visibleTextItems();
    if (index < 0 || length < 0 || index + length > items.length) {
      throw new RangeError("delete range is out of bounds");
    }

    const ops: Operation[] = [];
    for (const item of items.slice(index, index + length)) {
      const op: Operation = {
        id: this.nextId(),
        originLeft: item.id,
        content: { type: "delete" },
        deleted: false,
      };
      this.integrateOperation(op);
      ops.push(op);
    }

    return encodeOperations(ops);
  }

  applyUpdate(update: Uint8Array): void {
    for (const op of decodeOperations(update)) {
      this.integrateOperation(op);
      if (op.id.client === this.clientId) {
        this.nextClock = Math.max(this.nextClock, op.id.clock + 1);
      }
    }
  }

  encodeState(): Uint8Array {
    return encodeOperations([...this.operations.values()].sort(compareOpIdByOperation));
  }

  stateVector(): StateVector {
    const vector: StateVector = new Map();
    for (const op of this.operations.values()) {
      vector.set(op.id.client, Math.max(vector.get(op.id.client) ?? 0, op.id.clock));
    }
    return vector;
  }

  encodeStateVector(): Uint8Array {
    return encodeStateVector(this.stateVector());
  }

  diff(remote: StateVector): Uint8Array {
    const missing = [...this.operations.values()]
      .filter((op) => op.id.clock > (remote.get(op.id.client) ?? 0))
      .sort(compareOpIdByOperation);
    return encodeOperations(missing);
  }

  diffFromEncodedStateVector(remote: Uint8Array): Uint8Array {
    return this.diff(decodeStateVector(remote));
  }

  compactTombstones(stable: StateVector): number {
    const referenced = new Map<string, number>();
    const deleted = new Map<string, OpId[]>();

    for (const op of this.operations.values()) {
      if (op.originLeft) {
        referenced.set(opKey(op.originLeft), (referenced.get(opKey(op.originLeft)) ?? 0) + 1);
      }
      if (op.originRight) {
        referenced.set(opKey(op.originRight), (referenced.get(opKey(op.originRight)) ?? 0) + 1);
      }
      if (op.content.type === "delete" && op.originLeft) {
        const key = opKey(op.originLeft);
        deleted.set(key, [...(deleted.get(key) ?? []), op.id]);
      }
    }

    const remove: string[] = [];
    for (const [targetKey, deleteIds] of deleted) {
      const target = this.operations.get(targetKey);
      if (!target || target.content.type !== "text") {
        continue;
      }
      if (!isStable(target.id, stable) || deleteIds.some((id) => !isStable(id, stable))) {
        continue;
      }
      if ((referenced.get(targetKey) ?? 0) > deleteIds.length) {
        continue;
      }
      if (deleteIds.some((id) => (referenced.get(opKey(id)) ?? 0) > 0)) {
        continue;
      }

      remove.push(targetKey, ...deleteIds.map(opKey));
    }

    for (const key of remove) {
      this.operations.delete(key);
    }
    return remove.length;
  }

  private nextId(): OpId {
    return { client: this.clientId, clock: this.nextClock++ };
  }

  private integrateOperation(op: Operation): void {
    if (op.id.client === 0) {
      throw new Error("operation client id must be non-zero");
    }

    if (this.operations.has(opKey(op.id))) {
      return;
    }

    if (!this.hasOrigin(op.originLeft) || !this.hasOrigin(op.originRight)) {
      this.pending.push(op);
      return;
    }

    this.operations.set(opKey(op.id), op);
    this.drainPending();
  }

  private hasOrigin(origin?: OpId): boolean {
    return !origin || this.operations.has(opKey(origin));
  }

  private drainPending(): void {
    while (true) {
      const remaining: Operation[] = [];
      let progressed = false;

      for (const op of this.pending) {
        if (this.hasOrigin(op.originLeft) && this.hasOrigin(op.originRight)) {
          this.operations.set(opKey(op.id), op);
          progressed = true;
        } else {
          remaining.push(op);
        }
      }

      this.pending = remaining;
      if (!progressed) {
        return;
      }
    }
  }

  private visibleTextItems(): TextItem[] {
    return this.rgaSequence().filter((item) => item.visible);
  }

  private rgaSequence(): TextItem[] {
    const deleted = new Set(
      [...this.operations.values()]
        .filter((op) => op.content.type === "delete" && op.originLeft)
        .map((op) => opKey(op.originLeft!)),
    );

    const children = new Map<string, Operation[]>();
    for (const op of this.operations.values()) {
      if (op.content.type !== "text") {
        continue;
      }
      const key = optionalOpKey(op.originLeft);
      children.set(key, [...(children.get(key) ?? []), op]);
    }

    for (const ops of children.values()) {
      sortRgaSiblings(ops);
    }

    const ordered: TextItem[] = [];
    appendRgaChildren(undefined, children, deleted, ordered);
    return ordered;
  }
}

export class RaftClient {
  readonly roomId: string;

  private readonly endpoint: string;
  private readonly protocols?: string | string[];
  private readonly WebSocketImpl: typeof WebSocket;
  private readonly autoReconnect: boolean;
  private readonly reconnectDelayMs: number;
  private readonly queueWhileOffline: boolean;
  private readonly maxQueuedUpdates: number;
  private socket: WebSocket | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closeRequested = false;
  private queuedUpdates: Uint8Array[] = [];
  private updateHandlers = new Set<UpdateHandler>();
  private stateHandlers = new Set<StateHandler>();
  private presenceHandlers = new Set<PresenceHandler>();
  private stateValue: ConnectionState = "idle";

  constructor(options: RaftClientOptions) {
    const WebSocketImpl = options.WebSocketImpl ?? globalThis.WebSocket;
    if (!WebSocketImpl) {
      throw new Error("RaftClient requires a WebSocket implementation");
    }

    this.roomId = options.roomId;
    this.endpoint = joinRoomUrl(options.url, options.roomId);
    this.protocols = options.protocols;
    this.WebSocketImpl = WebSocketImpl;
    this.autoReconnect = options.autoReconnect ?? false;
    this.reconnectDelayMs = options.reconnectDelayMs ?? 1_000;
    this.queueWhileOffline = options.queueWhileOffline ?? true;
    this.maxQueuedUpdates = options.maxQueuedUpdates ?? 1_000;
  }

  get state(): ConnectionState {
    return this.stateValue;
  }

  connect(): void {
    this.closeRequested = false;
    this.clearReconnectTimer();

    if (
      this.socket &&
      (this.socket.readyState === this.WebSocketImpl.CONNECTING ||
        this.socket.readyState === this.WebSocketImpl.OPEN)
    ) {
      return;
    }

    this.setState("connecting");
    const socket = new this.WebSocketImpl(this.endpoint, this.protocols);
    socket.binaryType = "arraybuffer";
    this.socket = socket;

    socket.addEventListener("open", () => {
      this.setState("open");
      this.flushQueue();
    });
    socket.addEventListener("close", () => {
      if (this.socket === socket) {
        this.socket = null;
      }
      this.setState("closed");
      if (this.autoReconnect && !this.closeRequested) {
        this.scheduleReconnect();
      }
    });
    socket.addEventListener("message", (event) => {
      if (event.data instanceof ArrayBuffer) {
        this.handleMessage(new Uint8Array(event.data));
      }
    });
  }

  disconnect(): void {
    this.closeRequested = true;
    this.clearReconnectTimer();
    this.socket?.close();
    this.socket = null;
    this.setState("closed");
  }

  send(update: Uint8Array): void {
    this.sendWireMessage({ type: WireMessageType.Update, payload: update });
  }

  sync(stateVector: Uint8Array): void {
    this.sendWireMessage({ type: WireMessageType.Sync, payload: stateVector });
  }

  sendStateVector(stateVector: Uint8Array): void {
    this.sendWireMessage({ type: WireMessageType.StateVector, payload: stateVector });
  }

  sendPresence(presence: unknown): void {
    const payload = new TextEncoder().encode(JSON.stringify(presence));
    this.sendWireMessage({ type: WireMessageType.Presence, payload });
  }

  onPresence(handler: PresenceHandler): () => void {
    this.presenceHandlers.add(handler);
    return () => this.presenceHandlers.delete(handler);
  }

  private sendWireMessage(message: WireMessage): void {
    const frame = encodeWireMessage(message);
    if (this.socket && this.socket.readyState === this.WebSocketImpl.OPEN) {
      this.socket.send(frame);
      return;
    }

    if (this.queueWhileOffline) {
      if (this.queuedUpdates.length >= this.maxQueuedUpdates) {
        throw new Error("RaftClient offline queue is full");
      }
      this.queuedUpdates.push(frame);
      return;
    }

    if (!this.socket || this.socket.readyState !== this.WebSocketImpl.OPEN) {
      throw new Error("RaftClient is not connected");
    }
  }

  onUpdate(handler: UpdateHandler): () => void {
    this.updateHandlers.add(handler);
    return () => this.updateHandlers.delete(handler);
  }

  onStateChange(handler: StateHandler): () => void {
    this.stateHandlers.add(handler);
    return () => this.stateHandlers.delete(handler);
  }

  private emitUpdate(update: Uint8Array): void {
    for (const handler of this.updateHandlers) {
      handler(update);
    }
  }

  private emitPresence(presence: unknown): void {
    for (const handler of this.presenceHandlers) {
      handler(presence);
    }
  }

  private handleMessage(data: Uint8Array): void {
    const message = isWireMessage(data) ? decodeWireMessage(data) : { type: WireMessageType.Update, payload: data };

    switch (message.type) {
      case WireMessageType.Update:
        this.emitUpdate(message.payload);
        break;
      case WireMessageType.Presence:
        this.emitPresence(JSON.parse(new TextDecoder().decode(message.payload)));
        break;
      case WireMessageType.Error:
        throw new Error(new TextDecoder().decode(message.payload));
      case WireMessageType.Sync:
      case WireMessageType.StateVector:
        break;
    }
  }

  private setState(state: ConnectionState): void {
    if (this.stateValue === state) {
      return;
    }

    this.stateValue = state;
    for (const handler of this.stateHandlers) {
      handler(state);
    }
  }

  private flushQueue(): void {
    if (!this.socket || this.socket.readyState !== this.WebSocketImpl.OPEN) {
      return;
    }

    const updates = this.queuedUpdates.splice(0);
    for (const update of updates) {
      this.socket.send(update);
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) {
      return;
    }

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, this.reconnectDelayMs);
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }
}

export function joinRoomUrl(baseUrl: string, roomId: string): string {
  const url = new URL(baseUrl);
  const basePath = url.pathname.replace(/\/$/, "");
  url.pathname = `${basePath}/rooms/${encodeURIComponent(roomId)}`;
  return url.toString();
}

export function encodeWireMessage(message: WireMessage): Uint8Array {
  const bytes = new Uint8Array(8 + message.payload.byteLength);
  const view = new DataView(bytes.buffer);
  bytes[0] = WIRE_MAGIC_0;
  bytes[1] = WIRE_MAGIC_1;
  bytes[2] = WIRE_VERSION;
  bytes[3] = message.type;
  view.setUint32(4, message.payload.byteLength, false);
  bytes.set(message.payload, 8);
  return bytes;
}

export function decodeWireMessage(bytes: Uint8Array): WireMessage {
  if (!isWireMessage(bytes)) {
    throw new Error("invalid Raft wire message");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const length = view.getUint32(4, false);
  if (bytes.byteLength !== 8 + length) {
    throw new Error("invalid Raft wire message length");
  }
  const type = bytes[3] as WireMessageType;
  if (type < WireMessageType.Sync || type > WireMessageType.Error) {
    throw new Error(`invalid Raft wire message type: ${type}`);
  }
  return { type, payload: bytes.slice(8) } as WireMessage;
}

export function isWireMessage(bytes: Uint8Array): boolean {
  return bytes.byteLength >= 8 && bytes[0] === WIRE_MAGIC_0 && bytes[1] === WIRE_MAGIC_1 && bytes[2] === WIRE_VERSION;
}

export function encodeOperations(ops: Operation[]): Uint8Array {
  const encoder = new TextEncoder();
  const chunks: Uint8Array[] = [];
  let totalLength = 4;

  for (const op of ops) {
    const content = encodeContent(op.content, encoder);
    chunks.push(content);
    totalLength += 8 * 6 + 1 + 1 + 4 + content.byteLength;
  }

  const bytes = new Uint8Array(totalLength);
  const view = new DataView(bytes.buffer);
  let offset = 0;
  view.setUint32(offset, ops.length, false);
  offset += 4;

  ops.forEach((op, index) => {
    const content = chunks[index]!;
    writeOpId(view, offset, op.id);
    offset += 16;
    writeOptionalOpId(view, offset, op.originLeft);
    offset += 16;
    writeOptionalOpId(view, offset, op.originRight);
    offset += 16;
    view.setUint8(offset, op.deleted ? 1 : 0);
    offset += 1;
    view.setUint8(offset, contentType(op.content));
    offset += 1;
    view.setUint32(offset, content.byteLength, false);
    offset += 4;
    bytes.set(content, offset);
    offset += content.byteLength;
  });

  return bytes;
}

export function decodeOperations(bytes: Uint8Array): Operation[] {
  const decoder = new TextDecoder(undefined, { fatal: true });
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let offset = 0;

  const count = readUint32(view, offset);
  offset += 4;

  const ops: Operation[] = [];
  for (let i = 0; i < count; i += 1) {
    const id = readOpId(view, offset);
    offset += 16;
    const originLeft = readOptionalOpId(view, offset);
    offset += 16;
    const originRight = readOptionalOpId(view, offset);
    offset += 16;
    const deleted = readUint8(view, offset) !== 0;
    offset += 1;
    const type = readUint8(view, offset);
    offset += 1;
    const contentLength = readUint32(view, offset);
    offset += 4;
    if (offset + contentLength > bytes.byteLength) {
      throw new Error("encoded operation ended unexpectedly");
    }
    const payload = bytes.slice(offset, offset + contentLength);
    offset += contentLength;

    ops.push({
      id,
      originLeft,
      originRight,
      deleted,
      content: decodeContent(type, payload, decoder),
    });
  }

  if (offset !== bytes.byteLength) {
    throw new Error("encoded operation contained trailing bytes");
  }

  return ops;
}

export function encodeStateVector(vector: StateVector): Uint8Array {
  const entries = [...vector.entries()].sort((left, right) => left[0] - right[0]);
  const bytes = new Uint8Array(4 + entries.length * 16);
  const view = new DataView(bytes.buffer);
  let offset = 0;
  view.setUint32(offset, entries.length, false);
  offset += 4;

  for (const [client, clock] of entries) {
    assertValidInteger(client, "client");
    assertValidInteger(clock, "clock");
    if (client === 0) {
      throw new Error("client id must be non-zero");
    }
    view.setBigUint64(offset, BigInt(client), false);
    view.setBigUint64(offset + 8, BigInt(clock), false);
    offset += 16;
  }

  return bytes;
}

export function decodeStateVector(bytes: Uint8Array): StateVector {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let offset = 0;
  const count = readUint32(view, offset);
  offset += 4;
  const vector: StateVector = new Map();

  for (let i = 0; i < count; i += 1) {
    const client = Number(readBigUint64(view, offset));
    const clock = Number(readBigUint64(view, offset + 8));
    offset += 16;
    if (client === 0) {
      throw new Error("client id must be non-zero");
    }
    vector.set(client, clock);
  }

  if (offset !== bytes.byteLength) {
    throw new Error("encoded state vector contained trailing bytes");
  }

  return vector;
}

interface TextItem {
  id: OpId;
  originLeft?: OpId;
  originRight?: OpId;
  text: string;
  visible: boolean;
}

function appendRgaChildren(
  parent: OpId | undefined,
  children: Map<string, Operation[]>,
  deleted: Set<string>,
  ordered: TextItem[],
): void {
  for (const op of children.get(optionalOpKey(parent)) ?? []) {
    if (op.content.type !== "text") {
      continue;
    }
    ordered.push({
      id: op.id,
      originLeft: op.originLeft,
      originRight: op.originRight,
      text: op.content.value,
      visible: !op.deleted && !deleted.has(opKey(op.id)),
    });
    appendRgaChildren(op.id, children, deleted, ordered);
  }
}

function sortRgaSiblings(ops: Operation[]): void {
  const sorted: Operation[] = [];
  for (const op of ops.splice(0)) {
    let index = 0;
    while (index < sorted.length && rgaSiblingAfter(op, sorted[index]!)) {
      index += 1;
    }
    sorted.splice(index, 0, op);
  }
  ops.push(...sorted);
}

function rgaSiblingAfter(left: Operation, right: Operation): boolean {
  if (left.originRight && sameOpId(left.originRight, right.id)) {
    return false;
  }
  if (right.originRight && sameOpId(right.originRight, left.id)) {
    return true;
  }
  return compareOpId(left.id, right.id) > 0;
}

function encodeContent(content: OpContent, encoder: TextEncoder): Uint8Array {
  switch (content.type) {
    case "text":
      return encoder.encode(content.value);
    case "bytes":
      return content.value;
    case "delete":
      return new Uint8Array();
  }
}

function decodeContent(type: number, payload: Uint8Array, decoder: TextDecoder): OpContent {
  switch (type) {
    case 1:
      return { type: "text", value: decoder.decode(payload) };
    case 2:
      return { type: "bytes", value: payload };
    case 3:
      return { type: "delete" };
    default:
      throw new Error(`invalid operation content type: ${type}`);
  }
}

function contentType(content: OpContent): number {
  switch (content.type) {
    case "text":
      return 1;
    case "bytes":
      return 2;
    case "delete":
      return 3;
  }
}

function writeOpId(view: DataView, offset: number, id: OpId): void {
  assertValidInteger(id.client, "client");
  assertValidInteger(id.clock, "clock");
  view.setBigUint64(offset, BigInt(id.client), false);
  view.setBigUint64(offset + 8, BigInt(id.clock), false);
}

function writeOptionalOpId(view: DataView, offset: number, id?: OpId): void {
  writeOpId(view, offset, id ?? { client: 0, clock: 0 });
}

function readOpId(view: DataView, offset: number): OpId {
  const id = {
    client: Number(readBigUint64(view, offset)),
    clock: Number(readBigUint64(view, offset + 8)),
  };
  if (id.client === 0) {
    throw new Error("operation client id must be non-zero");
  }
  return id;
}

function readOptionalOpId(view: DataView, offset: number): OpId | undefined {
  const id = {
    client: Number(readBigUint64(view, offset)),
    clock: Number(readBigUint64(view, offset + 8)),
  };
  return id.client === 0 ? undefined : id;
}

function readUint8(view: DataView, offset: number): number {
  ensureAvailable(view, offset, 1);
  return view.getUint8(offset);
}

function readUint32(view: DataView, offset: number): number {
  ensureAvailable(view, offset, 4);
  return view.getUint32(offset, false);
}

function readBigUint64(view: DataView, offset: number): bigint {
  ensureAvailable(view, offset, 8);
  return view.getBigUint64(offset, false);
}

function ensureAvailable(view: DataView, offset: number, length: number): void {
  if (offset + length > view.byteLength) {
    throw new Error("encoded operation ended unexpectedly");
  }
}

function assertValidInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative safe integer`);
  }
}

function opKey(id: OpId): string {
  return `${id.client}:${id.clock}`;
}

function optionalOpKey(id?: OpId): string {
  return id ? opKey(id) : "root";
}

function sameOpId(left: OpId, right: OpId): boolean {
  return left.client === right.client && left.clock === right.clock;
}

function compareOpIdByOperation(left: Operation, right: Operation): number {
  return compareOpId(left.id, right.id);
}

function compareOpId(left: OpId, right: OpId): number {
  return left.client - right.client || left.clock - right.clock;
}

function isStable(id: OpId, stable: StateVector): boolean {
  return (stable.get(id.client) ?? 0) >= id.clock;
}
