export type ConnectionState = "idle" | "connecting" | "open" | "closed";

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

export interface RaftClientOptions {
  roomId: string;
  url: string;
  protocols?: string | string[];
  WebSocketImpl?: typeof WebSocket;
}

export type UpdateHandler = (update: Uint8Array) => void;
export type StateHandler = (state: ConnectionState) => void;

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
    const deleted = new Set(
      [...this.operations.values()]
        .filter((op) => op.content.type === "delete" && op.originLeft)
        .map((op) => opKey(op.originLeft!)),
    );

    const ordered: TextItem[] = [];
    const ops = [...this.operations.values()]
      .filter((op) => op.content.type === "text")
      .sort(compareOpIdByOperation);

    for (const op of ops) {
      const item: TextItem = {
        id: op.id,
        originLeft: op.originLeft,
        text: op.content.type === "text" ? op.content.value : "",
        visible: !op.deleted && !deleted.has(opKey(op.id)),
      };

      let position = -1;
      if (op.originRight) {
        position = ordered.findIndex((candidate) => sameOpId(candidate.id, op.originRight!));
      } else if (op.originLeft) {
        const leftIndex = findLastTextItemIndex(ordered, op.originLeft);
        if (leftIndex >= 0) {
          position = leftIndex + 1;
          while (
            position < ordered.length &&
            op.originLeft &&
            ordered[position]?.originLeft &&
            sameOpId(ordered[position].originLeft!, op.originLeft) &&
            compareOpId(ordered[position].id, op.id) < 0
          ) {
            position += 1;
          }
        }
      }

      if (position >= 0) {
        ordered.splice(position, 0, item);
      } else {
        ordered.push(item);
      }
    }

    return ordered.filter((item) => item.visible);
  }
}

export class RaftClient {
  readonly roomId: string;

  private readonly endpoint: string;
  private readonly protocols?: string | string[];
  private readonly WebSocketImpl: typeof WebSocket;
  private socket: WebSocket | null = null;
  private updateHandlers = new Set<UpdateHandler>();
  private stateHandlers = new Set<StateHandler>();
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
  }

  get state(): ConnectionState {
    return this.stateValue;
  }

  connect(): void {
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

    socket.addEventListener("open", () => this.setState("open"));
    socket.addEventListener("close", () => this.setState("closed"));
    socket.addEventListener("message", (event) => {
      if (event.data instanceof ArrayBuffer) {
        this.emitUpdate(new Uint8Array(event.data));
      }
    });
  }

  disconnect(): void {
    this.socket?.close();
    this.socket = null;
    this.setState("closed");
  }

  send(update: Uint8Array): void {
    if (!this.socket || this.socket.readyState !== this.WebSocketImpl.OPEN) {
      throw new Error("RaftClient is not connected");
    }

    this.socket.send(update);
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

  private setState(state: ConnectionState): void {
    if (this.stateValue === state) {
      return;
    }

    this.stateValue = state;
    for (const handler of this.stateHandlers) {
      handler(state);
    }
  }
}

export function joinRoomUrl(baseUrl: string, roomId: string): string {
  const url = new URL(baseUrl);
  const basePath = url.pathname.replace(/\/$/, "");
  url.pathname = `${basePath}/rooms/${encodeURIComponent(roomId)}`;
  return url.toString();
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

interface TextItem {
  id: OpId;
  originLeft?: OpId;
  text: string;
  visible: boolean;
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

function sameOpId(left: OpId, right: OpId): boolean {
  return left.client === right.client && left.clock === right.clock;
}

function compareOpIdByOperation(left: Operation, right: Operation): number {
  return compareOpId(left.id, right.id);
}

function compareOpId(left: OpId, right: OpId): number {
  return left.client - right.client || left.clock - right.clock;
}

function findLastTextItemIndex(items: TextItem[], id: OpId): number {
  for (let index = items.length - 1; index >= 0; index -= 1) {
    if (sameOpId(items[index]!.id, id)) {
      return index;
    }
  }

  return -1;
}
