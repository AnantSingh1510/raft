export type ConnectionState = "idle" | "connecting" | "open" | "closed";

export interface RaftClientOptions {
  roomId: string;
  url: string;
  protocols?: string | string[];
  WebSocketImpl?: typeof WebSocket;
}

export type UpdateHandler = (update: Uint8Array) => void;
export type StateHandler = (state: ConnectionState) => void;

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
    this.roomId = options.roomId;
    this.endpoint = joinRoomUrl(options.url, options.roomId);
    this.protocols = options.protocols;
    this.WebSocketImpl = options.WebSocketImpl ?? WebSocket;
  }

  get state(): ConnectionState {
    return this.stateValue;
  }

  connect(): void {
    if (this.socket && (this.socket.readyState === WebSocket.CONNECTING || this.socket.readyState === WebSocket.OPEN)) {
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
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
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

