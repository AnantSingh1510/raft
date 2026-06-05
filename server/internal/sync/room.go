package sync

type Message struct {
	Sender *Client
	Data   []byte
}

type Room struct {
	ID        string
	join      chan *Client
	leave     chan *Client
	broadcast chan Message
	clients   map[*Client]struct{}
	history   [][]byte
	store     Store
}

func NewRoom(id string, store Store, history [][]byte) *Room {
	return &Room{
		ID:        id,
		join:      make(chan *Client),
		leave:     make(chan *Client),
		broadcast: make(chan Message, 128),
		clients:   map[*Client]struct{}{},
		history:   cloneUpdates(history),
		store:     store,
	}
}

func (r *Room) Join(client *Client) {
	r.join <- client
}

func (r *Room) Leave(client *Client) {
	r.leave <- client
}

func (r *Room) Broadcast(message Message) {
	r.Handle(message)
}

func (r *Room) Handle(message Message) {
	r.broadcast <- message
}

func (r *Room) Run() {
	for {
		select {
		case client := <-r.join:
			r.clients[client] = struct{}{}
			for _, update := range r.history {
				if !client.Send(EncodeWireMessage(WireMessage{Type: WireUpdate, Payload: update})) {
					r.drop(client)
					break
				}
			}
		case client := <-r.leave:
			if _, ok := r.clients[client]; ok {
				r.drop(client)
			}
		case message := <-r.broadcast:
			r.handleMessage(message)
		}
	}
}

func (r *Room) handleMessage(message Message) {
	wire, ok, err := DecodeWireMessage(message.Data)
	if err != nil {
		if message.Sender != nil {
			message.Sender.Send(EncodeWireMessage(WireMessage{Type: WireError, Payload: []byte(err.Error())}))
		}
		return
	}

	if !ok {
		r.acceptUpdate(message.Sender, message.Data)
		return
	}

	switch wire.Type {
	case WireUpdate:
		r.acceptUpdate(message.Sender, wire.Payload)
	case WireSync, WireStateVector:
		vector, err := DecodeStateVector(wire.Payload)
		if err != nil {
			if message.Sender != nil {
				message.Sender.Send(EncodeWireMessage(WireMessage{Type: WireError, Payload: []byte(err.Error())}))
			}
			return
		}
		for _, update := range r.history {
			if UpdateHasMissing(update, vector) {
				if message.Sender != nil && !message.Sender.Send(EncodeWireMessage(WireMessage{Type: WireUpdate, Payload: update})) {
					r.drop(message.Sender)
					return
				}
			}
		}
	case WirePresence:
		r.broadcastToPeers(message.Sender, EncodeWireMessage(wire))
	}
}

func (r *Room) acceptUpdate(sender *Client, update []byte) {
	update = append([]byte(nil), update...)
	if r.store != nil {
		_ = r.store.Append(r.ID, update)
	}
	r.history = append(r.history, update)
	r.broadcastToPeers(sender, EncodeWireMessage(WireMessage{Type: WireUpdate, Payload: update}))
}

func (r *Room) broadcastToPeers(sender *Client, data []byte) {
	for client := range r.clients {
		if client != sender {
			if !client.Send(data) {
				r.drop(client)
			}
		}
	}
}

func (r *Room) drop(client *Client) {
	delete(r.clients, client)
	close(client.send)
}

func cloneUpdates(updates [][]byte) [][]byte {
	cloned := make([][]byte, 0, len(updates))
	for _, update := range updates {
		cloned = append(cloned, append([]byte(nil), update...))
	}
	return cloned
}
