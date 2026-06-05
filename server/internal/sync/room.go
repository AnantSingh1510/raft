package sync

type Message struct {
	Sender *Client
	Data   []byte
}

type Room struct {
	ID         string
	join       chan *Client
	leave      chan *Client
	broadcast  chan Message
	clients    map[*Client]struct{}
	lastUpdate []byte
}

func NewRoom(id string) *Room {
	return &Room{
		ID:        id,
		join:      make(chan *Client),
		leave:     make(chan *Client),
		broadcast: make(chan Message, 128),
		clients:   map[*Client]struct{}{},
	}
}

func (r *Room) Join(client *Client) {
	r.join <- client
}

func (r *Room) Leave(client *Client) {
	r.leave <- client
}

func (r *Room) Broadcast(message Message) {
	r.broadcast <- message
}

func (r *Room) Run() {
	for {
		select {
		case client := <-r.join:
			r.clients[client] = struct{}{}
			if len(r.lastUpdate) > 0 {
				client.Send(r.lastUpdate)
			}
		case client := <-r.leave:
			if _, ok := r.clients[client]; ok {
				delete(r.clients, client)
				close(client.send)
			}
		case message := <-r.broadcast:
			r.lastUpdate = append(r.lastUpdate[:0], message.Data...)
			for client := range r.clients {
				if client != message.Sender {
					client.Send(message.Data)
				}
			}
		}
	}
}
