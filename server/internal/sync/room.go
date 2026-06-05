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
			for _, update := range r.history {
				if !client.Send(update) {
					r.drop(client)
					break
				}
			}
		case client := <-r.leave:
			if _, ok := r.clients[client]; ok {
				r.drop(client)
			}
		case message := <-r.broadcast:
			r.history = append(r.history, append([]byte(nil), message.Data...))
			for client := range r.clients {
				if client != message.Sender {
					if !client.Send(message.Data) {
						r.drop(client)
					}
				}
			}
		}
	}
}

func (r *Room) drop(client *Client) {
	delete(r.clients, client)
	close(client.send)
}
