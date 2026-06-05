package sync

import (
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

type Hub struct {
	mu    sync.Mutex
	rooms map[string]*Room
}

func NewHub() *Hub {
	return &Hub{rooms: map[string]*Room{}}
}

func (h *Hub) ServeRoom(w http.ResponseWriter, r *http.Request) {
	roomID := strings.TrimPrefix(r.URL.Path, "/rooms/")
	if roomID == "" || strings.Contains(roomID, "/") {
		http.Error(w, "room id is required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	room := h.room(roomID)
	client := NewClient(room, conn)
	room.Join(client)
	client.Run()
}

func (h *Hub) room(id string) *Room {
	h.mu.Lock()
	defer h.mu.Unlock()

	room, ok := h.rooms[id]
	if !ok {
		room = NewRoom(id)
		h.rooms[id] = room
		go room.Run()
	}

	return room
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool {
		return true
	},
}
