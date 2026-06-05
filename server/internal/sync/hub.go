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
	store Store
}

type HubOptions struct {
	DataDir string
	Store   Store
}

func NewHub(options ...HubOptions) (*Hub, error) {
	var opts HubOptions
	if len(options) > 0 {
		opts = options[0]
	}

	store := opts.Store
	if store == nil && opts.DataDir != "" {
		fileStore, err := NewFileStore(opts.DataDir)
		if err != nil {
			return nil, err
		}
		store = fileStore
	}

	return &Hub{rooms: map[string]*Room{}, store: store}, nil
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
		history := [][]byte(nil)
		if h.store != nil {
			loaded, err := h.store.Load(id)
			if err == nil {
				history = loaded
			}
		}
		room = NewRoom(id, h.store, history)
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
