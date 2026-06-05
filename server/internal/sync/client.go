package sync

import (
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	maxMessage = 1 << 20
)

type Client struct {
	room *Room
	conn *websocket.Conn
	send chan []byte
}

func NewClient(room *Room, conn *websocket.Conn) *Client {
	return &Client{
		room: room,
		conn: conn,
		send: make(chan []byte, 32),
	}
}

func (c *Client) Send(data []byte) {
	select {
	case c.send <- data:
	default:
		c.room.Leave(c)
	}
}

func (c *Client) Run() {
	go c.writePump()
	c.readPump()
}

func (c *Client) readPump() {
	defer func() {
		c.room.Leave(c)
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessage)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		messageType, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		c.room.Broadcast(Message{Sender: c, Data: data})
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case data, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
