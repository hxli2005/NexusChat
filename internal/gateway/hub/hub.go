package hub

import (
	"sync"

	"github.com/gorilla/websocket"
)

// ConnHub keeps websocket connections for users connected to this gateway node.
type ConnHub struct {
	mu    sync.RWMutex
	conns map[int64]*websocket.Conn
}

func New() *ConnHub {
	return &ConnHub{conns: make(map[int64]*websocket.Conn)}
}

func (h *ConnHub) Register(userID int64, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.conns[userID] = conn
}

func (h *ConnHub) Unregister(userID int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if conn, ok := h.conns[userID]; ok {
		_ = conn.Close()
		delete(h.conns, userID)
	}
}

func (h *ConnHub) Push(userID int64, msg []byte) bool {
	h.mu.RLock()
	conn, ok := h.conns[userID]
	h.mu.RUnlock()
	if !ok {
		return false
	}
	if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
		h.Unregister(userID)
		return false
	}
	return true
}
