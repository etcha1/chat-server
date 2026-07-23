package server

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/etcha1/chat-server/internal/model"
	"github.com/gorilla/websocket"
)

var GlobalHub *model.Hub
var once sync.Once

func InitHub() *model.Hub {
	once.Do(func() {
		GlobalHub = &model.Hub{
			Clients:    make(map[*websocket.Conn]bool),
			Register:   make(chan *websocket.Conn),
			Unregister: make(chan *websocket.Conn),
			Broadcast:  make(chan model.Message),
		}
		go RunHub(GlobalHub)
	})

	return GlobalHub
}

func RunHub(hub *model.Hub) {
	for {
		select {
		case conn := <-hub.Register:
			hub.Clients[conn] = true
		case conn := <-hub.Unregister:
			if hub.Clients[conn] {
				delete(hub.Clients, conn)
				conn.Close()
			}
		case message := <-hub.Broadcast:
			payload, err := json.Marshal(message)
			if err != nil {
				log.Printf("marshal broadcast message: %v", err)
				continue
			}

			for conn := range hub.Clients {
				if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
					log.Printf("broadcast write error: %v", err)
					delete(hub.Clients, conn)
					conn.Close()
				}
			}
		}
	}
}
