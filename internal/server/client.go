package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/etcha1/chat-server/internal/model"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// ServeWs handles websocket requests from the peer.
func ServeWs(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	hub := InitHub()
	hub.Register <- conn
	defer func() {
		hub.Unregister <- conn
	}()

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("read error: %v", err)
			}
			return
		}

		log.Printf("received message: %s", payload)
		var message model.Message
		err = json.Unmarshal(payload, &message)
		if err != nil {
			log.Printf("error parsing JSON: %v", err)
			return
		}

		GlobalHub.Broadcast <- message
	}
}
