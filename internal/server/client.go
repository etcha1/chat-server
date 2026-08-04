package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/etcha1/chat-server/internal/model"
	"github.com/etcha1/chat-server/internal/repository"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// ServeWs handles websocket requests from the peer.
func ServeWs(w http.ResponseWriter, r *http.Request, messageRepo *repository.MessageRepository) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	hub := InitHub(messageRepo)
	hub.Register <- conn
	defer func() {
		hub.Unregister <- conn
	}()

	joinedRoom := ""
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

		if message.Type == "join" {
			if message.Room != "" {
				joinedRoom = message.Room
				ack := make(chan struct{})
				hub.JoinRoom <- model.JoinRequest{Conn: conn, Room: joinedRoom, Username: message.Username, Ack: ack}
				<-ack
			}
			continue
		}

		if message.Type == "leave" {
			if joinedRoom != "" {
				ack := make(chan struct{})
				hub.LeaveRoom <- model.LeaveRequest{Conn: conn, Room: joinedRoom, Ack: ack}
				<-ack
				joinedRoom = ""
			}
			continue
		}

		if message.Room == "" && joinedRoom != "" {
			message.Room = joinedRoom
		}
		GlobalHub.Broadcast <- message
	}
}
