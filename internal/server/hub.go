package server

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/etcha1/chat-server/internal/model"
	"github.com/etcha1/chat-server/internal/repository"
	"github.com/gorilla/websocket"
)

var GlobalHub *model.Hub
var once sync.Once

func InitHub(messageRepo *repository.MessageRepository) *model.Hub {
	once.Do(func() {
		GlobalHub = &model.Hub{
			Clients:        make(map[*websocket.Conn]bool),
			Register:       make(chan *websocket.Conn),
			Unregister:     make(chan *websocket.Conn),
			Broadcast:      make(chan model.Message),
			JoinRoom:       make(chan model.JoinRequest),
			LeaveRoom:      make(chan model.LeaveRequest),
			RegisterRoom:   make(chan string),
			UnregisterRoom: make(chan string),
			Rooms:          make(map[string]map[*websocket.Conn]bool),
			ClientRooms:    make(map[*websocket.Conn]string),
			ClientUsers:    make(map[*websocket.Conn]string),
		}
		go RunHub(GlobalHub, messageRepo)
	})

	return GlobalHub
}

func RunHub(hub *model.Hub, messageRepo *repository.MessageRepository) {
	for {
		select {
		case conn := <-hub.Register:
			hub.Clients[conn] = true
		case conn := <-hub.Unregister:
			if room, ok := hub.ClientRooms[conn]; ok {
				if clients, ok := hub.Rooms[room]; ok {
					delete(clients, conn)
				}
				delete(hub.ClientRooms, conn)
			}
			if username, ok := hub.ClientUsers[conn]; ok {
				_ = username
				delete(hub.ClientUsers, conn)
			}
			if hub.Clients[conn] {
				delete(hub.Clients, conn)
				conn.Close()
			}
		case message := <-hub.Broadcast:
			payload, err := json.Marshal(message)
			if err != nil {
				log.Printf("marshal message: %v", err)
				continue
			}

			if message.Room == "" {
				for conn := range hub.Clients {
					if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
						log.Printf("broadcast write error: %v", err)
						delete(hub.Clients, conn)
						conn.Close()
					} else {
						messageRepo.CreateMessage(context.Background(), &message)
					}
				}
				continue
			}

			if clients, ok := hub.Rooms[message.Room]; ok {
				for conn := range clients {
					if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
						log.Printf("room broadcast write error: %v", err)
						delete(clients, conn)
						conn.Close()
					} else {
						messageRepo.CreateMessage(context.Background(), &message)
					}
				}
			}
		case join := <-hub.JoinRoom:
			if currentRoom, ok := hub.ClientRooms[join.Conn]; ok && currentRoom != "" {
				if clients, ok := hub.Rooms[currentRoom]; ok {
					delete(clients, join.Conn)
				}
			}
			if _, ok := hub.Rooms[join.Room]; !ok {
				hub.Rooms[join.Room] = make(map[*websocket.Conn]bool)
			}
			hub.Rooms[join.Room][join.Conn] = true
			hub.ClientRooms[join.Conn] = join.Room
			hub.ClientUsers[join.Conn] = join.Username

			presence := model.Message{
				Type:      "presence",
				Username:  join.Username,
				Room:      join.Room,
				Timestamp: time.Now(),
			}
			payload, err := json.Marshal(presence)
			if err != nil {
				log.Printf("marshal presence message: %v", err)
				continue
			}

			if clients, ok := hub.Rooms[join.Room]; ok {
				for conn := range clients {
					if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
						log.Printf("presence broadcast write error: %v", err)
						delete(clients, conn)
						conn.Close()
					}
				}
			}
			if join.Ack != nil {
				close(join.Ack)
			}

			messages, err := messageRepo.GetMessages(context.Background(), join.Room)
			if err != nil {
				log.Printf("getMessages error: %v", err)
				continue
			}
			for _, msg := range messages {
				msg.Type = "message"
				payload, err := json.Marshal(msg)
				if err != nil {
					log.Printf("marshal historical message: %v", err)
					continue
				}
				if err := join.Conn.WriteMessage(websocket.TextMessage, payload); err != nil {
					log.Printf("historical message write error: %v", err)
					delete(hub.Rooms[join.Room], join.Conn)
					join.Conn.Close()
					break
				}
			}
		case leave := <-hub.LeaveRoom:
			if currentRoom, ok := hub.ClientRooms[leave.Conn]; ok && currentRoom != "" {
				if username, ok := hub.ClientUsers[leave.Conn]; ok {
					presence := model.Message{
						Type:      "presence",
						Username:  username,
						Content:   "left",
						Room:      currentRoom,
						Timestamp: time.Now(),
					}
					payload, err := json.Marshal(presence)
					if err != nil {
						log.Printf("marshal leave presence message: %v", err)
					} else if clients, ok := hub.Rooms[currentRoom]; ok {
						for conn := range clients {
							if conn == leave.Conn {
								continue
							}
							if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
								log.Printf("leave presence write error: %v", err)
								delete(clients, conn)
								conn.Close()
							}
						}
					}
				}
				if clients, ok := hub.Rooms[currentRoom]; ok {
					delete(clients, leave.Conn)
				}
				delete(hub.ClientRooms, leave.Conn)
				delete(hub.ClientUsers, leave.Conn)
			}
			if leave.Ack != nil {
				close(leave.Ack)
			}
		case room := <-hub.RegisterRoom:
			if _, ok := hub.Rooms[room]; !ok {
				hub.Rooms[room] = make(map[*websocket.Conn]bool)
			}
		case room := <-hub.UnregisterRoom:
			if clients, ok := hub.Rooms[room]; ok {
				for conn := range clients {
					delete(clients, conn)
					conn.Close()
				}
				delete(hub.Rooms, room)
			}
		}
	}
}
