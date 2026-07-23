package model

import "github.com/gorilla/websocket"

type Hub struct {
	Clients    map[*websocket.Conn]bool
	Register   chan *websocket.Conn
	Unregister chan *websocket.Conn
	Broadcast  chan Message
}
