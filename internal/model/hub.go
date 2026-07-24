package model

import "github.com/gorilla/websocket"

type JoinRequest struct {
	Conn     *websocket.Conn
	Room     string
	Username string
	Ack      chan struct{}
}

type LeaveRequest struct {
	Conn *websocket.Conn
	Room string
	Ack  chan struct{}
}

type Hub struct {
	Clients        map[*websocket.Conn]bool
	Register       chan *websocket.Conn
	Unregister     chan *websocket.Conn
	Broadcast      chan Message
	JoinRoom       chan JoinRequest
	LeaveRoom      chan LeaveRequest
	RegisterRoom   chan string
	UnregisterRoom chan string
	Rooms          map[string]map[*websocket.Conn]bool
	ClientRooms    map[*websocket.Conn]string
	ClientUsers    map[*websocket.Conn]string
}
