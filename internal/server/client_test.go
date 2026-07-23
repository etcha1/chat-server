package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/etcha1/chat-server/internal/model"
	"github.com/gorilla/websocket"
)

func TestServeWsBroadcastsMessagesToAllClients(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(ServeWs))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"
	firstConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial first websocket: %v", err)
	}
	defer firstConn.Close()

	secondConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial second websocket: %v", err)
	}
	defer secondConn.Close()

	message := model.Message{Content: "hello from test", Timestamp: time.Now()}
	payload, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}

	if err := firstConn.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("write message: %v", err)
	}

	for name, conn := range map[string]*websocket.Conn{
		"first":  firstConn,
		"second": secondConn,
	} {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, received, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read message from %s websocket: %v", name, err)
		}
		if string(received) != string(payload) {
			t.Errorf("%s websocket received %q, expected %q", name, string(received), string(payload))
		}
	}
}
