package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestServeWsEchoesMessages(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(ServeWs))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("hello from test")); err != nil {
		t.Fatalf("write message: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}

	if string(payload) != "hello from test" {
		t.Fatalf("expected echoed message, got %q", string(payload))
	}
}
