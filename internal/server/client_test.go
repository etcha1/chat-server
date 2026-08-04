package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/etcha1/chat-server/internal/model"
	"github.com/etcha1/chat-server/internal/repository"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	pgxconn "github.com/jackc/pgx/v5/pgconn"
)

func resetHubForTests() {
	GlobalHub = nil
	once = sync.Once{}
}

func drainSocketMessages(t *testing.T, conn *websocket.Conn) {
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if strings.Contains(err.Error(), "i/o timeout") {
				return
			}
			t.Fatalf("drain socket messages: %v", err)
		}
	}
}

type stubQueryExecutor struct {
	rows pgx.Rows
}

func (s *stubQueryExecutor) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return s.rows, nil
}

func (s *stubQueryExecutor) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return nil
}

func (s *stubQueryExecutor) Exec(ctx context.Context, sql string, args ...any) (pgxconn.CommandTag, error) {
	return pgxconn.CommandTag{}, nil
}

type stubRows struct{}

func (s *stubRows) Close() {}
func (s *stubRows) Err() error { return nil }
func (s *stubRows) CommandTag() pgxconn.CommandTag { return pgxconn.CommandTag{} }
func (s *stubRows) FieldDescriptions() []pgxconn.FieldDescription { return nil }
func (s *stubRows) Next() bool { return false }
func (s *stubRows) Scan(dest ...any) error { return nil }
func (s *stubRows) Values() ([]any, error) { return nil, nil }
func (s *stubRows) RawValues() [][]byte { return nil }
func (s *stubRows) Conn() *pgx.Conn { return nil }

func newTestMessageRepository() *repository.MessageRepository {
	return repository.NewMessageRepository(&stubQueryExecutor{rows: &stubRows{}})
}

func TestServeWsBroadcastsMessagesToAllClients(t *testing.T) {
	resetHubForTests()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWs(w, r, newTestMessageRepository())
	}))
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

func TestServeWsRoutesMessagesByRoom(t *testing.T) {
	resetHubForTests()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWs(w, r, newTestMessageRepository())
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"
	generalConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial general websocket: %v", err)
	}
	defer generalConn.Close()

	randomConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial random websocket: %v", err)
	}
	defer randomConn.Close()

	joinPayload, err := json.Marshal(model.Message{
		Type:      "join",
		Username:  "alice",
		Room:      "general",
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal join message: %v", err)
	}
	if err := generalConn.WriteMessage(websocket.TextMessage, joinPayload); err != nil {
		t.Fatalf("write join message: %v", err)
	}
	generalConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = generalConn.ReadMessage()
	if err != nil {
		t.Fatalf("reading presence update from general room websocket: %v", err)
	}

	randomJoinPayload, err := json.Marshal(model.Message{
		Type:      "join",
		Username:  "bob",
		Room:      "random",
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal random join message: %v", err)
	}
	if err := randomConn.WriteMessage(websocket.TextMessage, randomJoinPayload); err != nil {
		t.Fatalf("write random join message: %v", err)
	}
	randomConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = randomConn.ReadMessage()
	if err != nil {
		t.Fatalf("reading presence update from random room websocket: %v", err)
	}

	message := model.Message{
		Type:      "message",
		Username:  "alice",
		Content:   "hello general",
		Room:      "general",
		Timestamp: time.Now(),
	}
	payload, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal room message: %v", err)
	}
	if err := generalConn.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("write room message: %v", err)
	}

	generalConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, received, err := generalConn.ReadMessage()
	if err != nil {
		t.Fatalf("reading message from general room websocket: %v", err)
	}
	if string(received) != string(payload) {
		t.Fatalf("general websocket received %q, expected %q", string(received), string(payload))
	}

	randomConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err = randomConn.ReadMessage()
	if err == nil {
		t.Fatal("random websocket unexpectedly received a message from the general room")
	}
	if !websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) && !strings.Contains(err.Error(), "i/o timeout") {
		t.Fatalf("expected timeout reading from random websocket, got %v", err)
	}
}

func TestServeWsLeaveRoomStopsReceivingMessages(t *testing.T) {
	resetHubForTests()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWs(w, r, newTestMessageRepository())
	}))
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

	joinPayload, err := json.Marshal(model.Message{
		Type:      "join",
		Username:  "alice",
		Room:      "general",
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal join message: %v", err)
	}
	if err := firstConn.WriteMessage(websocket.TextMessage, joinPayload); err != nil {
		t.Fatalf("write join message: %v", err)
	}
	if err := secondConn.WriteMessage(websocket.TextMessage, joinPayload); err != nil {
		t.Fatalf("write join message: %v", err)
	}

	drainSocketMessages(t, firstConn)
	drainSocketMessages(t, secondConn)

	leavePayload, err := json.Marshal(model.Message{
		Type:      "leave",
		Username:  "alice",
		Room:      "general",
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal leave message: %v", err)
	}
	if err := firstConn.WriteMessage(websocket.TextMessage, leavePayload); err != nil {
		t.Fatalf("write leave message: %v", err)
	}

	message := model.Message{
		Type:      "message",
		Username:  "bob",
		Content:   "hello after leave",
		Room:      "general",
		Timestamp: time.Now(),
	}
	payload, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal room message: %v", err)
	}
	if err := secondConn.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("write room message: %v", err)
	}

	firstConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err = firstConn.ReadMessage()
	if err == nil {
		t.Fatal("first websocket unexpectedly received a message after leaving the room")
	}
	if !strings.Contains(err.Error(), "i/o timeout") {
		t.Fatalf("expected timeout reading from first websocket after leave, got %v", err)
	}
}

func TestServeWsBroadcastsPresenceUpdatesForRoomMembers(t *testing.T) {
	resetHubForTests()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWs(w, r, newTestMessageRepository())
	}))
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

	joinPayload, err := json.Marshal(model.Message{
		Type:      "join",
		Username:  "alice",
		Room:      "general",
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal join message: %v", err)
	}
	if err := firstConn.WriteMessage(websocket.TextMessage, joinPayload); err != nil {
		t.Fatalf("write join message: %v", err)
	}

	firstConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, received, err := firstConn.ReadMessage()
	if err != nil {
		t.Fatalf("read presence update from first websocket: %v", err)
	}

	var presence model.Message
	if err := json.Unmarshal(received, &presence); err != nil {
		t.Fatalf("unmarshal presence payload: %v", err)
	}
	if presence.Type != "presence" {
		t.Fatalf("expected presence payload, got %q", presence.Type)
	}
	if presence.Username != "alice" {
		t.Fatalf("expected presence username alice, got %q", presence.Username)
	}
}
