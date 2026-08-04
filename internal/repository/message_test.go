package repository

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/etcha1/chat-server/internal/model"
	"github.com/jackc/pgx/v5"
	pgxconn "github.com/jackc/pgx/v5/pgconn"
)

type fakeQueryExecutor struct {
	queryRows pgx.Rows
	queryErr  error
	execErr   error
}

func (f *fakeQueryExecutor) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return f.queryRows, f.queryErr
}

func (f *fakeQueryExecutor) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return nil
}

func (f *fakeQueryExecutor) Exec(ctx context.Context, sql string, args ...any) (pgxconn.CommandTag, error) {
	return pgxconn.CommandTag{}, f.execErr
}

type fakeRows struct {
	rows   []model.Message
	err    error
	index  int
	ready  bool
}

func (f *fakeRows) Close() {}
func (f *fakeRows) Err() error { return f.err }
func (f *fakeRows) CommandTag() pgxconn.CommandTag { return pgxconn.CommandTag{} }
func (f *fakeRows) FieldDescriptions() []pgxconn.FieldDescription {
	return []pgxconn.FieldDescription{
		{Name: "id"},
		{Name: "username"},
		{Name: "content"},
		{Name: "timestamp"},
		{Name: "room"},
	}
}
func (f *fakeRows) Next() bool {
	if f.index >= len(f.rows) {
		return false
	}
	f.ready = true
	return true
}
func (f *fakeRows) Scan(dest ...any) error {
	if !f.ready || f.index >= len(f.rows) {
		return errors.New("no rows")
	}
	row := f.rows[f.index]
	f.index++
	f.ready = false
	for i, target := range dest {
		switch i {
		case 0:
			if v, ok := target.(*int); ok {
				*v = row.ID
			}
		case 1:
			if v, ok := target.(*string); ok {
				*v = row.Username
			}
		case 2:
			if v, ok := target.(*string); ok {
				*v = row.Content
			}
		case 3:
			if v, ok := target.(*time.Time); ok {
				*v = row.Timestamp
			}
		case 4:
			if v, ok := target.(*string); ok {
				*v = row.Room
			}
		}
	}
	return nil
}
func (f *fakeRows) Values() ([]any, error) { return nil, nil }
func (f *fakeRows) RawValues() [][]byte { return nil }
func (f *fakeRows) Conn() *pgx.Conn { return nil }

func TestGetMessagesReturnsRowsFromDatabase(t *testing.T) {
	msg := model.Message{Username: "alice", Content: "hello", Timestamp: time.Now(), Room: "general"}
	repo := NewMessageRepository(&fakeQueryExecutor{queryRows: &fakeRows{rows: []model.Message{msg}}})

	got, err := repo.GetMessages(context.Background(), "general")
	if err != nil {
		t.Fatalf("GetMessages returned unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 message, got %d", len(got))
	}
	if got[0].Content != msg.Content {
		t.Fatalf("expected content %q, got %q", msg.Content, got[0].Content)
	}
}

func TestGetMessagesReturnsErrorWhenQueryFails(t *testing.T) {
	repo := NewMessageRepository(&fakeQueryExecutor{queryErr: errors.New("boom")})

	_, err := repo.GetMessages(context.Background(), "general")
	if err == nil {
		t.Fatal("expected an error from GetMessages")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected wrapped query error, got %v", err)
	}
}

func TestCreateMessageReturnsErrorWhenExecFails(t *testing.T) {
	repo := NewMessageRepository(&fakeQueryExecutor{execErr: errors.New("insert failed")})
	msg := &model.Message{Username: "alice", Content: "hello", Timestamp: time.Now(), Room: "general"}

	err := repo.CreateMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error from CreateMessage")
	}
	if !strings.Contains(err.Error(), "createMessage") {
		t.Fatalf("expected wrapped createMessage error, got %v", err)
	}
}

func TestCreateMessageSucceeds(t *testing.T) {
	repo := NewMessageRepository(&fakeQueryExecutor{})
	msg := &model.Message{Username: "alice", Content: "hello", Timestamp: time.Now(), Room: "general"}

	if err := repo.CreateMessage(context.Background(), msg); err != nil {
		t.Fatalf("CreateMessage returned unexpected error: %v", err)
	}
}
