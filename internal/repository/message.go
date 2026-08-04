package repository

import (
	"context"
	"fmt"

	"github.com/etcha1/chat-server/internal/model"
	"github.com/jackc/pgx/v5"
	pgxconn "github.com/jackc/pgx/v5/pgconn"
)

type queryExecutor interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgxconn.CommandTag, error)
}

// MessageRepository handles database operations for Messages.
type MessageRepository struct {
	db queryExecutor
}

// NewMessageRepository acts as a constructor to inject the DB connection.
func NewMessageRepository(db queryExecutor) *MessageRepository {
	return &MessageRepository{db: db}
}

func (mr *MessageRepository) GetMessages(ctx context.Context, room string) ([]model.Message, error) {
	rows, err := mr.db.Query(ctx, "SELECT * FROM messages WHERE room = $1", room)
	if err != nil {
		return nil, fmt.Errorf("getMessages: %w", err)
	}
	defer rows.Close()

	messages, err := pgx.CollectRows(rows, pgx.RowToStructByName[model.Message])
	if err != nil {
		return nil, fmt.Errorf("getMessages: %w", err)
	}

	return messages, nil
}

func (mr *MessageRepository) CreateMessage(ctx context.Context, message *model.Message) error {
	_, err := mr.db.Exec(ctx, "INSERT INTO messages (username, content, timestamp, room) VALUES ($1, $2, $3, $4)",
		message.Username, message.Content, message.Timestamp, message.Room)
	if err != nil {
		return fmt.Errorf("createMessage: %w", err)
	}
	return nil
}
