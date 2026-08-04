package model

import "time"

type Message struct {
	ID        int       `db:"id" json:"id"`
	Type      string    `db:"-" json:"type"`
	Username  string    `db:"username" json:"username"`
	Content   string    `db:"content" json:"content"`
	Timestamp time.Time `db:"timestamp" json:"timestamp"`
	Room      string    `db:"room" json:"room"`
}
