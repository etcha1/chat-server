package database

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
)

func GetConnection() *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Printf("Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	pingErr := conn.Ping(context.Background())
	if pingErr != nil {
		log.Fatal(pingErr)
	}
	log.Println("Connected to database!")

	return conn
}
