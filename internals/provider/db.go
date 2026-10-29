package provider

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Pool *pgxpool.Pool
}

func (db *DB) Init(uri string) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		log.Fatalf("Failed to create database pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	fmt.Println("Database connected ...")

	db.Pool = pool
}
