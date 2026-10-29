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

func (db *DB) CreateTable() {
	ctx := context.Background()
	_, err := db.Pool.Exec(ctx,
		`
		CREATE TABLE IF NOT EXISTS vehicle_locations (
	    	id SERIAL PRIMARY KEY,
	    	vehicle_id VARCHAR(50) NOT NULL,
	    	latitude DOUBLE PRECISION NOT NULL,
	    	longitude DOUBLE PRECISION NOT NULL,
	    	timestamp BIGINT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS stop_locations (
    		id SERIAL PRIMARY KEY,
    		area_name VARCHAR(50) NOT NULL,
    		latitude DOUBLE PRECISION NOT NULL,
    		longitude DOUBLE PRECISION NOT NULL,
    		CONSTRAINT unique_area UNIQUE (area_name)
		);

		INSERT INTO stop_locations (area_name, latitude, longitude)
			VALUES
    		('Bundaran HI', -6.193605, 106.823016),
    		('Monas',       -6.175400, 106.827200),
    		('Blok M',      -6.243000, 106.800000)
    		ON CONFLICT ON CONSTRAINT unique_area DO NOTHING;
		`)

	if err != nil {
		fmt.Printf("Error on create table: %v", err)
	}
}
