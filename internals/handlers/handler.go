package handler

import (
	"fmt"
	"log"

	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler struct {
	db       *pgxpool.Pool
	validate *validator.Validate
	app      *fiber.App
	port     string
}

func Init(db *pgxpool.Pool, validate *validator.Validate, port string) *Handler {
	app := fiber.New()

	return &Handler{db, validate, app, port}
}

func (h *Handler) InitRoute() {
	h.app.Get("vehicles/:vehicle_id/location", h.GetVehicleLatestLocation)
	h.app.Get("vehicles/:vehicle_id/history", h.GetVehicleHistory)
}

func (h *Handler) Listen() {
	log.Fatal(h.app.Listen(fmt.Sprintf(":%s", h.port)))
}
