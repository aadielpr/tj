package handler

import (
	"log"

	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler struct {
	db       *pgxpool.Pool
	validate *validator.Validate
	app      *fiber.App
}

func Init(db *pgxpool.Pool, validate *validator.Validate) *Handler {
	app := fiber.New()

	return &Handler{db, validate, app}
}

func (h *Handler) InitRoute() {
	h.app.Get("vehicles/:vehicle_id/location", h.GetVehicleLatestLocation)
	h.app.Get("vehicles/:vehicle_id/history", h.GetVehicleHistory)
}

func (h *Handler) Listen() {
	log.Fatal(h.app.Listen(":3000"))
}
