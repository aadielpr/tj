package handler

import (
	"errors"
	"strconv"
	"tj/internals/util"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
)

func (h *Handler) GetVehicleLatestLocation(c fiber.Ctx) error {
	vehicleId := c.Params("vehicle_id")
	err := h.validate.Var(vehicleId, "required,alphanum")

	loc := util.Location{}
	err = h.db.QueryRow(c,
		`	SELECT vehicle_id, latitude, longitude, timestamp FROM vehicle_locations 
				WHERE vehicle_id = $1
				ORDER BY timestamp DESC
				LIMIT 1
			`, vehicleId).Scan(&loc.VehicleID, &loc.Latitude, &loc.Longitude, &loc.Timestamp)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"ok":         false,
				"statusCode": fiber.StatusBadRequest,
				"message":    "Location not found",
			})
		}

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"ok":         false,
			"statusCode": fiber.StatusInternalServerError,
			"message":    "Internal Server Error",
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"ok":         true,
		"statusCode": fiber.StatusOK,
		"data":       fiber.Map{"location": loc},
	})
}

func (h *Handler) GetVehicleHistory(c fiber.Ctx) error {
	sUnix, err := strconv.ParseInt(c.Query("start"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"ok":         false,
			"statusCode": fiber.StatusBadRequest,
			"message":    "start param is not valid",
		})
	}

	eUnix, err := strconv.ParseInt(c.Query("end"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"ok":         false,
			"statusCode": fiber.StatusBadRequest,
			"message":    "end param is not valid",
		})
	}

	getVehicleParam := util.GetVehicleParam{
		VehicleID: c.Params("vehicle_id"),
		Start:     sUnix,
		End:       eUnix,
	}

	err = h.validate.Struct(getVehicleParam)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"ok":         false,
			"statusCode": fiber.StatusBadRequest,
			"message":    err.Error(),
		})
	}

	rows, err := h.db.Query(c,
		`	SELECT vehicle_id, latitude, longitude, timestamp FROM vehicle_locations 
				WHERE vehicle_id = $1 AND timestamp >= $2 AND timestamp <= $3
			`,
		getVehicleParam.VehicleID, getVehicleParam.Start, getVehicleParam.End)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"ok":         false,
			"statusCode": fiber.StatusInternalServerError,
			"message":    "Internal Server Error",
			"detail":     err.Error(),
		})
	}
	defer rows.Close()

	locations := []util.Location{}

	for rows.Next() {
		loc := util.Location{}
		err := rows.Scan(&loc.VehicleID, &loc.Latitude, &loc.Longitude, &loc.Timestamp)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"ok":         false,
				"statusCode": fiber.StatusInternalServerError,
				"message":    "Internal Server Error",
				"detail":     err.Error(),
			})
		}

		locations = append(locations, loc)
	}

	if err := rows.Err(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"ok":         false,
			"statusCode": fiber.StatusInternalServerError,
			"message":    "Internal Server Error",
			"detail":     err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"ok":         true,
		"statusCode": fiber.StatusOK,
		"locations":  locations,
	})
}
