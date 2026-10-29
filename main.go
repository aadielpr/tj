package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"tj/internals/provider"
	"tj/internals/util"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

const BROKER = "mqtt://localhost:1883"
const TOPIC = "fleet/vehicle/+/location"
const CLIENTID = "tj-test"

const RABBITMQ_GEOFENCE_QUEUE = "geofence_alerts"
const RABBITMQ_GEOFENCE_EXCHANGE = "fleet.events"
const RABBITMQ_GEOFENCE_ROUTE = "fleet.events.route"

const RABBITMQ_RADIUSCHECKER_QUEUE = "radius_checker"
const RABBITMQ_RADIUSCHECKER_EXCHANGE = "radius.check.exchange"
const RABBITMQ_RADIUSCHECKER_ROUTE = "radius.check.route"

type LatLong struct {
	Latitude  float64 `json:"latitude" validate:"required,latitude"`
	Longitude float64 `json:"longitude" validate:"required,longitude"`
}

type Location struct {
	VehicleID string  `json:"vehicle_id" validate:"required,alphanum"`
	Latitude  float64 `json:"latitude" validate:"required,latitude"`
	Longitude float64 `json:"longitude" validate:"required,longitude"`
	Timestamp int64   `json:"timestamp" validate:"required,unixtimestamp"`
}

type GetVehicleParam struct {
	VehicleID string `json:"vehicle_id" validate:"required,alphanum"`
	Start     int64  `json:"start" validate:"required,unixtimestamp"`
	End       int64  `json:"end" validate:"required,unixtimestamp"`
}

type Area struct {
	ID        int     `json:"ID"`
	AreaName  string  `json:"area_name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type GeofenceEventMessage struct {
	VehicleID string  `json:"vehicle_id"`
	Event     string  `json:"event"`
	Location  LatLong `json:"location"`
	Timestamp int64   `json:"timestamp"`
}

func main() {
	ctx := context.Background()

	app := fiber.New()
	validate := validator.New()
	err := validate.RegisterValidation("unixtimestamp", validateUnix)
	if err != nil {
		log.Fatalf("Failed to create database pool: %v", err)
		os.Exit(1)
	}

	db := provider.DB{}
	rmq := provider.RabbitMQ{}
	mqttx := provider.MQTT{}

	db.Init(ctx, "postgres://postgres:postgres@localhost:5432/tj?sslmode=disable")
	defer db.Pool.Close()

	rmq.Init()
	defer rmq.Connection.Close()
	defer rmq.Channel.Close()

	mqttx.Init(BROKER, CLIENTID)
	defer mqttx.Client.Disconnect(500)


	lch := make(chan Location, 25)
	go receiveLocationWorker(ctx, db.Pool, lch, rmq.Channel)

	subscribeToken := mqttx.Client.Subscribe(TOPIC, 1, func(c mqtt.Client, m mqtt.Message) {
		onReceivedLocation(m, lch, validate)
	})

	if subscribeToken.Wait() && subscribeToken.Error() != nil {
		log.Fatalf("On received location subscriber error: %v ", subscribeToken.Error())
	}

	// rabbitmq
	err = rmq.Channel.ExchangeDeclare(RABBITMQ_RADIUSCHECKER_EXCHANGE, "direct", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to create %v exchange %v", RABBITMQ_RADIUSCHECKER_EXCHANGE, err)
	}

	err = rmq.Channel.ExchangeDeclare(RABBITMQ_GEOFENCE_EXCHANGE, "direct", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to create %v exchange %v", RABBITMQ_GEOFENCE_EXCHANGE, err)
	}

	_, err = rmq.Channel.QueueDeclare(RABBITMQ_GEOFENCE_QUEUE, true, false, false, false,
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		log.Fatalf("Failed to create rabbitmq %v queue %v", RABBITMQ_GEOFENCE_QUEUE, err)
	}

	_, err = rmq.Channel.QueueDeclare(RABBITMQ_RADIUSCHECKER_QUEUE, true, false, false, false,
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		log.Fatalf("Failed to create rabbitmq %v queue %v", RABBITMQ_RADIUSCHECKER_QUEUE, err)
	}

	err = rmq.Channel.QueueBind(
		RABBITMQ_GEOFENCE_QUEUE,
		RABBITMQ_GEOFENCE_ROUTE,
		RABBITMQ_GEOFENCE_EXCHANGE,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind rabbitmq %v queue %v", RABBITMQ_GEOFENCE_QUEUE, err)
	}

	err = rmq.Channel.QueueBind(
		RABBITMQ_RADIUSCHECKER_QUEUE,
		RABBITMQ_RADIUSCHECKER_ROUTE,
		RABBITMQ_RADIUSCHECKER_EXCHANGE,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind rabbitmq %v queue %v", RABBITMQ_RADIUSCHECKER_QUEUE, err)
	}

	brwCh, err := rmq.Channel.Consume(RABBITMQ_RADIUSCHECKER_QUEUE, "", true, false, false, false, nil)
	gfwCh, err := rmq.Channel.Consume(RABBITMQ_GEOFENCE_QUEUE, "", true, false, false, false, nil)

	go checkBusRadiusWorker(brwCh, db.Pool, ctx, rmq.Channel)
	go geofenceAlertWorker(gfwCh, db.Pool, ctx)

	app.Get("vehicles/:vehicle_id/location", func(c fiber.Ctx) error {
		vehicleId := c.Params("vehicle_id")
		err := validate.Var(vehicleId, "required,alphanum")

		loc := Location{}
		err = db.Pool.QueryRow(ctx,
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
	})

	app.Get("vehicles/:vehicle_id/history", func(c fiber.Ctx) error {
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

		getVehicleParam := GetVehicleParam{
			VehicleID: c.Params("vehicle_id"),
			Start:     sUnix,
			End:       eUnix,
		}

		rows, err := db.Pool.Query(ctx,
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

		locations := []Location{}

		for rows.Next() {
			loc := Location{}
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
	})

	log.Fatal(app.Listen(":3000"))
}

func checkBusRadiusWorker(msgs <-chan amqp.Delivery, db *pgxpool.Pool, ctx context.Context, amqpCh *amqp.Channel) {
	for msg := range msgs {
		loc := Location{}

		err := json.Unmarshal(msg.Body, &loc)
		if err != nil {
			fmt.Printf("On received location unmarshal error (checkBusRadiusWorker): %v ", err.Error())
			return
		}

		rows, err := db.Query(ctx, `SELECT id, area_name, latitude, longitude FROM stop_locations`)
		if err != nil {
			fmt.Printf("Error when querying stop_locations %v \n", err)
			return
		}
		defer rows.Close()

		areas := make([]Area, 0)

		for rows.Next() {
			area := Area{}
			err := rows.Scan(&area.ID, &area.AreaName, &area.Latitude, &area.Longitude)
			if err != nil {
				fmt.Printf("Row scan error %v \n", err)
				return
			}

			areas = append(areas, area)
		}

		if err := rows.Err(); err != nil {
			fmt.Printf("Row scan error %v \n", err)
			return
		}

		for _, area := range areas {
			if util.IsWithinRadius(area.Latitude, area.Longitude, loc.Latitude, loc.Longitude, 50) {
				geofenceMessage := GeofenceEventMessage{
					VehicleID: loc.VehicleID,
					Event:     "geofence_entry",
					Location:  LatLong{Latitude: loc.Latitude, Longitude: loc.Longitude},
					Timestamp: loc.Timestamp,
				}

				geofenceMessageB, err := json.Marshal(geofenceMessage)
				if err != nil {
					fmt.Printf("Error on marshaling geofence message: %v", err)
					return
				}

				err = amqpCh.PublishWithContext(ctx, RABBITMQ_GEOFENCE_EXCHANGE, RABBITMQ_GEOFENCE_ROUTE, false, false,
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        geofenceMessageB,
					})
				if err != nil {
					fmt.Printf("Error on publish to radius checker: %v", err)
					return
				}
			}
		}
	}
}

func geofenceAlertWorker(msgs <-chan amqp.Delivery, db *pgxpool.Pool, ctx context.Context) {
	for msg := range msgs {
		fmt.Println("[GEOFENCE ALERT WORKER]")
		fmt.Println(string(msg.Body))
		fmt.Println()
	}
}

func saveLocation(ctx context.Context, db *pgxpool.Pool, l Location) error {
	_, err := db.Exec(ctx, `
		INSERT INTO 
			vehicle_locations(vehicle_id, latitude, longitude, timestamp)
		VALUES
			($1, $2, $3, $4)
	`, l.VehicleID, l.Latitude, l.Longitude, l.Timestamp)

	return err
}

func receiveLocationWorker(ctx context.Context, db *pgxpool.Pool, lch <-chan Location, amqpCh *amqp.Channel) {
	for location := range lch {
		if err := saveLocation(ctx, db, location); err != nil {
			fmt.Printf("Error on insert location: %v", err)
			return
		}

		locB, err := json.Marshal(location)
		if err != nil {
			fmt.Printf("Error on marshaling: %v", err)
			return
		}

		err = amqpCh.PublishWithContext(ctx, RABBITMQ_RADIUSCHECKER_EXCHANGE, RABBITMQ_RADIUSCHECKER_ROUTE, false, false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        locB,
			})
		if err != nil {
			fmt.Printf("Error on publish to radius checker: %v", err)
			return
		}
	}
}

func onReceivedLocation(m mqtt.Message, lch chan<- Location, validate *validator.Validate) {
	loc := Location{}

	err := json.Unmarshal(m.Payload(), &loc)
	if err != nil {
		fmt.Printf("On received loc unmarshal error: %v ", err.Error())
		return
	}

	err = validate.Struct(loc)
	if err != nil {
		fmt.Printf("On received loc unmarshal error: %v ", err.Error())
		return
	}

	fmt.Println("[RECEIVE LOCATION WORKER]")
	fmt.Println("topic: ", m.Topic())
	fmt.Println("vehicle_id: ", loc.VehicleID)
	fmt.Println("latitude: ", loc.Latitude)
	fmt.Println("longitude: ", loc.Longitude)
	fmt.Println("timestamp: ", loc.Timestamp)
	fmt.Print("\n")

	lch <- loc
}

func validateUnix(fl validator.FieldLevel) bool {
	val := fl.Field().Int()

	minTs := int64(0)
	maxTs := int64(1798736399)

	return val >= minTs && val <= maxTs
}
