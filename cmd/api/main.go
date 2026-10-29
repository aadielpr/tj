package main

import (
	"log"
	"os"
	handler "tj/internals/handlers"
	"tj/internals/messaging"
	"tj/internals/provider"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Warning: .env file not found")
	}

	validator := provider.Validator{}
	db := provider.DB{}
	rmq := provider.RabbitMQ{}
	mqttx := provider.MQTT{}

	validator.Init()

	db.Init(os.Getenv("PSQL_URI"))
	defer db.Pool.Close()

	rmq.Init(os.Getenv("RABBIT_URI"))
	defer rmq.Connection.Close()
	defer rmq.Channel.Close()

	mqttx.Init(os.Getenv("MQTT_URI"))
	defer mqttx.Client.Disconnect(500)

	messaging := messaging.Init(db.Pool, rmq, mqttx, validator.Validate)
	go messaging.SaveLocationWorker()
	go messaging.BusRadiusWorker()
	go messaging.GeofenceAlertWorker()
	go messaging.ReceiveLocationWorker()

	handler := handler.Init(db.Pool, validator.Validate, os.Getenv("PORT"))
	handler.InitRoute()

	handler.Listen()
}
