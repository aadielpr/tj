package main

import (
	handler "tj/internals/handlers"
	"tj/internals/messaging"
	"tj/internals/provider"
)

func main() {
	validator := provider.Validator{}
	db := provider.DB{}
	rmq := provider.RabbitMQ{}
	mqttx := provider.MQTT{}

	validator.Init()

	db.Init("postgres://postgres:postgres@localhost:5432/tj?sslmode=disable")
	defer db.Pool.Close()

	rmq.Init()
	defer rmq.Connection.Close()
	defer rmq.Channel.Close()

	mqttx.Init()
	defer mqttx.Client.Disconnect(500)

	messaging := messaging.Init(db.Pool, rmq, mqttx, validator.Validate)
	go messaging.SaveLocationWorker()
	go messaging.BusRadiusWorker()
	go messaging.GeofenceAlertWorker()
	go messaging.ReceiveLocationWorker()

	handler := handler.Init(db.Pool, validator.Validate)
	handler.InitRoute()

	handler.Listen()
}
