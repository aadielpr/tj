package provider

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
}

func (rmq *RabbitMQ) Init() {
	con, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to rabbitmq: %v", err)
	}
	rmq.Connection = con

	ch, err := con.Channel()
	if err != nil {
		log.Fatalf("Failed to open rabbitmq channel: %v", err)
	}
	rmq.Channel = ch

	fmt.Println("RabbitMQ connected..")
}

func (rmq *RabbitMQ) declareExchange() {}
