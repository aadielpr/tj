package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"tj/internals/util"

	amqp "github.com/rabbitmq/amqp091-go"
)

const RABBITMQ_GEOFENCE_QUEUE = "geofence_alerts"
const RABBITMQ_GEOFENCE_EXCHANGE = "fleet.events"
const RABBITMQ_GEOFENCE_ROUTE = "fleet.events.route"

const RABBITMQ_RADIUSCHECKER_QUEUE = "radius_checker"
const RABBITMQ_RADIUSCHECKER_EXCHANGE = "radius.check.exchange"
const RABBITMQ_RADIUSCHECKER_ROUTE = "radius.check.route"

type RabbitMQ struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
}

func (rmq *RabbitMQ) Init(uri string) {
	con, err := amqp.Dial(uri)
	if err != nil {
		log.Fatalf("Failed to connect to rabbitmq: %v", err)
	}
	rmq.Connection = con

	ch, err := con.Channel()
	if err != nil {
		log.Fatalf("Failed to open rabbitmq channel: %v", err)
	}
	rmq.Channel = ch

	rmq.declareExchange()

	fmt.Println("RabbitMQ connected..")
}

func (rmq *RabbitMQ) declareExchange() {
	err := rmq.Channel.ExchangeDeclare(
		RABBITMQ_RADIUSCHECKER_EXCHANGE,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to create %v exchange %v", RABBITMQ_RADIUSCHECKER_EXCHANGE, err)
	}

	err = rmq.Channel.ExchangeDeclare(
		RABBITMQ_GEOFENCE_EXCHANGE,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to create %v exchange %v", RABBITMQ_GEOFENCE_EXCHANGE, err)
	}

	_, err = rmq.Channel.QueueDeclare(
		RABBITMQ_GEOFENCE_QUEUE,
		true,
		false,
		false,
		false,
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		log.Fatalf("Failed to create rabbitmq %v queue %v", RABBITMQ_GEOFENCE_QUEUE, err)
	}

	_, err = rmq.Channel.QueueDeclare(
		RABBITMQ_RADIUSCHECKER_QUEUE,
		true,
		false,
		false,
		false,
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
}

func (rmq *RabbitMQ) RadiusCheckerConsumerChannel() <-chan amqp.Delivery {
	ch, err := rmq.Channel.Consume(RABBITMQ_RADIUSCHECKER_QUEUE, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to get consumer for %v queue %v", RABBITMQ_RADIUSCHECKER_QUEUE, err)
	}

	return ch
}

func (rmq *RabbitMQ) GeofenceAlertConsumerChannel() <-chan amqp.Delivery {
	ch, err := rmq.Channel.Consume(RABBITMQ_GEOFENCE_QUEUE, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to get consumer for %v queue %v", RABBITMQ_GEOFENCE_QUEUE, err)
	}

	return ch
}

func (rmq *RabbitMQ) RadiusCheckerPublisher(ctx context.Context, loc util.Location) {
	byte, err := json.Marshal(loc)
	if err != nil {
		fmt.Printf("Radius checker publisher marshal error: %v", err)
		return
	}

	err = rmq.Channel.PublishWithContext(
		ctx,
		RABBITMQ_RADIUSCHECKER_EXCHANGE,
		RABBITMQ_RADIUSCHECKER_ROUTE,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        byte,
		},
	)
	if err != nil {
		fmt.Printf("Error on publish to radius checker: %v", err)
		return
	}
}

func (rmq *RabbitMQ) GeoFenceAlertPublisher(
	ctx context.Context,
	geofenceEvent util.GeofenceEventMessage,
) {
	geofenceMessageB, err := json.Marshal(geofenceEvent)
	if err != nil {
		fmt.Printf("Error on marshaling geofence message: %v", err)
		return
	}

	err = rmq.Channel.PublishWithContext(
		ctx,
		RABBITMQ_GEOFENCE_EXCHANGE,
		RABBITMQ_GEOFENCE_ROUTE,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        geofenceMessageB,
		},
	)
	if err != nil {
		fmt.Printf("Error on publish to radius checker: %v", err)
		return
	}
}
