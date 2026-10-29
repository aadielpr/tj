package messaging

import (
	"tj/internals/provider"
	"tj/internals/util"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Messaging struct {
	db       *pgxpool.Pool
	lch      chan util.Location
	rmq      provider.RabbitMQ
	validate *validator.Validate
	mqttx    provider.MQTT
}

func Init(db *pgxpool.Pool, rmq provider.RabbitMQ, mqttx provider.MQTT, validate *validator.Validate) *Messaging {
	return &Messaging{
		db:       db,
		lch:      make(chan util.Location, 25),
		rmq:      rmq,
		validate: validate,
		mqttx:    mqttx,
	}
}
