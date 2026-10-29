package provider

import (
	"fmt"
	"log"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const BROKER = "mqtt://localhost:1883"
const CLIENT_ID = "tj-test"

type MQTT struct {
	Client mqtt.Client
}

func (mqttx *MQTT) Init() {
	opt := mqtt.NewClientOptions()
	opt.AddBroker(BROKER)
	opt.SetClientID(CLIENT_ID)
	opt.SetAutoReconnect(true)

	opt.OnConnect = func(c mqtt.Client) {
		fmt.Println("MQTT connected..")
	}
	opt.OnConnectionLost = func(c mqtt.Client, err error) {
		fmt.Printf("MQTT disconnected.. %v \n", err)
	}

	cl := mqtt.NewClient(opt)
	token := cl.Connect()
	if token.Wait() && token.Error() != nil {
		log.Fatalf("MQTT token error: %v ", token.Error())
	}

	mqttx.Client = cl
}
