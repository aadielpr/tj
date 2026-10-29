package provider

import (
	"fmt"
	"log"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MQTT struct {
	Client mqtt.Client
}

func (mqttx *MQTT) Init(broker string, clientId string) {
	opt := mqtt.NewClientOptions()
	opt.AddBroker(broker)
	opt.SetClientID(clientId)
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
