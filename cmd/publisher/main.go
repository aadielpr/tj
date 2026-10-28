package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const BROKER = "mqtt://localhost:1883"
const TOPIC = "fleet/vehicle/{vehicle_id}/location"
const CLIENTID = "tj-location-publisher"

type Input struct {
	VehicleID string  `json:"vehicle_id"`
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"long"`
}

type Location struct {
	VehicleID string  `json:"vehicle_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timestamp int64   `json:"timestamp"`
}

func main() {
	opt := mqtt.NewClientOptions()
	opt.AddBroker(BROKER)
	opt.SetClientID(CLIENTID)
	opt.SetAutoReconnect(true)

	opt.OnConnect = func(c mqtt.Client) {
		fmt.Println("MQTT connected..")
	}
	opt.OnConnectionLost = func(c mqtt.Client, err error) {
		fmt.Printf("MQTT disconnected.. %v \n", err)
	}

	client := mqtt.NewClient(opt)
	clientToken := client.Connect()
	if clientToken.Wait() && clientToken.Error() != nil {
		panic(clientToken.Error())
	}
	defer client.Disconnect(500)

	data, err := os.ReadFile("input.json")
	if err != nil {
		panic(err)
	}

	input := []Input{}

	if err := json.Unmarshal(data, &input); err != nil {
		panic(err)
	}

	for {
		idx := rand.Intn(len(input))
		inp := input[idx]
		location := Location{
			VehicleID: inp.VehicleID,
			Latitude:  inp.Latitude,
			Longitude: inp.Longitude,
			Timestamp: time.Now().Unix(),
		}

		payload, err := json.Marshal(location)
		if err != nil {
			log.Fatalf("On marshal location error: %v", err)
		}

		topic := strings.ReplaceAll(TOPIC, "{vehicle_id}", inp.VehicleID)
		publishToken := client.Publish(topic, 1, false, payload)
		if publishToken.Wait() && publishToken.Error() != nil {
			log.Fatalf("On publish location error: %v", publishToken.Error())
		}

		fmt.Printf("Published to %s: %s\n", TOPIC, payload)

		time.Sleep(2 * time.Second)
	}
}
