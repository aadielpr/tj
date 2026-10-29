package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
	"tj/internals/provider"
	"tj/internals/util"
)

const TOPIC = "fleet/vehicle/{vehicle_id}/location"

func main() {
	mqttx := provider.MQTT{}

	mqttx.Init()
	defer mqttx.Client.Disconnect(500)

	data, err := os.ReadFile("input.json")
	if err != nil {
		panic(err)
	}

	input := []util.LocationJsonInput{}

	if err := json.Unmarshal(data, &input); err != nil {
		panic(err)
	}

	for {
		idx := rand.Intn(len(input))
		inp := input[idx]
		location := util.Location{
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
		publishToken := mqttx.Client.Publish(topic, 1, false, payload)
		if publishToken.Wait() && publishToken.Error() != nil {
			log.Fatalf("On publish location error: %v", publishToken.Error())
		}

		fmt.Printf("Published to %s: %s\n", TOPIC, payload)

		time.Sleep(2 * time.Second)
	}
}
