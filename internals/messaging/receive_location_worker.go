package messaging

import (
	"encoding/json"
	"fmt"
	"tj/internals/util"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const TOPIC = "fleet/vehicle/+/location"

func (mssg *Messaging) ReceiveLocationWorker() {
	subscribeToken := mssg.mqttx.Client.Subscribe(TOPIC, 1, func(c mqtt.Client, m mqtt.Message) {
		loc := util.Location{}

		err := json.Unmarshal(m.Payload(), &loc)
		if err != nil {
			fmt.Printf("On received loc unmarshal error: %v ", err.Error())
			return
		}

		err = mssg.validate.Struct(loc)
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

		mssg.lch <- loc
	})

	if subscribeToken.Wait() && subscribeToken.Error() != nil {
		fmt.Printf("On received location subscriber error: %v ", subscribeToken.Error())
	}
}
