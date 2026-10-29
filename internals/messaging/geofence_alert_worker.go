package messaging

import "fmt"

func (mssg *Messaging) GeofenceAlertWorker() {
	ch := mssg.rmq.GeofenceAlertConsumerChannel()
	for msg := range ch {
		fmt.Println("[GEOFENCE ALERT WORKER]")
		fmt.Println(string(msg.Body))
		fmt.Println()
	}
}
