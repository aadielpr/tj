package messaging

import (
	"context"
	"fmt"
)

func (mssg *Messaging) SaveLocationWorker() {
	ctx := context.Background()
	for loc := range mssg.lch {
		_, err := mssg.db.Exec(ctx,
			`
			INSERT INTO 
				vehicle_locations(vehicle_id, latitude, longitude, timestamp)
			VALUES
				($1, $2, $3, $4)
		`, loc.VehicleID, loc.Latitude, loc.Longitude, loc.Timestamp)

		if err != nil {
			fmt.Printf("Error on insert location: %v", err)
		}

		mssg.rmq.RadiusCheckerPublisher(ctx, loc)
	}
}
