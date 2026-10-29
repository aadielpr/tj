package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"tj/internals/util"
)

func (mssg *Messaging) BusRadiusWorker() {
	ctx := context.Background()
	ch := mssg.rmq.RadiusCheckerConsumerChannel()

	for msg := range ch {
		loc := util.Location{}

		err := json.Unmarshal(msg.Body, &loc)
		if err != nil {
			fmt.Printf("On received location unmarshal error (checkBusRadiusWorker): %v ", err.Error())
			return
		}

		rows, err := mssg.db.Query(ctx, `SELECT id, area_name, latitude, longitude FROM stop_locations`)
		if err != nil {
			fmt.Printf("Error when querying stop_locations %v \n", err)
			return
		}
		defer rows.Close()

		areas := make([]util.Area, 0)

		for rows.Next() {
			area := util.Area{}
			err := rows.Scan(&area.ID, &area.AreaName, &area.Latitude, &area.Longitude)
			if err != nil {
				fmt.Printf("Row scan error %v \n", err)
				return
			}

			areas = append(areas, area)
		}

		if err := rows.Err(); err != nil {
			fmt.Printf("Row scan error %v \n", err)
			return
		}

		for _, area := range areas {
			if util.IsWithinRadius(area.Latitude, area.Longitude, loc.Latitude, loc.Longitude, 50) {
				geofenceEvent := util.GeofenceEventMessage{
					VehicleID: loc.VehicleID,
					Event:     "geofence_entry",
					Location:  util.LatLong{Latitude: loc.Latitude, Longitude: loc.Longitude},
					Timestamp: loc.Timestamp,
				}

				mssg.rmq.GeoFenceAlertPublisher(ctx, geofenceEvent)
			}
		}
	}
}
