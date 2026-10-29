package util

type LatLong struct {
	Latitude  float64 `json:"latitude" validate:"required,latitude"`
	Longitude float64 `json:"longitude" validate:"required,longitude"`
}

type Location struct {
	VehicleID string  `json:"vehicle_id" validate:"required,alphanum"`
	Latitude  float64 `json:"latitude" validate:"required,latitude"`
	Longitude float64 `json:"longitude" validate:"required,longitude"`
	Timestamp int64   `json:"timestamp" validate:"required,unixtimestamp"`
}

type Area struct {
	ID        int     `json:"ID"`
	AreaName  string  `json:"area_name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type GeofenceEventMessage struct {
	VehicleID string  `json:"vehicle_id"`
	Event     string  `json:"event"`
	Location  LatLong `json:"location"`
	Timestamp int64   `json:"timestamp"`
}

type GetVehicleParam struct {
	VehicleID string `json:"vehicle_id" validate:"required,alphanum"`
	Start     int64  `json:"start" validate:"required,unixtimestamp"`
	End       int64  `json:"end" validate:"required,unixtimestamp"`
}

type LocationJsonInput struct {
	VehicleID string  `json:"vehicle_id"`
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"long"`
}

