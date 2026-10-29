package util

import "math"

func IsWithinRadius(
	fixlat, fixlong float64,
	inlat, inlong float64,
	radius float64,
) bool {
	const earthRadius = 6371000.0 // meters

	toRadians := func(deg float64) float64 {
		return deg * math.Pi / 180
	}

	dLat := toRadians(inlat - fixlat)
	dLon := toRadians(inlong - fixlong)

	lat1Rad := toRadians(fixlat)
	lat2Rad := toRadians(inlat)

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(dLon/2)*math.Sin(dLon/2)

	a = math.Max(0, math.Min(1, a))

	distance := 2 * earthRadius * math.Asin(math.Sqrt(a))

	return distance <= radius
}
