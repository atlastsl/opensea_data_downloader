package utils

import (
	"math"
	"slices"
	"strconv"
	"strings"
)

func euclideanDistance(x1, y1, x2, y2 int) float64 {
	return math.Pow(math.Pow(float64(x1)-float64(x2), 2)+math.Pow(float64(y1)-float64(y2), 2), 0.5)
}

func manhattanDistance(x1, y1, x2, y2 int) int {
	return int(math.Abs(float64(x1-x2)) + math.Abs(float64(y1-y2)))
}

func Distance2Points(x1, y1, x2, y2 int, metric string) float64 {
	if metric == "euclidean" {
		return euclideanDistance(x1, y1, x2, y2)
	} else if metric == "manhattan" {
		return float64(manhattanDistance(x1, y1, x2, y2))
	}
	return 0.0
}

func Distance1Point1Zone(x1, y1 int, zoneXY [][]int, metric string) float64 {
	distances := ArrayMap(zoneXY, func(point2 []int) (bool, float64) {
		return true, Distance2Points(x1, y1, point2[0], point2[1], metric)
	}, true, 0.0)
	slices.Sort(distances)
	return distances[0]
}

func Distance1Point1ZoneLoc(x1, y1 int, zoneXYLoc []string, metric string) float64 {
	zoneXY := ArrayMap(zoneXYLoc, func(p2Loc string) (bool, []int) {
		coords := strings.Split(p2Loc, ",")
		p2X, _ := strconv.Atoi(coords[0])
		p2Y, _ := strconv.Atoi(coords[1])
		return true, []int{p2X, p2Y}
	}, true, nil)
	return Distance1Point1Zone(x1, y1, zoneXY, metric)
}
