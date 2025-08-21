package helpers

import (
	"OpenSeaDataDownloader/utils"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type DecentralandFPParcelInfo struct {
	X       int8   `bson:"x"`
	Y       int8   `bson:"y"`
	NftId   string `bson:"nftId"`
	TokenId string `bson:"tokenId"`
}

type DecentralandFocalPoint struct {
	mgm.DefaultModel `bson:",inline,omitempty"`
	FocalPointId     string                      `bson:"focal_point_id,omitempty"`
	FocalPointType   string                      `bson:"focal_point_type,omitempty"`
	EstateId         string                      `bson:"estate_id,omitempty"`
	DclId            string                      `bson:"dcl_id,omitempty"`
	Name             string                      `bson:"name,omitempty"`
	Description      string                      `bson:"description,omitempty"`
	ParcelsLoc       []string                    `bson:"parcels_loc,omitempty"`
	ParcelsCount     int                         `bson:"parcels_count,omitempty"`
	Parcels          []*DecentralandFPParcelInfo `bson:"parcels,omitempty"`
	Category         string                      `bson:"category,omitempty"`
}

var (
	dclPlazas               = make([]*DecentralandFocalPoint, 0)
	dclRoads                = make([]*DecentralandFocalPoint, 0)
	dclDistricts            = make([]*DecentralandFocalPoint, 0)
	dclDisCategories        = make([]string, 0)
	dclSmallDistrictMaxSize = 100
)

func getDclFocalPointsOfType(fpType string, dbInstance *mongo.Database) ([]*DecentralandFocalPoint, error) {
	dbCollection := CollectionInstance(dbInstance, &DecentralandFocalPoint{})
	cursor, err := dbCollection.Find(context.Background(), bson.M{"focal_point_type": fpType})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	dclFocalPoints := make([]*DecentralandFocalPoint, 0)
	err = cursor.All(context.Background(), &dclFocalPoints)
	if err != nil {
		return nil, err
	}
	return dclFocalPoints, nil
}

func GetDclFocalPoints(dbInstance *mongo.Database) error {
	var err error
	dclPlazas, err = getDclFocalPointsOfType("plaza", dbInstance)
	if err != nil {
		return err
	}
	dclRoads, err = getDclFocalPointsOfType("road", dbInstance)
	if err != nil {
		return err
	}
	dclDistricts, err = getDclFocalPointsOfType("district", dbInstance)
	if err != nil {
		return err
	}
	for _, district := range dclDistricts {
		if !slices.Contains(dclDisCategories, strings.Split(district.Category, " ")[0]) {
			dclDisCategories = append(dclDisCategories, strings.Split(district.Category, " ")[0])
		}
	}
	return nil
}

func GetDclDistanceToFocalPoints(parcelX, parcelY int, metric string) map[string]float64 {
	distances := make(map[string]float64)

	distanceMin := math.MaxFloat64
	for _, plaza := range dclPlazas {
		plazaDis := utils.Distance1Point1ZoneLoc(parcelX, parcelY, plaza.ParcelsLoc, metric)
		key := fmt.Sprintf("DIS__PLAZA__%s", strings.ToUpper(plaza.DclId))
		distances[key] = plazaDis
		if plazaDis < distanceMin {
			distanceMin = plazaDis
		}
	}
	distances["DIS__PLAZA"] = distanceMin

	distanceMin = math.MaxFloat64
	for _, road := range dclRoads {
		roadDis := utils.Distance1Point1ZoneLoc(parcelX, parcelY, road.ParcelsLoc, metric)
		if roadDis < distanceMin {
			distanceMin = roadDis
		}
	}
	distances["DIS__ROAD"] = distanceMin

	distCatDistancesMap := map[string]float64{}
	for _, category := range dclDisCategories {
		distCatDistancesMap[strings.ToUpper(category)] = math.MaxFloat64
	}
	distanceMin = math.MaxFloat64
	for _, district := range dclDistricts {
		districtDis := utils.Distance1Point1ZoneLoc(parcelX, parcelY, district.ParcelsLoc, metric)
		key := fmt.Sprintf("DIS__DISTRICT__%s", strings.ToUpper(district.DclId))
		distances[key] = districtDis
		if district.ParcelsCount > dclSmallDistrictMaxSize {
			distCatKey := strings.ToUpper(strings.Split(district.Category, " ")[0])
			if districtDis < distCatDistancesMap[distCatKey] {
				distCatDistancesMap[distCatKey] = districtDis
			}
		}
		if districtDis < distanceMin {
			distanceMin = districtDis
		}
	}
	for category, distance := range distCatDistancesMap {
		key := fmt.Sprintf("DIS__DISTCAT__%s", category)
		distances[key] = distance
	}
	distances["DIS__DISTRICT"] = distanceMin

	return distances
}

func GetDclDistanceToFocalPointsHT() (h []string, t []string) {
	h = make([]string, 0)
	t = make([]string, 0)

	for _, plaza := range dclPlazas {
		key := fmt.Sprintf("DIS__PLAZA__%s", strings.ToUpper(plaza.DclId))
		h = append(h, key)
		t = append(t, "float64")
	}
	h = append(h, "DIS__PLAZA")
	t = append(t, "float64")

	h = append(h, "DIS__ROAD")
	t = append(t, "float64")

	distCatDistancesMap := map[string]float64{}
	for _, category := range dclDisCategories {
		distCatDistancesMap[strings.ToUpper(category)] = math.MaxFloat64
	}
	for _, district := range dclDistricts {
		key := fmt.Sprintf("DIS__DISTRICT__%s", strings.ToUpper(district.DclId))
		h = append(h, key)
		t = append(t, "float64")
	}
	for category, _ := range distCatDistancesMap {
		key := fmt.Sprintf("DIS__DISTCAT__%s", category)
		h = append(h, key)
		t = append(t, "float64")
	}
	h = append(h, "DIS__DISTRICT")
	t = append(t, "float64")

	return h, t
}
