package helpers

import (
	"OpenSeaDataDownloader/utils"
	"fmt"
	"path/filepath"
	"time"
)

type DecentralandParcel struct {
	Id      string `mapstructure:"id"`
	X       int    `mapstructure:"x"`
	Y       int    `mapstructure:"y"`
	TokenId string `mapstructure:"tokenId"`
}

type DecentralandParcelList struct {
	Ok   bool                           `mapstructure:"ok"`
	Data map[string]*DecentralandParcel `mapstructure:"data"`
}

func ReadDecentralandParcels() map[string]*DecentralandParcel {
	filePath := filepath.Join("events", "data", "decentraland_parcels.json")
	resp := &DecentralandParcelList{}
	err := utils.ReadJsonFile(filePath, resp)
	if err != nil {
		panic(err)
	}
	parcelsList := make(map[string]*DecentralandParcel)
	for _, parcel := range resp.Data {
		parcelsList[parcel.TokenId] = parcel
	}
	return parcelsList
}

func Logging(loggingPrefix, line string) {
	println(fmt.Sprintf("[%s] // (Opensea DL) %s // %s", time.Now().Format(time.RFC3339), loggingPrefix, line))
}
