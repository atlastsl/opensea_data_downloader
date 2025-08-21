package downloader

import (
	"OpenSeaDataDownloader/helpers"
	"OpenSeaDataDownloader/utils"
	"fmt"
)

func ExportOperations(metaverse, source, metric string) {
	loggingPrefix := fmt.Sprintf("EXPORT DATA { %s | %s }", metaverse, source)
	helpers.Logging(loggingPrefix, "Start...")

	helpers.Logging(loggingPrefix, "Connection to database...")
	dbInstance, err := helpers.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer helpers.CloseDatabaseConnection(dbInstance)
	helpers.Logging(loggingPrefix, "Connected to database !!!")

	helpers.Logging(loggingPrefix, "Prepare additional data...")
	err = helpers.ReadCurrencyPrices(dbInstance)
	if err != nil {
		panic(err)
	}
	if metaverse == "decentraland" {
		err = helpers.GetDclFocalPoints(dbInstance)
		if err != nil {
			panic(err)
		}
	}
	helpers.Logging(loggingPrefix, "Additional data fetched !!!")

	helpers.Logging(loggingPrefix, "Get operations from database...")
	//longFields := []string{
	//	"transaction_hash", "order_hash", "order_id", "maker", "taker", "buyer", "seller", "payment_token",
	//	"asset_contract", "asset_id", "buyer_order_hash", "seller_order_hash", "block_hash",
	//}
	result, err := GetOperationsForExport(metaverse, source, metric, nil, dbInstance, loggingPrefix)
	if err != nil {
		panic(err)
	}
	helpers.Logging(loggingPrefix, "Operations retrieved from database")

	helpers.Logging(loggingPrefix, "Writing operations in file...")
	filename := fmt.Sprintf("./files/operations_test_%s_%s.csv", metaverse, source)
	err = utils.WriteInCsv2(filename, result.Operations, result.ColNames, result.ColTypes)
	if err != nil {
		panic(err)
	}
	helpers.Logging(loggingPrefix, "Operations saved in file !!!")
}
