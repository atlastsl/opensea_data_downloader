package downloader

import (
	"OpenSeaDataDownloader/helpers"
	"OpenSeaDataDownloader/utils"
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SecondMarketOperation struct {
	mgm.DefaultModel  `bson:",inline"`
	OperationId       string     `bson:"operation_id" json:"operation_id" mapstructure:"operation_id"`
	DownloadedFrom    string     `bson:"downloaded_from" json:"downloaded_from" mapstructure:"downloaded_from"`
	Type              string     `bson:"type" json:"type" mapstructure:"type"`
	Source            string     `bson:"source" json:"source" mapstructure:"source"`
	LastUpdatedAt     *time.Time `bson:"last_updated_at,omitempty" json:"last_updated_at" mapstructure:"last_updated_at"`
	Date              *time.Time `bson:"date" json:"date" mapstructure:"date"`
	Metaverse         string     `bson:"metaverse,omitempty" json:"metaverse" mapstructure:"metaverse"`
	Blockchain        string     `bson:"blockchain,omitempty" json:"blockchain" mapstructure:"blockchain"`
	Cursor            string     `bson:"cursor,omitempty" json:"cursor" mapstructure:"cursor"`
	Reverted          bool       `bson:"reverted,omitempty" json:"reverted" mapstructure:"reverted"`
	OrderId           string     `bson:"order_id,omitempty" json:"order_id" mapstructure:"order_id"`
	OrderHash         string     `bson:"order_hash,omitempty" json:"order_hash" mapstructure:"order_hash"`
	TransactionHash   string     `bson:"transaction_hash,omitempty" json:"transaction_hash" mapstructure:"transaction_hash"`
	TransactionType   string     `bson:"transaction_type,omitempty" json:"transaction_type" mapstructure:"transaction_type"`
	Maker             string     `bson:"maker,omitempty" json:"maker" mapstructure:"maker"`
	Taker             string     `bson:"taker,omitempty" json:"taker" mapstructure:"taker"`
	Buyer             string     `bson:"buyer,omitempty" json:"buyer" mapstructure:"buyer"`
	Seller            string     `bson:"seller,omitempty" json:"seller" mapstructure:"seller"`
	AssetContract     string     `bson:"asset_contract,omitempty" json:"asset_contract" mapstructure:"asset_contract"`
	AssetType         string     `bson:"asset_type,omitempty" json:"asset_type" mapstructure:"asset_type"`
	AssetId           string     `bson:"asset_id,omitempty" json:"asset_id" mapstructure:"asset_id"`
	AssetLocation     string     `bson:"asset_location,omitempty" json:"asset_location" mapstructure:"asset_location"`
	AssetLocX         *int       `bson:"asset_loc_x" json:"asset_loc_x" mapstructure:"asset_loc_x"`
	AssetLocY         *int       `bson:"asset_loc_y" json:"asset_loc_y" mapstructure:"asset_loc_y"`
	AssetValue        int        `bson:"asset_value,omitempty" json:"asset_value" mapstructure:"asset_value"`
	PaymentBlockchain string     `bson:"payment_blockchain,omitempty" json:"payment_blockchain" mapstructure:"payment_blockchain"`
	PaymentType       string     `bson:"payment_type,omitempty" json:"payment_type" mapstructure:"payment_type"`
	PaymentToken      string     `bson:"payment_token,omitempty" json:"payment_token" mapstructure:"payment_token"`
	PaymentCurrency   string     `bson:"payment_currency,omitempty" json:"payment_currency" mapstructure:"payment_currency"`
	PaymentAmount     float64    `bson:"payment_amount,omitempty" json:"payment_amount" mapstructure:"operation_id"`
	PaymentAmountUsd  float64    `bson:"payment_amount_usd,omitempty" json:"payment_amount_usd" mapstructure:"payment_amount_usd"`
	PaymentCcyPrice   float64    `bson:"payment_ccy_price,omitempty" json:"payment_ccy_price" mapstructure:"payment_ccy_price"`
	BuyerOrderHash    string     `bson:"buyer_order_hash,omitempty" json:"buyer_order_hash" mapstructure:"buyer_order_hash"`
	SellerOrderHash   string     `bson:"seller_order_hash,omitempty" json:"seller_order_hash" mapstructure:"seller_order_hash"`
	BlockHash         string     `bson:"block_hash,omitempty" json:"block_hash" mapstructure:"block_hash"`
	BlockNumber       int64      `bson:"block_number,omitempty" json:"block_number" mapstructure:"block_number"`
	LogIndex          int64      `bson:"log_index,omitempty" json:"log_index" mapstructure:"log_index"`
	Data              any        `bson:"data" json:"data" mapstructure:"data"`
}

type SecondMarketOperationPerAsset struct {
	Asset      string                   `mapstructure:"asset"`
	Count      int64                    `mapstructure:"count"`
	Operations []*SecondMarketOperation `mapstructure:"operations"`
}

type SecondMarketOperationExport struct {
	Operations []map[string]any
	ColNames   []string
	ColTypes   []string
}

func (o SecondMarketOperation) CollectionName() string {
	return "second_market_operations"
}

func FindLastRecordedOperation(downloadedFrom, metaverse, blockchain, contractId string, eventTypes []string, dbInstance *mongo.Database) (*SecondMarketOperation, error) {
	lastOperation := &SecondMarketOperation{}
	dbCollection := helpers.CollectionInstance(dbInstance, lastOperation)
	payload := bson.M{"downloaded_from": downloadedFrom, "metaverse": metaverse, "type": bson.M{"$in": eventTypes}}
	if blockchain != "" {
		payload["blockchain"] = blockchain
	}
	if contractId != "" {
		payload["asset_contract"] = contractId
	}
	err := dbCollection.FirstWithCtx(context.Background(), payload, lastOperation, &options.FindOneOptions{Sort: bson.M{"date": -1}})
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		} else {
			lastOperation = nil
		}
	}
	if lastOperation != nil {
		return lastOperation, nil
	}
	return nil, nil
}

func Save2ndMarketOperations(operations []*SecondMarketOperation, dbInstance *mongo.Database) error {
	if operations != nil && len(operations) > 0 {
		dbCollection := helpers.CollectionInstance(dbInstance, &SecondMarketOperation{})

		dbRequests := make([]mongo.WriteModel, len(operations))
		for i, operation := range operations {
			var filterPayload = bson.M{"operation_id": operation.OperationId, "type": operation.Type, "source": operation.Source, "date": operation.Date}
			dbRequests[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(operation).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), dbRequests)
		return err
	}
	return nil
}

func GetAssetType(metaverse string, contractId string) string {
	assetType := ""
	if metaverse == "decentraland" {
		if contractId == "0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d" {
			assetType = "land"
		} else if contractId == "0x959e104e1a4db6317fa58f8295f586e1a978c297" {
			assetType = "estate"
		}
	}
	return assetType
}

func GetOperations(metaverse, source string, dbInstance *mongo.Database) ([]*SecondMarketOperation, error) {
	dbCollection := helpers.CollectionInstance(dbInstance, &SecondMarketOperation{})
	opts := options.Find().SetSort(bson.M{"date": 1}).SetLimit(100000)
	cursor, err := dbCollection.Find(context.Background(), bson.M{"downloaded_from": source, "metaverse": metaverse}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	operations := make([]*SecondMarketOperation, 0)
	err = cursor.All(context.Background(), &operations)
	return operations, err
}

func sortOperationFunc(a, b map[string]any) int {
	aDate, _ := a["date"].(*time.Time)
	bDate, _ := b["date"].(*time.Time)
	if aDate.UnixMilli() < bDate.UnixMilli() {
		return -1
	} else if aDate.UnixMilli() > bDate.UnixMilli() {
		return 1
	}
	return 0
}

func filterGetPreviousListingOrBid(prevType string, o *SecondMarketOperation, opList []*SecondMarketOperation) int {
	for i := len(opList) - 1; i >= 0; i-- {
		diff := new(big.Float).Sub(new(big.Float).SetFloat64(o.PaymentAmount), new(big.Float).SetFloat64(opList[i].PaymentAmount))
		diff = new(big.Float).Abs(diff)
		diffCmp := diff.Cmp(new(big.Float).SetFloat64(0.0))
		sameAsset := opList[i].AssetId == o.AssetId
		sameAmount := diffCmp == 0
		goodCcy := opList[i].PaymentCurrency == o.PaymentCurrency
		sameMaker := false
		if prevType == "LIST" {
			sameMaker = opList[i].Maker == o.Seller
		} else if prevType == "BID" {
			sameMaker = opList[i].Maker == o.Buyer
		}
		if sameAsset && sameMaker && goodCcy && sameAmount {
			return i
		}
	}
	return -1
}

func findExportedOpIndex(target map[string]any, toFind string) bool {
	targetId, _ := target["operation_id"].(string)
	return targetId == toFind
}

func initializeExportOpAddInfo() (m map[string]interface{}) {
	m = map[string]interface{}{
		"related_to":      "",
		"rt_date":         "",
		"rt_time_diff":    0.0,
		"rt_operation_id": "",
	}
	return m
}

func initializeExportOpAddInfoHT() (h []string, t []string) {
	h = []string{"related_to", "rt_date", "rt_time_diff", "rt_operation_id"}
	t = []string{"string", "string", "float64", "string"}
	return h, t
}

func populateExportOpAddInfo(m *map[string]interface{}, relatedTo string, rtDate *time.Time, rtTimeDiff float64, rtOperationId string) {
	(*m)["related_to"] = relatedTo
	(*m)["rt_date"] = rtDate.Format(time.RFC3339Nano)
	(*m)["rt_time_diff"] = rtTimeDiff
	(*m)["rt_operation_id"] = rtOperationId
}

func GetOperationsForExport(metaverse, source, metric string, longFields []string, dbInstance *mongo.Database, loggingPrefix string) (*SecondMarketOperationExport, error) {

	/*
		Step 1 : Pipeline to get data from database
	*/
	dbLoggingPrefix := loggingPrefix + " [" + "GetOperationsForExport" + "]"
	helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Fetch data from database..."))
	dbCollection := helpers.CollectionInstance(dbInstance, &SecondMarketOperation{})
	filter1Stage := bson.D{
		{"$match", bson.D{{"metaverse", metaverse}, {"downloaded_from", source}}},
	}
	_ = bson.D{
		{"$match", bson.D{{"type", "SELL"}}},
	}
	distinctAssetsStage := bson.D{
		{"$group", bson.D{
			{"_id", "$asset_id"},
			{"count", bson.D{{"$sum", 1}}},
		}},
	}
	joinOperationsStage := bson.D{
		{"$lookup", bson.D{
			{"from", "rarible_operations"}, {"localField", "_id"},
			{"foreignField", "asset_id"}, {"as", "operations"},
		}},
	}
	sortStage := bson.D{
		{"$sort", bson.D{
			{"count", -1},
		}},
	}
	limitStage := bson.D{
		{"$limit", 20},
	}
	pipeline := mongo.Pipeline{filter1Stage, distinctAssetsStage, joinOperationsStage, sortStage, limitStage}
	opts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := dbCollection.Aggregate(context.Background(), pipeline, opts)
	if err != nil {
		return nil, err
	}
	rawOperationsPerSoldAssets := make([]bson.M, 0)
	err = cursor.All(context.Background(), &rawOperationsPerSoldAssets)
	if err != nil {
		return nil, err
	}

	mtvCurrencies := make([]string, 0)
	if metaverse == "decentraland" {
		mtvCurrencies = []string{"MANA", "ETH"}
	}
	helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Data fetched from database !!!"))

	/*
		Step 2 : Loop to parse data and convert to map[string]any
	*/
	helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Loop over assets..."))
	aCount := len(rawOperationsPerSoldAssets)
	aIndex := 0
	operations := make([]map[string]any, 0)
	for _, rawRopsaItem := range rawOperationsPerSoldAssets {
		aIndex++
		helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Processing asset %s [%d/%d] ...", rawRopsaItem["_id"], aIndex, aCount))

		/*
			Step 2.1 : Convert pipeline result to Operation Struct
		*/
		ropsaItem := &SecondMarketOperationPerAsset{}
		_ = utils.ConvertMapToStruct(rawRopsaItem, ropsaItem)

		windowStart := 0
		helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Processing asset %s [%d/%d]! Loop over asset operations...", rawRopsaItem["_id"], aIndex, aCount))
		oCount := len(ropsaItem.Operations)
		oIndex := 0
		for i, assetOp := range ropsaItem.Operations {
			oIndex++
			helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Processing operation %s [%d/%d] of asset %s [%d/%d]...", assetOp.OperationId, oIndex, oCount, rawRopsaItem["_id"], aIndex, aCount))
			/*
				Step 2.2 : Correct Currency Price & Amount USD if necessary and possible
			*/
			if assetOp.PaymentAmountUsd == 0 {
				price, ok := helpers.GetCurrencyPrice(assetOp.PaymentCurrency, *assetOp.Date)
				if ok {
					assetOp.PaymentCcyPrice = price
					bfAmtUsd := new(big.Float).Mul(new(big.Float).SetFloat64(price), new(big.Float).SetFloat64(assetOp.PaymentAmount))
					fAmtUsd, _ := bfAmtUsd.Float64()
					assetOp.PaymentAmountUsd = fAmtUsd
				}
			}

			/*
				Step 2.3 : Add related transactions for LISTs & BIDs, and reciprocally for SELLs
			*/
			astOpAddInfo := initializeExportOpAddInfo()
			if assetOp.Type == "SELL" {
				if i > 0 {
					rtOperation := new(SecondMarketOperation)
					listingIndex := filterGetPreviousListingOrBid("LIST", assetOp, ropsaItem.Operations[windowStart:i])
					if listingIndex >= 0 {
						listingOp := ropsaItem.Operations[windowStart+listingIndex]
						populateExportOpAddInfo(&astOpAddInfo, "LIST", listingOp.Date, assetOp.Date.Sub(*listingOp.Date).Hours()/24, listingOp.OperationId)
						windowStart += listingIndex + 1
						rtOperation = listingOp
					} else {
						bidIndex := filterGetPreviousListingOrBid("BID", assetOp, ropsaItem.Operations[windowStart:i])
						if bidIndex >= 0 {
							bidOp := ropsaItem.Operations[windowStart+bidIndex]
							populateExportOpAddInfo(&astOpAddInfo, "BID", bidOp.Date, assetOp.Date.Sub(*bidOp.Date).Hours()/24, bidOp.OperationId)
							windowStart += listingIndex + 1
							rtOperation = bidOp
						}
					}
					if rtOperation != nil {
						rtIndex := slices.IndexFunc(operations, func(m map[string]any) bool {
							return findExportedOpIndex(m, rtOperation.OperationId)
						})
						if rtIndex >= 0 {
							rtOperationMap := operations[rtIndex]
							populateExportOpAddInfo(&rtOperationMap, "SELL", assetOp.Date, assetOp.Date.Sub(*rtOperation.Date).Hours()/24, assetOp.OperationId)
						}
					}
				}
			}

			/*
				Step 2.4. Convert to Map
			*/
			assetOpMap := map[string]any{}
			_ = utils.ConvertStructToMap(assetOp, []string{}, &assetOpMap)
			for k, v := range astOpAddInfo {
				assetOpMap[k] = v
			}

			/*
				Step 2.5. Add Metaverse specific data
			*/
			if metaverse == "decentraland" {
				distances := helpers.GetDclDistanceToFocalPoints(*assetOp.AssetLocX, *assetOp.AssetLocY, metric)
				for k, v := range distances {
					assetOpMap[k] = v
				}
			}

			/*
				Step 2.6. Add Currencies info
			*/
			if len(mtvCurrencies) > 0 {
				currInfo := helpers.GetCurrenciesTimeData(mtvCurrencies, *assetOp.Date)
				for k, v := range currInfo {
					assetOpMap[k] = v
				}
			}

			/*
				Step 2.7. Shorten long string fields if necessary
			*/
			if longFields != nil && len(longFields) > 0 {
				utils.ShortenLongFields(assetOpMap, longFields)
			}

			/*
				Step 2.8. Operation treatment ended. All to list
			*/
			operations = append(operations, assetOpMap)
			helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Processed operation %s [%d/%d] of asset %s [%d/%d] !!!", assetOp.OperationId, oIndex, oCount, rawRopsaItem["_id"], aIndex, aCount))
		}

		helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Processed asset %s [%d/%d] !!!", rawRopsaItem["_id"], aIndex, aCount))
	}

	helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Processed all assets !!!"))

	/*
		Step 3. Build Headers & Data Types
	*/
	helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Build columns headers & types..."))
	headers := make([]string, 0)
	types := make([]string, 0)
	exclude := []string{"cursor", "reverted", "data"}
	if len(operations) > 0 {
		// Struct based headers & types
		h1, t1 := utils.GetStructToMapHT(operations[0], exclude)

		// Related transaction headers & types
		h2, t2 := initializeExportOpAddInfoHT()

		// Metavers specific data headers & types
		h3, t3 := make([]string, 0), make([]string, 0)
		if metaverse == "decentraland" {
			h3, t3 = helpers.GetDclDistanceToFocalPointsHT()
		}

		// Currencies info headers & types
		h4, t4 := helpers.GetCurrenciesTimeDataHeaders(mtvCurrencies)

		headers = append(headers, h1...)
		headers = append(headers, h2...)
		headers = append(headers, h3...)
		headers = append(headers, h4...)
		types = append(types, t1...)
		types = append(types, t2...)
		types = append(types, t3...)
		types = append(types, t4...)
	}
	helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Columns headers & types built !!!"))

	/*
		Step 3. Sort operations by date asc
	*/
	helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Sort operations..."))
	slices.SortFunc(operations, sortOperationFunc)
	helpers.Logging(dbLoggingPrefix, fmt.Sprintf("Operations sorted !!!"))

	result := &SecondMarketOperationExport{
		Operations: operations,
		ColNames:   headers,
		ColTypes:   types,
	}
	return result, nil
}
