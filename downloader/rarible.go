package downloader

import (
	"OpenSeaDataDownloader/helpers"
	"OpenSeaDataDownloader/utils"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

type RaribleTakerMakerInfo struct {
	Type       string `mapstructure:"@type" json:"@type"`
	Contract   string `mapstructure:"contract" json:"contract"`
	Blockchain string `mapstructure:"blockchain" json:"blockchain"`
	Collection string `mapstructure:"collection" json:"collection"`
	TokenId    string `mapstructure:"tokenId" json:"tokenId"`
}

type RaribleBlockchainInfo struct {
	TransactionHash string `mapstructure:"transactionHash" json:"transactionHash"`
	BlockHash       string `mapstructure:"blockHash" json:"blockHash"`
	BlockNumber     int64  `mapstructure:"blockNumber" json:"blockNumber"`
	LogIndex        int64  `mapstructure:"logIndex" json:"logIndex"`
}

type RaribleTakerMakerType struct {
	Type  *RaribleTakerMakerInfo `mapstructure:"type" json:"type"`
	Value string                 `mapstructure:"value" json:"value"`
}

type RaribleTActivity struct {
	Id              string                 `mapstructure:"id" json:"id"`
	Type            string                 `mapstructure:"@type" json:"@type"`
	Date            string                 `mapstructure:"date" json:"date"`
	LastUpdatedAt   string                 `mapstructure:"lastUpdatedAt" json:"lastUpdatedAt"`
	Cursor          string                 `mapstructure:"cursor" json:"cursor"`
	Reverted        bool                   `mapstructure:"reverted" json:"reverted"`
	Hash            string                 `mapstructure:"hash" json:"hash"`
	Maker           string                 `mapstructure:"maker" json:"maker"`
	Taker           string                 `mapstructure:"taker" json:"taker"`
	Make            *RaribleTakerMakerType `mapstructure:"make" json:"make"`
	Take            *RaribleTakerMakerType `mapstructure:"take" json:"take"`
	Source          string                 `mapstructure:"source" json:"source"`
	TransactionHash string                 `mapstructure:"transactionHash" json:"transactionHash"`
	BlockchainInfo  *RaribleBlockchainInfo `mapstructure:"blockchainInfo" json:"blockchainInfo"`
	OrderId         string                 `mapstructure:"orderId" json:"orderId"`
	Nft             *RaribleTakerMakerType `mapstructure:"nft" json:"nft"`
	Payment         *RaribleTakerMakerType `mapstructure:"payment" json:"payment"`
	Buyer           string                 `mapstructure:"buyer" json:"buyer"`
	Seller          string                 `mapstructure:"seller" json:"seller"`
	BuyerOrderHash  string                 `mapstructure:"buyerOrderHash" json:"buyerOrderHash"`
	SellerOrderHash string                 `mapstructure:"sellerOrderHash" json:"sellerOrderHash"`
	Price           string                 `mapstructure:"price" json:"price"`
	PriceUsd        string                 `mapstructure:"priceUsd" json:"priceUsd"`
	AmountUsd       string                 `mapstructure:"amountUsd" json:"amountUsd"`
	TransactionType string                 `mapstructure:"type" json:"type"`
}

type RaribleTActivityList struct {
	Cursor     string              `mapstructure:"cursor" json:"cursor"`
	Activities []*RaribleTActivity `mapstructure:"activities" json:"activities"`
}

func getRaribleNftActStartCursor(metaverse, blockchain, contractId string, eventTypes []string, dbInstance *mongo.Database) (string, error) {
	lastOperation, err := FindLastRecordedOperation("rarible", metaverse, blockchain, contractId, eventTypes, dbInstance)
	if err != nil {
		return "", err
	}
	if lastOperation != nil {
		return lastOperation.Cursor, nil
	}
	return "", nil
}

func getRaribleNftActivities(blockchain, contractId, cursor string, eventTypes []string) (*RaribleTActivityList, error) {
	url := "https://api.rarible.org/v0.1/activities/byCollection"

	collection := fmt.Sprintf("%s:%s", strings.ToUpper(blockchain), strings.ToLower(contractId))
	payload := map[string]any{
		"collection": collection,
		"size":       1000,
		"sort":       "EARLIEST_FIRST",
	}
	if eventTypes != nil && len(eventTypes) > 0 {
		payload["type"] = eventTypes
	}
	if cursor != "" {
		payload["cursor"] = cursor
	}

	headers := map[string]string{
		"X-API-KEY": os.Getenv("RARIBLE_API_KEY"),
	}

	activitiesList := &RaribleTActivityList{}
	err := utils.SendHttpRequest(url, "GET", headers, payload, activitiesList)
	return activitiesList, err
}

func parseRaribleNftActivity(rrbActivity *RaribleTActivity, metaverse, blockchain string, parcelList map[string]*helpers.DecentralandParcel, currencies map[string]string) *SecondMarketOperation {
	opDate, _ := time.Parse(time.RFC3339, rrbActivity.Date)
	opLastUpdatedAt, _ := time.Parse(time.RFC3339Nano, rrbActivity.LastUpdatedAt)
	maker, taker, buyer, seller := "", "", "", ""
	if rrbActivity.Maker != "" {
		maker = strings.Split(rrbActivity.Maker, ":")[1]
	}
	if rrbActivity.Taker != "" {
		taker = strings.Split(rrbActivity.Taker, ":")[1]
	}
	if rrbActivity.Buyer != "" {
		buyer = strings.Split(rrbActivity.Buyer, ":")[1]
	}
	if rrbActivity.Seller != "" {
		seller = strings.Split(rrbActivity.Seller, ":")[1]
	}
	var assetInfo *RaribleTakerMakerInfo
	var assetValue int
	if rrbActivity.Type == "SELL" {
		assetInfo = rrbActivity.Nft.Type
		assetValue, _ = strconv.Atoi(rrbActivity.Nft.Value)
	} else if rrbActivity.Type == "LIST" {
		assetInfo = rrbActivity.Make.Type
		assetValue, _ = strconv.Atoi(rrbActivity.Make.Value)
	} else if rrbActivity.Type == "BID" {
		assetInfo = rrbActivity.Take.Type
		assetValue, _ = strconv.Atoi(rrbActivity.Take.Value)
	}
	assetContract, assetType, assetId, assetLocation := "", "", "", ""
	var assetLocX, assetLocY *int
	if assetInfo != nil {
		assetContract = strings.Split(assetInfo.Contract, ":")[1]
		assetType = GetAssetType(metaverse, assetContract)
		assetId = assetInfo.TokenId
		parcel, ok := parcelList[assetId]
		if ok {
			assetLocation = parcel.Id
			assetLocX = &parcel.X
			assetLocY = &parcel.Y
		}
	}
	var paymentInfo *RaribleTakerMakerInfo
	var paymentAmount, paymentAmountUsd, paymentCurrencyPrice float64
	if rrbActivity.Type == "SELL" {
		paymentInfo = rrbActivity.Payment.Type
		if rrbActivity.Payment.Value != "" {
			paymentAmount, _ = strconv.ParseFloat(rrbActivity.Payment.Value, 64)
		} else {
			paymentAmount, _ = strconv.ParseFloat(rrbActivity.Price, 64)
		}
	} else if rrbActivity.Type == "LIST" {
		paymentInfo = rrbActivity.Take.Type
		if rrbActivity.Take.Value != "" {
			paymentAmount, _ = strconv.ParseFloat(rrbActivity.Take.Value, 64)
		} else {
			paymentAmount, _ = strconv.ParseFloat(rrbActivity.Price, 64)
		}
	} else if rrbActivity.Type == "BID" {
		paymentInfo = rrbActivity.Make.Type
		if rrbActivity.Make.Value != "" {
			paymentAmount, _ = strconv.ParseFloat(rrbActivity.Make.Value, 64)
		} else {
			paymentAmount, _ = strconv.ParseFloat(rrbActivity.Price, 64)
		}
	}
	if rrbActivity.AmountUsd != "" {
		paymentAmountUsd, _ = strconv.ParseFloat(rrbActivity.AmountUsd, 64)
	} else {
		paymentAmountUsd, _ = strconv.ParseFloat(rrbActivity.PriceUsd, 64)
	}
	if paymentAmount != 0 {
		paymentCurrencyPrice = paymentAmountUsd / paymentAmount
	}
	paymentBlockchain, paymentType, paymentCurrency, paymentToken := "", "", "", ""
	if paymentInfo != nil {
		paymentType = paymentInfo.Type
		if paymentInfo.Blockchain != "" {
			paymentBlockchain = paymentInfo.Blockchain
			paymentCurrency = paymentInfo.Type
		} else if paymentInfo.Contract != "" {
			paymentBlockchain = strings.Split(paymentInfo.Contract, ":")[0]
			paymentToken = strings.Split(paymentInfo.Contract, ":")[1]
			paymentCurrency, _ = currencies[strings.ToLower(paymentInfo.Contract)]
		}
	}
	blockHash, blockNumber, logIndex := "", int64(0), int64(0)
	if rrbActivity.BlockchainInfo != nil {
		blockHash = rrbActivity.BlockchainInfo.BlockHash
		blockNumber = rrbActivity.BlockchainInfo.BlockNumber
		logIndex = rrbActivity.BlockchainInfo.LogIndex
	}
	operation := &SecondMarketOperation{
		OperationId:       rrbActivity.Id,
		DownloadedFrom:    "rarible",
		Type:              rrbActivity.Type,
		Source:            rrbActivity.Source,
		Date:              &opDate,
		LastUpdatedAt:     &opLastUpdatedAt,
		Cursor:            rrbActivity.Cursor,
		Reverted:          rrbActivity.Reverted,
		OrderId:           rrbActivity.OrderId,
		OrderHash:         rrbActivity.Hash,
		TransactionHash:   rrbActivity.TransactionHash,
		TransactionType:   rrbActivity.TransactionType,
		Maker:             maker,
		Taker:             taker,
		Buyer:             buyer,
		Seller:            seller,
		Metaverse:         metaverse,
		Blockchain:        blockchain,
		AssetContract:     assetContract,
		AssetType:         assetType,
		AssetId:           assetId,
		AssetLocation:     assetLocation,
		AssetLocX:         assetLocX,
		AssetLocY:         assetLocY,
		AssetValue:        assetValue,
		PaymentBlockchain: paymentBlockchain,
		PaymentType:       paymentType,
		PaymentToken:      paymentToken,
		PaymentCurrency:   paymentCurrency,
		PaymentAmount:     paymentAmount,
		PaymentAmountUsd:  paymentAmountUsd,
		PaymentCcyPrice:   paymentCurrencyPrice,
		BuyerOrderHash:    rrbActivity.BuyerOrderHash,
		SellerOrderHash:   rrbActivity.SellerOrderHash,
		BlockHash:         blockHash,
		BlockNumber:       blockNumber,
		LogIndex:          logIndex,
		Data:              rrbActivity,
	}
	return operation
}

func RaribleLaunch(blockchain, metaverse, assetContract string, eventTypes []string) {
	loggingPrefix := fmt.Sprintf("{ %s | %s | %s }", blockchain, metaverse, strings.Join(eventTypes, ","))
	helpers.Logging(loggingPrefix, "Start...")

	helpers.Logging(loggingPrefix, "Connection to database...")
	dbInstance, err := helpers.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer helpers.CloseDatabaseConnection(dbInstance)
	helpers.Logging(loggingPrefix, "Connected to database !!!")

	helpers.Logging(loggingPrefix, "Read currencies & parcels data...")
	parcelsList := helpers.ReadDecentralandParcels()
	currencies, err := helpers.GetCurrencies(blockchain, dbInstance)
	if err != nil {
		panic(err)
	}
	helpers.Logging(loggingPrefix, "Read currencies & parcels data OK !!!")

	helpers.Logging(loggingPrefix, "Getting first request `cursor` ...")
	startCursor, err := getRaribleNftActStartCursor(metaverse, blockchain, assetContract, eventTypes, dbInstance)
	if err != nil {
		panic(err)
	}
	helpers.Logging(loggingPrefix, "First request `cursor` OK !!!")

	helpers.Logging(loggingPrefix, "Starting requests loop...")
	nextToken := startCursor
	stop := false
	var loopErr error
	requestCount := 0
	for !stop {
		requestCount++
		helpers.Logging(loggingPrefix, fmt.Sprintf("Running request #%d ...", requestCount))

		activityList, e1 := getRaribleNftActivities(blockchain, assetContract, nextToken, eventTypes)
		if e1 != nil {
			stop = true
			loopErr = e1
		} else if activityList == nil {
			stop = true
			loopErr = errors.New("error when parsing events list")
		} else {
			operations := make([]*SecondMarketOperation, len(activityList.Activities))
			for i, activity := range activityList.Activities {
				operations[i] = parseRaribleNftActivity(activity, metaverse, blockchain, parcelsList, currencies)
			}
			err = Save2ndMarketOperations(operations, dbInstance)
			if err != nil {
				loopErr = err
				helpers.Logging(loggingPrefix, fmt.Sprintf("Error occurred when saving data for request #%d ...", requestCount))
				stop = true
			} else {
				helpers.Logging(loggingPrefix, fmt.Sprintf("Save data for request #%d ...", requestCount))
				if activityList.Cursor != "" {
					nextToken = activityList.Cursor
					if len(activityList.Activities) == 0 {
						stop = true
					} else {
						stop = true
						time.Sleep(1 * time.Second)
					}
				} else {
					stop = true
				}
			}
		}

		helpers.Logging(loggingPrefix, fmt.Sprintf("Request #%d done !", requestCount))
	}

	if loopErr != nil {
		helpers.Logging(loggingPrefix, fmt.Sprintf("Error occurred in loop #%d [Message = %s]", requestCount, loopErr.Error()))
	}

	helpers.Logging(loggingPrefix, "END...")
}
