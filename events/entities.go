package main

import (
	"OpenSeaDataDownloader/utils"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type EventPayment struct {
	Quantity     string `json:"quantity"`
	TokenAddress string `json:"token_address"`
	Decimals     int    `json:"decimals"`
	Symbol       string `json:"symbol"`
}

type EventAsset struct {
	Identifier          string `json:"identifier"`
	Collection          string `json:"collection"`
	Contract            string `json:"contract"`
	TokenStandard       string `json:"token_standard"`
	Name                string `json:"name"`
	Description         string `json:"description"`
	ImageUrl            string `json:"image_url"`
	DisplayImageUrl     string `json:"display_image_url"`
	DisplayAnimationUrl string `json:"display_animation_url"`
	MetadataUrl         string `json:"metadata_url"`
	OpenseaUrl          string `json:"opensea_url"`
	UpdatedAt           string `json:"updated_at"`
	IsDisabled          bool   `json:"is_disabled"`
	IsNsfw              bool   `json:"is_nsfw"`
}

type Event struct {
	EventType        string        `json:"event_type"`
	EventTimestamp   int64         `json:"event_timestamp"`
	Transaction      string        `json:"transaction"`
	OrderHash        string        `json:"order_hash"`
	ProtocolAddress  string        `json:"protocol_address"`
	Chain            string        `json:"chain"`
	Payment          *EventPayment `json:"payment"`
	ClosingDate      int64         `json:"closing_date"`
	Seller           string        `json:"seller"`
	Buyer            string        `json:"buyer"`
	FromAddress      string        `json:"from_address"`
	ToAddress        string        `json:"to_address"`
	OrderType        string        `json:"order_type"`
	StartDate        int64         `json:"start_date"`
	ExpirationDate   int64         `json:"expiration_date"`
	Asset            *EventAsset   `json:"asset"`
	Nft              *EventAsset   `json:"nft"`
	Quantity         int           `json:"quantity"`
	Maker            string        `json:"maker"`
	Taker            string        `json:"taker"`
	Criteria         any           `json:"criteria"`
	IsPrivateListing bool          `json:"is_private_listing"`
}

type EventList struct {
	AssetEvents []*Event `json:"asset_events"`
	Next        string   `json:"next"`
}

func sendEventsRequest(collection string, eventTypes []string, before int64, nextToken string) (*EventList, error) {
	url := fmt.Sprintf("https://api.opensea.io/api/v2/events/collection/%s", collection)

	payload := make(map[string]any)
	if eventTypes != nil && len(eventTypes) > 0 {
		payload["event_type"] = eventTypes
	}
	if before != 0 {
		payload["before"] = strconv.FormatInt(before, 10)
	}
	if nextToken != "" {
		payload["next"] = nextToken
	}
	payload["limit"] = 50

	headers := map[string]string{
		"x-api-key": os.Getenv("OPENSEA_API_KEY"),
	}

	eventsList := &EventList{}
	err := utils.SendHttpRequest(url, "GET", headers, payload, eventsList)
	return eventsList, err
}

type Operation struct {
	mgm.DefaultModel `bson:",inline"`
	Metaverse        string     `bson:"metaverse"`
	Type             string     `bson:"type"`
	Date             time.Time  `bson:"date"`
	TransactionHash  string     `bson:"transaction_hash"`
	OrderHash        string     `bson:"order_hash"`
	ProtocolAddress  string     `bson:"protocol_address,omitempty"`
	Blockchain       string     `bson:"blockchain"`
	PaymentAmount    float64    `bson:"payment_amount"`
	PaymentCurrency  string     `bson:"payment_currency"`
	PaymentToken     string     `bson:"payment_token"`
	ClosingDate      *time.Time `bson:"closing_date"`
	StartDate        *time.Time `bson:"start_date"`
	ExpirationDate   *time.Time `bson:"expiration_date"`
	Seller           string     `bson:"seller"`
	Buyer            string     `bson:"buyer"`
	FromAddress      string     `bson:"from_address"`
	ToAddress        string     `bson:"to_address"`
	Maker            string     `bson:"maker"`
	Taker            string     `bson:"taker"`
	AssetId          string     `bson:"asset_id"`
	AssetMetaverse   string     `bson:"asset_metaverse"`
	AssetContract    string     `bson:"asset_contract"`
	AssetType        string     `bson:"asset_type"`
	AssetLocation    string     `bson:"asset_location"`
	AssetLocX        *int       `bson:"asset_loc_x"`
	AssetLocY        *int       `bson:"asset_loc_y"`
	AssetName        string     `bson:"asset_name"`
	AssetDescription string     `bson:"asset_description"`
	AssetImage       string     `bson:"asset_image"`
	AssetMetadata    string     `bson:"asset_metadata"`
	AssetOpensea     string     `bson:"asset_opensea"`
	AssetUpdatedAt   *time.Time `bson:"asset_updated_at"`
	AssetIsDisabled  bool       `bson:"asset_is_disabled"`
	AssetIsNsfw      bool       `bson:"asset_is_nsfw"`
	Quantity         int        `bson:"quantity"`
	IsPrivateListing bool       `bson:"is_private_listing"`
}

func (o Operation) CollectionName() string {
	return "opensea_operations"
}

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

func readDecentralandParcels() map[string]*DecentralandParcel {
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

var decimals = map[int]string{
	2:  "100",
	4:  "10000",
	6:  "1000000",
	8:  "100000000",
	10: "10000000000",
	12: "1000000000000",
	14: "100000000000000",
	16: "10000000000000000",
	18: "1000000000000000000",
}

func parseEvent(event *Event, metaverse string, parcelList map[string]*DecentralandParcel) *Operation {
	operationType := ""
	if event.EventType == "order" {
		operationType = event.OrderType
		if operationType == "item_offer" {
			operationType = "offer"
		}
	} else {
		operationType = event.EventType
	}
	paymentAmount, paymentCurrency, paymentToken := 0.0, "", ""
	if event.Payment != nil {
		bigAmount := new(big.Float)
		bigAmount, _ = bigAmount.SetString(event.Payment.Quantity)
		bigDecimalsStr := decimals[event.Payment.Decimals]
		bigDecimals, _ := new(big.Float).SetString(bigDecimalsStr)
		bigPayAmount := new(big.Float).Quo(bigAmount, bigDecimals)
		paymentAmount, _ = bigPayAmount.Float64()
		paymentCurrency = event.Payment.Symbol
		paymentToken = event.Payment.TokenAddress
	}
	var closingDate, startDate, expirationDate *time.Time
	if event.ClosingDate != 0 {
		tmp := time.UnixMilli(event.ClosingDate * 1000)
		closingDate = &tmp
	}
	if event.StartDate != 0 {
		tmp := time.UnixMilli(event.StartDate * 1000)
		startDate = &tmp
	}
	if event.ExpirationDate != 0 {
		tmp := time.UnixMilli(event.ExpirationDate * 1000)
		expirationDate = &tmp
	}
	buyer, seller, from, to, maker, taker := "", "", "", "", "", ""
	if event.Buyer != "" {
		buyer = event.Buyer
	}
	if event.Seller != "" {
		seller = event.Seller
	}
	if event.FromAddress != "" {
		from = event.FromAddress
	} else {
		from = seller
	}
	if event.ToAddress != "" {
		to = event.ToAddress
	} else {
		to = buyer
	}
	if event.Maker != "" {
		maker = event.Maker
		if from == "" {
			if event.OrderType == "listing" {
				from = maker
			} else if event.OrderType == "item_offer" {
				to = maker
			}
		}
	}
	var asset *EventAsset
	if event.Asset != nil {
		asset = event.Asset
	} else if event.Nft != nil {
		asset = event.Nft
	} else {
		asset = &EventAsset{}
	}
	assetLocation := ""
	var assetLocX, assetLocY *int
	var assetUpdatedAt *time.Time
	if asset.Identifier != "" {
		parcel, ok := parcelList[asset.Identifier]
		if ok {
			assetLocation = parcel.Id
			assetLocX = &parcel.X
			assetLocY = &parcel.Y
		}
		tmp, eParse := time.Parse(time.RFC3339Nano, asset.UpdatedAt)
		if eParse == nil {
			assetUpdatedAt = &tmp
		}
	}
	assetType := ""
	if asset.Contract == "0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d" {
		assetType = "land"
	} else if asset.Contract == "0x959e104e1a4db6317fa58f8295f586e1a978c297" {
		assetType = "estate"
	}
	return &Operation{
		Metaverse:        metaverse,
		Type:             operationType,
		Date:             time.UnixMilli(event.EventTimestamp * 1000),
		TransactionHash:  event.Transaction,
		OrderHash:        event.OrderHash,
		ProtocolAddress:  event.ProtocolAddress,
		Blockchain:       event.Chain,
		PaymentAmount:    paymentAmount,
		PaymentCurrency:  paymentCurrency,
		PaymentToken:     paymentToken,
		ClosingDate:      closingDate,
		StartDate:        startDate,
		ExpirationDate:   expirationDate,
		Buyer:            buyer,
		Seller:           seller,
		FromAddress:      from,
		ToAddress:        to,
		Maker:            maker,
		Taker:            taker,
		AssetId:          asset.Identifier,
		AssetMetaverse:   asset.Collection,
		AssetContract:    asset.Contract,
		AssetType:        assetType,
		AssetLocation:    assetLocation,
		AssetLocX:        assetLocX,
		AssetLocY:        assetLocY,
		AssetName:        asset.Name,
		AssetDescription: asset.Description,
		AssetImage:       asset.ImageUrl,
		AssetMetadata:    asset.MetadataUrl,
		AssetOpensea:     asset.OpenseaUrl,
		AssetUpdatedAt:   assetUpdatedAt,
		AssetIsDisabled:  asset.IsDisabled,
		AssetIsNsfw:      asset.IsNsfw,
		Quantity:         event.Quantity,
		IsPrivateListing: event.IsPrivateListing,
	}
}

func getTimestampStart(metaverse string, eventTypes []string, dbInstance *mongo.Database) (int64, error) {
	lastOperation := &Operation{}
	dbCollection := utils.CollectionInstance(dbInstance, lastOperation)
	payload := bson.M{"metaverse": metaverse, "type": bson.M{"$in": eventTypes}}
	err := dbCollection.FirstWithCtx(context.Background(), payload, lastOperation, &options.FindOneOptions{Sort: bson.M{"date": 1}})
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return 0, err
		} else {
			lastOperation = nil
		}
	}
	if lastOperation != nil {
		return lastOperation.Date.UnixMilli() / 1000, nil
	}
	return 0, nil
}

var (
	loggingPrefix = ""
)

func logging(line string) {
	println(fmt.Sprintf("[%s] // (Opensea DL) %s // %s", time.Now().Format(time.RFC3339), loggingPrefix, line))
}

func saveOperations(operations []*Operation, dbInstance *mongo.Database) error {
	if operations != nil && len(operations) > 0 {
		dbCollection := utils.CollectionInstance(dbInstance, &Operation{})

		dbRequests := make([]mongo.WriteModel, len(operations))
		for i, operation := range operations {
			var filterPayload = bson.M{"metaverse": operation.Metaverse, "type": operation.Type, "date": operation.Date, "transaction_hash": operation.TransactionHash, "order_hash": operation.OrderHash}
			dbRequests[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(operation).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), dbRequests)
		return err
	}
	return nil
}

func launch(metaverse string, eventTypes []string, outputDir string) {
	loggingPrefix = fmt.Sprintf("Metaverse = %s | Events = %s", metaverse, strings.Join(eventTypes, ","))
	logging("Start...")

	logging("Read parcels data...")
	parcelsList := readDecentralandParcels()
	logging("Read parcels data OK !!!")

	logging("Connection to database...")
	dbInstance, err := utils.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer utils.CloseDatabaseConnection(dbInstance)
	logging("Connected to database !!!")

	logging("Getting first request `before` timestamp...")
	startTimestamp, err := getTimestampStart(metaverse, eventTypes, dbInstance)
	if err != nil {
		panic(err)
	}
	logging("First request `before` timestamp OK !!!")

	logging("Starting requests loop...")
	nextToken := ""
	stop := false
	var loopErr error
	requestCount := 0
	for !stop {
		requestCount++
		logging(fmt.Sprintf("Running request #%d ...", requestCount))

		eventsList, e1 := sendEventsRequest(metaverse, eventTypes, startTimestamp, nextToken)
		if e1 != nil {
			stop = true
			loopErr = e1
		} else if eventsList == nil {
			stop = true
			loopErr = errors.New("error when parsing events list")
		} else {
			operations := make([]*Operation, len(eventsList.AssetEvents))
			for i, event := range eventsList.AssetEvents {
				operations[i] = parseEvent(event, metaverse, parcelsList)
			}
			err = saveOperations(operations, dbInstance)
			if err != nil {
				loopErr = err
				logging(fmt.Sprintf("Error occurred when saving data for request #%d ...", requestCount))
				stop = true
			} else {
				logging(fmt.Sprintf("Save data for request #%d ...", requestCount))
				if eventsList.Next != "" {
					nextToken = eventsList.Next
				} else {
					stop = true
				}
			}
		}

		logging(fmt.Sprintf("Request #%d done !", requestCount))
	}

	if loopErr != nil {
		logging(fmt.Sprintf("Error occurred in loop #%d [Message = %s]", requestCount, loopErr.Error()))
	}

	logging("END...")
}

func usage() {
	log.Println("Usage: strategy [-x collection (decentraland | thesandbox)] [-e events (comma-separated)] [-o output_dir]")
	flag.PrintDefaults()
}

func showUsageAndExit(exitCode int) {
	usage()
	os.Exit(exitCode)
}

func readFlags() (*string, *string, *string, bool) {
	var collection = flag.String("x", "", "Collection (decentraland | thesandbox)")
	var eventsListStr = flag.String("e", "", "events (comma-separated)")
	var outputDir = flag.String("o", "", "output_dir")
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *collection == "" {
		showUsageAndExit(0)
		return nil, nil, nil, false
	}
	if *eventsListStr == "" {
		showUsageAndExit(0)
		return nil, nil, nil, false
	}
	if *outputDir == "" {
		showUsageAndExit(0)
		return nil, nil, nil, false
	}
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Fail to load %s env file", ".env")
		return nil, nil, nil, false
	}

	return collection, eventsListStr, outputDir, true
}

func main() {
	collection, eventsListStr, outputDir, ok := readFlags()
	if !ok {
		os.Exit(0)
	}
	eventTypes := strings.Split(*eventsListStr, ",")
	launch(*collection, eventTypes, *outputDir)
}
