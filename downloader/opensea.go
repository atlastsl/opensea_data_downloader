package downloader

import (
	"OpenSeaDataDownloader/helpers"
	"OpenSeaDataDownloader/utils"
	"errors"
	"fmt"
	"math/big"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/mongo"
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

func getOpenseaTimestampStart(metaverse string, eventTypes []string, dbInstance *mongo.Database) (int64, error) {
	lastOperation, err := FindLastRecordedOperation("opensea", metaverse, "", "", eventTypes, dbInstance)
	if err != nil {
		return 0, err
	}
	if lastOperation != nil {
		return lastOperation.Date.UnixMilli() / 1000, nil
	}
	return 0, nil
}

func getOpenseaEventsRequest(collection string, eventTypes []string, before int64, nextToken string) (*EventList, error) {
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

func formatType(rawType string) string {
	if rawType == "sale" {
		return "SELL"
	} else if rawType == "listing" {
		return "LIST"
	} else if rawType == "item_offer" {
		return "BID"
	} else if rawType == "TRANSFER" {
		return "TRANSFER"
	}
	return ""
}

func parseOpenseaEvent(event *Event, metaverse, blockchain string, parcelList map[string]*helpers.DecentralandParcel) *SecondMarketOperation {
	operationType := ""
	if event.EventType == "order" {
		operationType = event.OrderType
	} else {
		operationType = event.EventType
	}
	operationType = formatType(event.OrderType)
	paymentAmount, paymentCurrency, paymentToken, paymentType := 0.0, "", "", ""
	if event.Payment != nil {
		bigAmount := new(big.Float)
		bigAmount, _ = bigAmount.SetString(event.Payment.Quantity)
		bigDecimalsStr := decimals[event.Payment.Decimals]
		bigDecimals, _ := new(big.Float).SetString(bigDecimalsStr)
		bigPayAmount := new(big.Float).Quo(bigAmount, bigDecimals)
		paymentAmount, _ = bigPayAmount.Float64()
		paymentCurrency = event.Payment.Symbol
		paymentToken = event.Payment.TokenAddress
		if slices.Contains([]string{"ETH", "POL", "MATIC"}, paymentCurrency) {
			paymentType = paymentCurrency
		} else {
			paymentType = "ERC20"
		}
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
	eventTime := time.UnixMilli(event.EventTimestamp * 1000)
	assetType := GetAssetType(metaverse, asset.Contract)
	hashPayload := fmt.Sprintf("%s:%s:%s:%s:%s:%s", metaverse, operationType, eventTime.Format(time.RFC3339Nano), event.OrderHash, from, asset.Identifier)
	operationId := utils.CreateHash(hashPayload)
	openseaOp := &Operation{
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
	operation := &SecondMarketOperation{
		OperationId:       operationId,
		DownloadedFrom:    "opensea",
		Type:              operationType,
		Source:            "OPEN_SEA",
		Date:              &eventTime,
		LastUpdatedAt:     &eventTime,
		Cursor:            strconv.FormatInt(eventTime.UnixMilli(), 10),
		Reverted:          false,
		OrderId:           "",
		OrderHash:         event.OrderHash,
		TransactionHash:   event.Transaction,
		TransactionType:   "",
		Maker:             maker,
		Taker:             taker,
		Buyer:             buyer,
		Seller:            seller,
		Metaverse:         metaverse,
		Blockchain:        blockchain,
		AssetContract:     asset.Contract,
		AssetType:         assetType,
		AssetId:           asset.Identifier,
		AssetLocation:     assetLocation,
		AssetLocX:         assetLocX,
		AssetLocY:         assetLocY,
		AssetValue:        event.Quantity,
		PaymentBlockchain: event.Chain,
		PaymentType:       paymentType,
		PaymentToken:      paymentToken,
		PaymentCurrency:   paymentCurrency,
		PaymentAmount:     paymentAmount,
		PaymentAmountUsd:  0,
		PaymentCcyPrice:   0,
		BuyerOrderHash:    "",
		SellerOrderHash:   "",
		BlockHash:         "",
		BlockNumber:       0,
		LogIndex:          0,
		Data: map[string]any{
			"opensea": openseaOp,
			"rawData": event,
		},
	}
	return operation
}

func OpenseaLaunch(blockchain, metaverse string, eventTypes []string) {
	loggingPrefix := fmt.Sprintf("{ %s | %s | %s }", blockchain, metaverse, strings.Join(eventTypes, ","))
	helpers.Logging(loggingPrefix, "Start...")

	maxTimestamp, minTimestamp := 1672531200, 1672531200

	helpers.Logging(loggingPrefix, "Read parcels data...")
	parcelsList := helpers.ReadDecentralandParcels()
	helpers.Logging(loggingPrefix, "Read parcels data OK !!!")

	helpers.Logging(loggingPrefix, "Connection to database...")
	dbInstance, err := helpers.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer helpers.CloseDatabaseConnection(dbInstance)
	helpers.Logging(loggingPrefix, "Connected to database !!!")

	helpers.Logging(loggingPrefix, "Getting first request `before` timestamp...")
	startTimestamp, err := getOpenseaTimestampStart(metaverse, eventTypes, dbInstance)
	if startTimestamp == 0 {
		startTimestamp = int64(maxTimestamp)
	}
	if err != nil {
		panic(err)
	}
	helpers.Logging(loggingPrefix, "First request `before` timestamp OK !!!")

	helpers.Logging(loggingPrefix, "Starting requests loop...")
	nextToken := ""
	stop := false
	var loopErr error
	requestCount := 0
	for !stop {
		requestCount++
		helpers.Logging(loggingPrefix, fmt.Sprintf("Running request #%d ...", requestCount))

		eventsList, e1 := getOpenseaEventsRequest(metaverse, eventTypes, startTimestamp, nextToken)
		if e1 != nil {
			stop = true
			loopErr = e1
		} else if eventsList == nil {
			stop = true
			loopErr = errors.New("error when parsing events list")
		} else {
			operations := make([]*SecondMarketOperation, len(eventsList.AssetEvents))
			for i, event := range eventsList.AssetEvents {
				operations[i] = parseOpenseaEvent(event, metaverse, blockchain, parcelsList)
			}
			err = Save2ndMarketOperations(operations, dbInstance)
			if err != nil {
				loopErr = err
				helpers.Logging(loggingPrefix, fmt.Sprintf("Error occurred when saving data for request #%d ...", requestCount))
				stop = true
			} else {
				helpers.Logging(loggingPrefix, fmt.Sprintf("Save data for request #%d ...", requestCount))
				if eventsList.Next != "" {
					if len(eventsList.AssetEvents) > 0 && eventsList.AssetEvents[len(eventsList.AssetEvents)-1].EventTimestamp < int64(minTimestamp) {
						stop = true
					} else {
						nextToken = eventsList.Next
						stop = true
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
