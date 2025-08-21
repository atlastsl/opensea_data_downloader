package helpers

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Currency struct {
	mgm.DefaultModel `bson:",inline"`
	Blockchain       string `bson:"blockchain,omitempty"`
	Contract         string `bson:"contract,omitempty"`
	Decimals         int64  `bson:"decimals,omitempty"`
	Name             string `bson:"name,omitempty"`
	Symbols          string `bson:"symbols,omitempty"`
	PriceMap         string `bson:"price_map,omitempty"`
	PriceSlug        string `bson:"price_slug,omitempty"`
	MainCurrency     bool   `bson:"main_currency"`
}

type CurrencyPrice struct {
	mgm.DefaultModel `bson:",inline"`
	Currency         string    `bson:"currency,omitempty"`
	Start            time.Time `bson:"start,omitempty"`
	End              time.Time `bson:"end,omitempty"`
	Open             float64   `bson:"open,omitempty"`
	High             float64   `bson:"high,omitempty"`
	Low              float64   `bson:"low,omitempty"`
	Close            float64   `bson:"close,omitempty"`
	Avg              float64   `bson:"avg,omitempty"`
	Volume           float64   `bson:"volume,omitempty"`
	MarketCap        float64   `bson:"market_cap,omitempty"`
}

var (
	currencyPrices = make(map[string][]*CurrencyPrice)
)

func GetCurrencies(blockchain string, dbInstance *mongo.Database) (map[string]string, error) {
	dbCollection := CollectionInstance(dbInstance, &Currency{})
	cursor, err := dbCollection.Find(context.Background(), bson.M{"blockchain": blockchain})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]*Currency, 0)
	err = cursor.All(context.Background(), &results)
	currencies := make(map[string]string)
	for _, result := range results {
		currencies[fmt.Sprintf("%s:%s", strings.ToLower(result.Blockchain), strings.ToLower(result.Contract))] = result.Symbols
	}
	return currencies, nil
}

func ReadCurrencyPrices(dbInstance *mongo.Database) error {
	curCollection := CollectionInstance(dbInstance, &Currency{})
	rawCurrencies, err := curCollection.Distinct(context.Background(), "symbols", bson.M{})
	if err != nil {
		return err
	}
	currencies := make([]string, 0)
	for _, currency := range rawCurrencies {
		currencies = append(currencies, currency.(string))
	}

	pricesCollection := CollectionInstance(dbInstance, &CurrencyPrice{})
	currencyPrices = make(map[string][]*CurrencyPrice)
	for _, currency := range currencies {
		cursor, e0 := pricesCollection.Find(context.Background(), bson.M{"currency": currency}, &options.FindOptions{Sort: bson.M{"start": 1}})
		if e0 != nil {
			return e0
		}
		_currencyPrices := make([]*CurrencyPrice, 0)
		e0 = cursor.All(context.Background(), &_currencyPrices)
		if e0 != nil {
			return e0
		}
		_ = cursor.Close(context.Background())
		currencyPrices[currency] = _currencyPrices
	}

	return nil
}

func GetCurrencyPrice(currency string, date time.Time) (price float64, exists bool) {
	price = 0.0
	exists = false
	filteredPrices, hasCp := currencyPrices[currency]
	if hasCp && filteredPrices != nil && len(filteredPrices) > 0 {
		if date.UnixMilli() < filteredPrices[0].Start.UnixMilli() {
			price = filteredPrices[0].Open
			exists = true
		} else if date.UnixMilli() >= filteredPrices[len(filteredPrices)-1].End.UnixMilli() {
			price = filteredPrices[len(filteredPrices)-1].Close
			exists = true
		} else {
			bestPriceInstance := new(CurrencyPrice)
			for _, priceItem := range filteredPrices {
				if priceItem.Start.UnixMilli() <= date.UnixMilli() && date.UnixMilli() < priceItem.End.UnixMilli() {
					bestPriceInstance = priceItem
					break
				}
			}
			if bestPriceInstance != nil {
				openP := new(big.Float).SetFloat64(bestPriceInstance.Open)
				closeP := new(big.Float).SetFloat64(bestPriceInstance.Close)
				highP := new(big.Float).SetFloat64(bestPriceInstance.High)
				lowP := new(big.Float).SetFloat64(bestPriceInstance.Low)
				temp := new(big.Float).Add(openP, closeP)
				temp = temp.Add(temp, highP)
				temp = temp.Add(temp, lowP)
				temp = temp.Quo(temp, new(big.Float).SetFloat64(4.0))
				price, _ = temp.Float64()
				exists = true
			}
		}
	}
	return price, exists
}

func GetCurrencyMarketCap(currency string, date time.Time) (marketCap float64, exists bool) {
	marketCap = 0.0
	exists = false
	filteredPrices, hasCp := currencyPrices[currency]
	if hasCp && filteredPrices != nil && len(filteredPrices) > 0 {
		if date.UnixMilli() < filteredPrices[0].Start.UnixMilli() {
			marketCap = filteredPrices[0].MarketCap
			exists = true
		} else if date.UnixMilli() >= filteredPrices[len(filteredPrices)-1].End.UnixMilli() {
			marketCap = filteredPrices[len(filteredPrices)-1].MarketCap
			exists = true
		} else {
			bestPriceInstance := new(CurrencyPrice)
			for _, priceItem := range filteredPrices {
				if priceItem.Start.UnixMilli() <= date.UnixMilli() && date.UnixMilli() < priceItem.End.UnixMilli() {
					bestPriceInstance = priceItem
					break
				}
			}
			if bestPriceInstance != nil {
				marketCap = bestPriceInstance.MarketCap
				exists = true
			}
		}
	}
	return marketCap, exists
}

func GetCurrenciesTimeData(currencies []string, date time.Time) (data map[string]float64) {
	data = make(map[string]float64)
	for _, currency := range currencies {
		price, _ := GetCurrencyPrice(currency, date)
		marketCap, _ := GetCurrencyMarketCap(currency, date)
		data[fmt.Sprintf("%s_PRICE", currency)] = price
		data[fmt.Sprintf("%s_MARKET_CAP", currency)] = marketCap
	}
	return data
}

func GetCurrenciesTimeDataHeaders(currencies []string) (h []string, t []string) {
	h = make([]string, 0)
	t = make([]string, 0)
	for _, currency := range currencies {
		h = append(h, fmt.Sprintf("%s_PRICE", currency))
		h = append(h, fmt.Sprintf("%s_MARKET_CAP", currency))
		t = append(t, "float64")
		t = append(t, "float64")
	}
	return h, t
}
