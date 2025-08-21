package main

import (
	"OpenSeaDataDownloader/downloader"
	"flag"
	"log"
	"os"
	"slices"
	"strings"

	"github.com/joho/godotenv"
)

type AppInput struct {
	Purpose       string
	Source        string
	Metaverse     string
	Blockchain    string
	AssetContract string
	EventTypes    []string
	Metric        string
}

func usage() {
	log.Println("Usage: metav2dmarket [-p purpose] [-s source] [-x metaverse] [-b blockchain] [-c asset_contract] [-e events (comma-separated)] [-m metric]\n" +
		"\tmetav2dmarket -p download [-s source] [-x metaverse] [-b blockchain] [-c asset_contract] [-e events (comma-separated)]\n" +
		"\tmetav2dmarket -p export [-s source] [-x metaverse] [-m metric]")
	flag.PrintDefaults()
}

func showUsageAndExit(exitCode int) {
	usage()
	os.Exit(exitCode)
}

func readFlags() (*AppInput, bool) {
	var purpose = flag.String("p", "", "Purpose (download | export)")
	var source = flag.String("s", "", "Source (opensea | rarible)")
	var metaverse = flag.String("x", "", "Metaverse (decentraland | thesandbox)")
	var blockchain = flag.String("b", "", "Blockchain (ethereum | polygon)")
	var assetContract = flag.String("c", "", "Asset Contract")
	var eventsListStr = flag.String("e", "", "events (comma-separated)")
	var metric = flag.String("m", "", "metric (euclidean | manhattan)")
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *purpose == "" || !slices.Contains([]string{"export", "download"}, *purpose) {
		showUsageAndExit(0)
		return nil, false
	}
	if *source == "" || !slices.Contains([]string{"opensea", "rarible"}, *source) {
		showUsageAndExit(0)
		return nil, false
	}
	if *metaverse == "" || !slices.Contains([]string{"decentraland"}, *metaverse) {
		showUsageAndExit(0)
		return nil, false
	}
	eventsListArr := make([]string, 0)
	if *purpose == "download" {
		if *blockchain == "" || !slices.Contains([]string{"ethereum", "polygon"}, *blockchain) {
			showUsageAndExit(0)
			return nil, false
		}
		if *assetContract == "" {
			showUsageAndExit(0)
			return nil, false
		}
		if *eventsListStr == "" {
			showUsageAndExit(0)
			return nil, false
		}
		eventsListArr = strings.Split(*eventsListStr, ",")
	} else {
		if *metric == "" || !slices.Contains([]string{"euclidean", "manhattan"}, *metric) {
			showUsageAndExit(0)
			return nil, false
		}
	}
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Fail to load %s env file", ".env")
		return nil, false
	}

	input := &AppInput{
		Purpose:       *purpose,
		Source:        *source,
		Metaverse:     *metaverse,
		Blockchain:    *blockchain,
		AssetContract: *assetContract,
		EventTypes:    eventsListArr,
		Metric:        *metric,
	}

	return input, true
}

func main() {
	appInput, ok := readFlags()
	if !ok {
		os.Exit(0)
	}
	if appInput.Purpose == "download" {
		if appInput.Source == "opensea" {
			downloader.OpenseaLaunch(appInput.Blockchain, appInput.Metaverse, appInput.EventTypes)
		} else if appInput.Source == "rarible" {
			downloader.RaribleLaunch(appInput.Blockchain, appInput.Metaverse, appInput.AssetContract, appInput.EventTypes)
		}
	} else if appInput.Purpose == "export" {
		downloader.ExportOperations(appInput.Metaverse, appInput.Source, appInput.Metric)
	}
}
