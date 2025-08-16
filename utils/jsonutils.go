package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
)

func PrettyPrintObject(object interface{}) {
	jsonStr, err := json.MarshalIndent(object, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(jsonStr))
}

func ReadJsonFile(filePath string, target any) error {
	jsonResDistrictStr, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	var jsonFile any
	err = json.Unmarshal(jsonResDistrictStr, &jsonFile)
	if err != nil {
		return err
	}
	if reflect.TypeOf(jsonFile).Kind() == reflect.Slice {
		jsonFile = map[string]any{
			"data": jsonFile,
		}
	}
	err = ConvertMapToStruct(jsonFile.(map[string]any), target)
	return err
}
