package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	url2 "net/url"
	"reflect"
	"strings"
)

func queryfyPayload(payload map[string]any, prefix string) []string {
	queryParamsArr := make([]string, 0)
	for key, value := range payload {
		vKey := key
		if prefix != "" {
			vKey = fmt.Sprintf("%s.%s", prefix, key)
		}
		if reflect.TypeOf(value).Kind() == reflect.Map {
			valueQParamsArr := queryfyPayload(value.(map[string]any), vKey)
			queryParamsArr = append(queryParamsArr, valueQParamsArr...)
		} else if reflect.TypeOf(value).Kind() == reflect.Slice {
			for _, v := range value.([]string) {
				vv := url2.QueryEscape(fmt.Sprintf("%v", v))
				queryParamsArr = append(queryParamsArr, fmt.Sprintf("%s=%s", vKey, vv))
			}
		} else {
			vv := url2.QueryEscape(fmt.Sprintf("%v", payload[key]))
			queryParamsArr = append(queryParamsArr, fmt.Sprintf("%s=%s", vKey, vv))
		}
	}
	return queryParamsArr
}

func SendHttpRequest(url, method string, headers map[string]string, payload map[string]any, output any) error {
	_url := url
	var _body io.Reader
	if method == "GET" || method == "DELETE" {
		if payload != nil && len(payload) > 0 {
			queryParamsArr := queryfyPayload(payload, "")
			queryParamsStr := strings.Join(queryParamsArr, "&")
			_url = _url + "?" + queryParamsStr
		}
	} else {
		jsonPayload, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return err
		}
		_body = bytes.NewBuffer(jsonPayload)
	}
	req, err := http.NewRequest(method, _url, _body)
	if err != nil {
		return err
	}

	req.Header.Add("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if headers != nil && len(headers) > 0 {
		for key, value := range headers {
			req.Header.Add(key, value)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	statusCode := resp.StatusCode
	if statusCode != http.StatusOK {
		respJson := make(map[string]any)
		err = json.Unmarshal(respBody, &respJson)
		if err != nil {
			return err
		}
		errorsObj, errorsObjExists := respJson["errors"]
		if errorsObjExists {
			errorMessage := strings.Join(errorsObj.([]string), "|")
			return errors.New(errorMessage)
		} else {
			return errors.New(fmt.Sprintf("request failed - return error status code %d", statusCode))
		}
	}

	err = json.Unmarshal(respBody, output)
	return err
}
