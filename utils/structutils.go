package utils

import (
	"errors"
	"reflect"
	"slices"
	"strings"

	"github.com/mitchellh/mapstructure"
)

func ConvertMapToStruct(m map[string]any, target interface{}) error {
	config := &mapstructure.DecoderConfig{
		ErrorUnused: false,
		Result:      target,
	}
	decoder, e1 := mapstructure.NewDecoder(config)
	if e1 != nil {
		return e1
	}
	e2 := decoder.Decode(m)
	if e2 != nil {
		return e2
	}
	return nil
}

func ConvertStructToMap(o any, exclude []string, target *map[string]any) error {
	if o == nil {
		return errors.New("cannot convert to map")
	}
	sType := reflect.TypeOf(o)
	sValue := reflect.ValueOf(o)
	if sType.Kind() == reflect.Ptr {
		sType = sType.Elem()
		sValue = sValue.Elem()
	}
	if sType.Kind() != reflect.Struct {
		return errors.New("object is not a struct")
	}
	for i := 0; i < sType.NumField(); i++ {
		field := sType.Field(i)
		tag := field.Tag.Get("json")
		if tag == "-" || tag == "" {
			tag = field.Tag.Get("bson")
		}
		mField := strings.Split(tag, ",")[0]
		if mField != "" && !slices.Contains(exclude, mField) {
			if field.Type.Kind() == reflect.Ptr {
				(*target)[mField] = sValue.Field(i).Elem().Interface()
			} else {
				(*target)[mField] = sValue.Field(i).Interface()
			}
		}
	}
	return nil
}

func GetStructToMapHT(o any, exclude []string) (h []string, t []string) {
	rt := reflect.TypeOf(o)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	h = make([]string, 0)
	t = make([]string, 0)
	if rt.Kind() == reflect.Struct {
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			tag := f.Tag.Get("json")
			if tag == "-" || tag == "" {
				tag = f.Tag.Get("bson")
			}
			s := strings.Split(tag, ",")[0]
			if s != "" && !slices.Contains(exclude, s) {
				h = append(h, s)
				if f.Type.Kind() == reflect.Ptr {
					t = append(t, f.Type.Elem().Kind().String())
				} else {
					t = append(t, f.Type.Kind().String())
				}
			}
		}
	}
	return h, t
}

func GetJsonFieldsNames(o any, exclude []string) []string {
	rt := reflect.TypeOf(o)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() == reflect.Map {
		fields := make([]string, 0)
		for s := range o.(map[string]any) {
			if !slices.Contains(exclude, s) {
				fields = append(fields, s)
			}
		}
		return fields
	} else if rt.Kind() == reflect.Struct {
		fields := make([]string, 0)
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			tag := f.Tag.Get("json")
			if tag == "-" || tag == "" {
				tag = f.Tag.Get("bson")
			}
			s := strings.Split(tag, ",")[0]
			if s != "" && !slices.Contains(exclude, s) {
				fields = append(fields, s)
			}
		}
		return fields
	}
	return nil
}

func GetJsonFieldsTypes(o any, exclude []string) []string {
	rt := reflect.TypeOf(o)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() == reflect.Map {
		types := make([]string, 0)
		for s, v := range o.(map[string]any) {
			if !slices.Contains(exclude, s) {
				types = append(types, reflect.TypeOf(v).Kind().String())
			}
		}
		return types
	} else if rt.Kind() == reflect.Struct {
		types := make([]string, 0)
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			tag := f.Tag.Get("json")
			if tag == "-" || tag == "" {
				tag = f.Tag.Get("bson")
			}
			s := strings.Split(tag, ",")[0]
			if s != "" && !slices.Contains(exclude, s) {
				if f.Type.Kind() == reflect.Ptr {
					types = append(types, f.Type.Elem().Kind().String())
				} else {
					types = append(types, f.Type.Kind().String())
				}
			}
		}
		return types
	}
	return nil
}
