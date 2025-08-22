package utils

func MapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func shortenLongField(operationMap map[string]any, field string) {
	longValue, ok := operationMap[field].(string)
	if ok {
		operationMap[field] = ShortenString(longValue)
	}
}

func ShortenLongFields(operationMap map[string]any, fields []string) {
	for _, field := range fields {
		shortenLongField(operationMap, field)
	}
}
