package utils

func ShortenString(str string) string {
	if len(str) > 20 {
		return str[0:6] + "..." + str[(len(str)-4):]
	}
	return str
}
