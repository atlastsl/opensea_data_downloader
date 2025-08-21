package helpers

import (
	"fmt"
	"time"
)

func Logging(loggingPrefix, line string) {
	println(fmt.Sprintf("[%s] // (Opensea DL) %s // %s", time.Now().Format(time.RFC3339), loggingPrefix, line))
}
