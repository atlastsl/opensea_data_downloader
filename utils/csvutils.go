package utils

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/gocarina/gocsv"
)

type Writer struct {
	Delimiter rune
	Quote     rune
	UseCRLF   bool
	w         *bufio.Writer
}

func newCsvWriter(w io.Writer) *Writer {
	return &Writer{
		Delimiter: ';',
		Quote:     '"',
		UseCRLF:   false,
		w:         bufio.NewWriter(w),
	}
}

func csvValidDelim(r rune) bool {
	return r != 0 && r != '"' && r != '\r' && r != '\n' && utf8.ValidRune(r) && r != utf8.RuneError
}

var errCsvInvalidDelim = errors.New("csv: invalid field or comment delimiter")

func (w *Writer) csvFieldNeedsQuotes(field string, fieldType string, quoteStringOnly bool) bool {
	if quoteStringOnly {
		if strings.Contains(fieldType, "int") || strings.Contains(fieldType, "float") || strings.Contains(fieldType, "bool") {
			return false
		}
		return true
	}

	if field == "" {
		return false
	}

	if field == `\.` {
		return true
	}

	if w.Delimiter < utf8.RuneSelf {
		for i := 0; i < len(field); i++ {
			c := field[i]
			if c == '\n' || c == '\r' || c == '"' || c == byte(w.Delimiter) {
				return true
			}
		}
	} else {
		if strings.ContainsRune(field, w.Delimiter) || strings.ContainsAny(field, "\"\r\n") {
			return true
		}
	}

	r1, _ := utf8.DecodeRuneInString(field)
	return unicode.IsSpace(r1)
}

func (w *Writer) csvWrite(record, types []string, quoteStringOnly bool) error {
	if !csvValidDelim(w.Delimiter) {
		return errCsvInvalidDelim
	}

	for n, field := range record {
		if n > 0 {
			if _, err := w.w.WriteRune(w.Delimiter); err != nil {
				return err
			}
		}
		fType := ""
		if types != nil && len(types) > n {
			fType = types[n]
		}

		// If we don't have to have a quoted field then just
		// write out the field and continue to the next field.
		if !w.csvFieldNeedsQuotes(field, fType, quoteStringOnly) {
			if _, err := w.w.WriteString(field); err != nil {
				return err
			}
			continue
		}

		if err := w.w.WriteByte('"'); err != nil {
			return err
		}
		for len(field) > 0 {
			// Search for special characters.
			i := strings.IndexAny(field, "\"\r\n")
			if i < 0 {
				i = len(field)
			}

			// Copy verbatim everything before the special character.
			if _, err := w.w.WriteString(field[:i]); err != nil {
				return err
			}
			field = field[i:]

			// Encode the special character.
			if len(field) > 0 {
				var err error
				switch field[0] {
				case '"':
					_, err = w.w.WriteString(`""`)
				case '\r':
					if !w.UseCRLF {
						err = w.w.WriteByte('\r')
					}
				case '\n':
					if w.UseCRLF {
						_, err = w.w.WriteString("\r\n")
					} else {
						err = w.w.WriteByte('\n')
					}
				}
				field = field[1:]
				if err != nil {
					return err
				}
			}
		}
		if err := w.w.WriteByte('"'); err != nil {
			return err
		}
	}
	var err error
	if w.UseCRLF {
		_, err = w.w.WriteString("\r\n")
	} else {
		err = w.w.WriteByte('\n')
	}
	return err
}

func (w *Writer) csvFlush() {
	w.w.Flush()
}

func (w *Writer) csvError() error {
	_, err := w.w.Write(nil)
	return err
}

func WriteInCsv1(filename string, data []any) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	err = gocsv.MarshalFile(&data, file)
	if err != nil {
		return err
	}
	return nil
}

func WriteInCsv2(filename string, data []map[string]any, _headers, _types []string) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := newCsvWriter(file)
	defer writer.csvFlush()

	var headers = make([]string, 0)
	if _headers != nil {
		for _, k := range _headers {
			headers = append(headers, k)
		}
	} else {
		if len(data) > 0 {
			for k := range data[0] {
				headers = append(headers, k)
			}
		}
	}

	var types = make([]string, 0)
	if _types != nil {
		types = _types
	} else {
		if len(data) > 0 {
			for k := range data[0] {
				types = append(types, reflect.TypeOf(k).Kind().String())
			}
		}
	}

	err = writer.csvWrite(headers, nil, false)
	if err != nil {
		return err
	}

	for _, recordMap := range data {
		var row []string
		for _, header := range headers {
			value := recordMap[header]
			row = append(row, fmt.Sprint(value))
		}
		err = writer.csvWrite(row, types, true)
		if err != nil {
			return err
		}
	}
	return nil
}
