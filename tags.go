package sqlxt

import (
	"reflect"
	"strings"
)

func parseTag(field reflect.StructField) ([]string, bool) {
	tag, ok := field.Tag.Lookup("sql")
	if !ok {
		return []string{field.Name}, true
	}
	if tag == "-" {
		return nil, false
	}
	return strings.Split(tag, ","), true
}
