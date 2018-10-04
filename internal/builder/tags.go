package builder

import (
	"reflect"
	"strings"
)

type tag string

func parseTag(field reflect.StructField) (*tag, bool) {
	value, ok := field.Tag.Lookup("sql")
	if !ok {
		t := tag(field.Name)
		return &t, true
	}
	if value == "-" {
		return nil, false
	}

	t := tag(value)
	return &t, true
}

func (t *tag) toLower() {
	*t = tag(strings.ToLower(t.string()))
}

func (t *tag) string() string {
	return string(*t)
}
