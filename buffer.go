package sqlxt

import (
	"errors"
	"fmt"
	"reflect"
)

type buffer struct {
	refType   reflect.Type
	refValue  reflect.Value
	dimension int
}

func newBuffer(i interface{}) (*buffer, error) {
	refValue := reflect.ValueOf(i)
	refType := reflect.TypeOf(i)

	if !isAllowed(refType.Kind()) {
		return nil, fmt.Errorf("%v is not supported type", refType)
	}

	// if pointer, get the value and the type of the pointer
	if refType.Kind() == reflect.Ptr {
		refValue = refValue.Elem()
		refType = refType.Elem()
	}

	return &buffer{
		refType:   refType,
		refValue:  refValue,
		dimension: calculateDimension(refType)}, nil
}

func (b *buffer) OneRowExpected() bool {
	return b.dimension <= 1
}

func (b *buffer) Index(index int) (*buffer, error) {
	return nil, nil
}

func (b *buffer) MapRow(data []reflect.Value, columns []string) error {
	if len(data) == 0 || len(columns) == 0 {
		return errors.New("insufficient data or columns")
	}
	if len(data) != len(columns) {
		return errors.New("len of data and columns does not match")
	}

	//TODO what happens if dimension > 1

	switch b.refType.Kind() {
	case reflect.Struct:
		return b.mapToStruct(data, columns)
	}
	return errors.New("unsupported")
}

func (b *buffer) mapToStruct(data []reflect.Value, columns []string) error {
	fieldMap := make(map[string]reflect.Value)
	for i := 0; i < b.refType.NumField(); i++ {
		tag, ok := parseTag(b.refType.Field(i))
		if !ok {
			continue
		}
		fieldMap[tag[0]] = b.refValue.Field(i)
	}

	for i, col := range columns {
		field, ok := fieldMap[col]
		if !ok {
			continue
		}

		field.Set(data[i].Elem())
	}

	return nil
}

func isAllowed(kind reflect.Kind) bool {
	return kind == reflect.Ptr || kind == reflect.Map || kind == reflect.Slice
}

func calculateDimension(t reflect.Type) int {
	dimension := 0
	switch t.Kind() {
	case reflect.Slice, reflect.Map:
		dimension = 1 + calculateDimension(t.Elem())
	case reflect.Struct:
		dimension++
	}
	return dimension
}
