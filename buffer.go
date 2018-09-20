package sqlxt

import (
	"errors"
	"fmt"
	"reflect"
)

type buffer struct {
	Type      reflect.Type
	Value     reflect.Value
	Dimension int
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
		Type:      refType,
		Value:     refValue,
		Dimension: calculateDimension(refType)}, nil
}

func (b *buffer) OneRowExpected() bool {
	return b.Dimension <= 1
}

func (b *buffer) Next() (*buffer, error) {
	if b.Dimension <= 1 {
		return nil, errors.New("buffer dimension is <=1")
	}

	switch b.Type.Kind() {
	case reflect.Slice:
		sliceType := b.Type.Elem()
		b.Value.Set(reflect.Append(b.Value, reflect.New(sliceType)))

		lastElement := b.Value.Index(b.Value.Len() - 1)
		return newBuffer(lastElement.Addr().Interface())
	}

	return nil, errors.New("unsupported")
}

func (b *buffer) AddRow(data []reflect.Value, columns []string) error {
	if len(data) == 0 || len(columns) == 0 {
		return errors.New("insufficient data or columns")
	}
	if len(data) != len(columns) {
		return errors.New("len of data and columns does not match")
	}

	//TODO what happens if dimension > 1

	switch b.Type.Kind() {
	case reflect.Struct:
		return b.addToStruct(data, columns)
	}
	return errors.New("unsupported")
}

func (b *buffer) addToStruct(data []reflect.Value, columns []string) error {
	fieldMap := make(map[string]reflect.Value)
	for i := 0; i < b.Type.NumField(); i++ {
		tag, ok := parseTag(b.Type.Field(i))
		if !ok {
			continue
		}
		fieldMap[tag[0]] = b.Value.Field(i)
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
