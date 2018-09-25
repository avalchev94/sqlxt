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
		b.Value.Set(reflect.Append(b.Value, reflect.New(sliceType).Elem()))

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
	if b.Dimension > 1 {
		return errors.New("buffer's dimension > 1")
	}

	switch b.Type.Kind() {
	case reflect.Struct:
		return b.addToStruct(data, columns)
	case reflect.Slice:
		return b.addToSlice(data)
	case reflect.Map:
		return b.addToMap(data, columns)
	}
	return b.addToPrimitive(data)
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
	//TODO: typesafety
	for i, col := range columns {
		field, ok := fieldMap[col]
		if !ok {
			continue
		}
		field.Set(data[i].Elem())
	}
	return nil
}

func (b *buffer) addToSlice(data []reflect.Value) error {
	sliceType := b.Type.Elem()
	for _, d := range data {
		dataType := d.Type().Elem()
		switch {
		case dataType.AssignableTo(sliceType):
			b.Value.Set(reflect.Append(b.Value, d.Elem()))
		case dataType.ConvertibleTo(sliceType):
			converted := d.Elem().Convert(sliceType)
			b.Value.Set(reflect.Append(b.Value, converted))
		default:
			return fmt.Errorf("%v is not assignable/convertible to %v", dataType, sliceType)
		}
	}
	return nil
}

func (b *buffer) addToPrimitive(data []reflect.Value) error {
	dataType := data[0].Type().Elem()

	switch {
	case dataType.AssignableTo(b.Type):
		b.Value.Set(data[0].Elem())
	case dataType.ConvertibleTo(b.Type):
		converted := data[0].Elem().Convert(b.Type)
		b.Value.Set(converted)
	default:
		return fmt.Errorf("%v is not assignable/convertible to %v", dataType, b.Type)
	}

	return nil
}

func (b *buffer) addToMap(data []reflect.Value, columns []string) error {
	keyType := b.Type.Key()

	// check what we can use for map key
	useColumn, useIndex := false, false
	if reflect.TypeOf("").AssignableTo(keyType) {
		useColumn = true
	} else if reflect.TypeOf(1).AssignableTo(keyType) {
		useIndex = true
	} else {
		return fmt.Errorf("%v is not suitable type for map key", keyType)
	}

	// if the map is not initialized, initialize it
	if b.Value.IsNil() {
		b.Value.Set(reflect.MakeMap(b.Type))
	}

	valueType := b.Type.Elem()
	for i, d := range data {
		dType := d.Elem().Type()
		if !dType.AssignableTo(valueType) {
			return fmt.Errorf("%v can't be assigned to map type %v", dType, valueType)
		}

		switch {
		case useColumn:
			b.Value.SetMapIndex(reflect.ValueOf(columns[i]), d.Elem())
		case useIndex:
			b.Value.SetMapIndex(reflect.ValueOf(i), d.Elem())
		}
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
