package sqlxt

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/lib/pq"
)

type builder struct {
	Type      reflect.Type
	Value     reflect.Value
	Dimension int
}

func newBuilder(i interface{}) (*builder, error) {
	if i == nil {
		return nil, fmt.Errorf(`sqlxt: "dest" parameter is nil`)
	}

	refValue := reflect.ValueOf(i)
	refType := reflect.TypeOf(i)

	if !scannerImplemented(refValue) {
		if refType.Kind() == reflect.Ptr {
			refType = refType.Elem()
			refValue = refValue.Elem()
		}

		if !refValue.CanSet() {
			return nil, fmt.Errorf("sqlxt: %v should be settable", refType)
		}
	}

	return &builder{
		Type:      refType,
		Value:     refValue,
		Dimension: calculateDimension(refType)}, nil
}

func (b *builder) OneRowExpected() bool {
	return b.Dimension <= 1
}

func (b *builder) Next() (*builder, error) {
	if b.Dimension <= 1 {
		return nil, fmt.Errorf("sqlxt: dimension is <=1")
	}

	switch b.Type.Kind() {
	case reflect.Slice:
		sliceType := b.Type.Elem()

		if sliceType.Kind() == reflect.Ptr {
			newElement := reflect.New(sliceType.Elem())
			b.Value.Set(reflect.Append(b.Value, newElement))

			return newBuilder(newElement.Interface())
		}

		newElement := reflect.New(sliceType).Elem()
		b.Value.Set(reflect.Append(b.Value, newElement))

		lastElement := b.Value.Index(b.Value.Len() - 1)
		return newBuilder(lastElement.Addr().Interface())
	}

	return nil, fmt.Errorf("sqlxt: %v is not supported for multiple rows scanning", b.Type)
}

func (b *builder) BuildParameters(columns []*sql.ColumnType) ([]reflect.Value, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("columns == 0")
	}

	if b.Dimension > 1 {
		return nil, fmt.Errorf("sqlxt: dimension should be 1 or 0")
	}

	parameters := []reflect.Value{b.Value}
	switch b.Type.Kind() {
	case reflect.Struct:
		parameters = b.structParameters(columns)
	case reflect.Map, reflect.Slice:
		valueType := b.Type.Elem()
		parameters = make([]reflect.Value, len(columns))
		for i := range parameters {
			parameters[i] = reflect.New(valueType)
		}
	}

	//TODO: move that in separate file; handle more databases
	for i, p := range parameters {
		if p.Kind() == reflect.Slice {
			parameters[i] = reflect.ValueOf(pq.Array(parameters[i].Addr().Interface()))
		}
	}

	for i, p := range parameters {
		if p.CanAddr() {
			parameters[i] = parameters[i].Addr()
		}
	}

	return parameters, nil
}

func (b *builder) UpdateDestination(params []reflect.Value, columns []*sql.ColumnType) error {
	if len(params) == 0 {
		return fmt.Errorf("sqlxt: params == 0")
	}

	switch b.Type.Kind() {
	case reflect.Map:
		return b.updateMap(params, columns)
	case reflect.Slice:
		for _, p := range params {
			b.Value.Set(reflect.Append(b.Value, p.Elem()))
		}
	}
	return nil
}

func (b *builder) structParameters(columns []*sql.ColumnType) []reflect.Value {
	parser := newStructParser()
	parser.Parse(b.Value)

	parameters := make([]reflect.Value, len(columns))
	for i, col := range columns {
		if field, ok := parser.Field(strings.ToLower(col.Name())); ok {
			parameters[i] = field
		}
	}

	// add valid value for the missing parameters
	for i, p := range parameters {
		if !p.IsValid() {
			parameters[i] = reflect.New(columns[i].ScanType())
		}
	}

	return parameters
}

func (b *builder) updateMap(params []reflect.Value, columns []*sql.ColumnType) error {
	keyType := b.Type.Key()

	if b.Value.IsNil() {
		b.Value.Set(reflect.MakeMap(b.Type))
	}

	// check what we can use for map key
	useColumn, useIndex := false, false
	if reflect.TypeOf("").AssignableTo(keyType) {
		useColumn = true
	} else if reflect.TypeOf(1).AssignableTo(keyType) {
		useIndex = true
	} else {
		return fmt.Errorf("sqlxt: %v is not suitable type for map key", keyType)
	}

	for i, col := range columns {
		var key reflect.Value
		switch {
		case useColumn:
			key = reflect.ValueOf(col.Name())
		case useIndex:
			key = reflect.ValueOf(i)
		}
		b.Value.SetMapIndex(key, params[i].Elem())
	}
	return nil
}

func calculateDimension(t reflect.Type) int {
	dimension := 0

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.Slice, reflect.Map:
		dimension = 1 + calculateDimension(t.Elem())
	case reflect.Struct:
		dimension++
	}
	return dimension
}

func scannerImplemented(v reflect.Value) bool {
	_, ok := v.Interface().(sql.Scanner)
	return ok
}
