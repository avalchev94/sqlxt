package builder

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/lib/pq"
)

type Builder struct {
	Type      reflect.Type
	Value     reflect.Value
	Dimension int
}

func New(dest interface{}) (*Builder, error) {
	switch {
	case dest == nil:
		return nil, fmt.Errorf("builder: parameter is nil")
	case reflect.ValueOf(dest).Kind() != reflect.Ptr:
		return nil, fmt.Errorf("builder: parameter should be passed by addr(pointer)")
	}

	refValue := reflect.ValueOf(dest)
	refType := reflect.TypeOf(dest)

	if !scannerImplemented(refValue) {
		refValue = refValue.Elem()
		refType = refType.Elem()
	}

	return &Builder{
		Type:      refType,
		Value:     refValue,
		Dimension: calculateDimension(refType)}, nil
}

func (b *Builder) OneRowExpected() bool {
	return b.Dimension <= 1
}

func (b *Builder) Next() (*Builder, error) {
	if b.Dimension <= 1 {
		return nil, fmt.Errorf("builder: Next should be used when dimension > 1")
	}

	switch b.Type.Kind() {
	case reflect.Slice, reflect.Chan:
		var element reflect.Value
		var elementType = b.Type.Elem()

		switch elementType.Kind() {
		case reflect.Ptr:
			element = reflect.New(elementType.Elem())
		default:
			element = reflect.New(elementType)
		}
		return New(element.Interface())
	default:
		return nil, fmt.Errorf("builder: Next is not supported on %v", b.Type)
	}
}

func (b *Builder) Add(builder *Builder) error {
	if b.Dimension <= 1 {
		return fmt.Errorf("sqlxt: Add should be used when dimension > 1")
	}

	retrieveElement := func(builder *Builder) reflect.Value {
		wantedType := b.Type.Elem()
		element := builder.Value

		if wantedType.Kind() != element.Kind() {
			if wantedType.Kind() == reflect.Ptr {
				element = element.Addr()
			} else {
				element = element.Elem()
			}
		}

		return element
	}

	switch b.Type.Kind() {
	case reflect.Slice:
		b.Value.Set(reflect.Append(b.Value, retrieveElement(builder)))
	case reflect.Chan:
		b.Value.Send(retrieveElement(builder))
	default:
		return fmt.Errorf("builder: Add is not supported on %v", b.Type)
	}

	return nil
}

func (b *Builder) Parameters(columns []*sql.ColumnType) ([]reflect.Value, error) {
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
		parameterType := b.Type.Elem()
		parameters = make([]reflect.Value, len(columns))
		for i := range parameters {
			parameters[i] = reflect.New(parameterType)
		}
	case reflect.Chan:
		return nil, fmt.Errorf("sqlxt: still not supported")
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

func (b *Builder) structParameters(columns []*sql.ColumnType) []reflect.Value {
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

func (b *Builder) Update(params []reflect.Value, columns []*sql.ColumnType) error {
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
	case reflect.Chan:
		return fmt.Errorf("sqlxt: still not supported")
	}
	return nil
}

func (b *Builder) updateMap(params []reflect.Value, columns []*sql.ColumnType) error {
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

	switch t.Kind() {
	case reflect.Ptr:
		return calculateDimension(t.Elem())
	case reflect.Slice, reflect.Map, reflect.Chan:
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
