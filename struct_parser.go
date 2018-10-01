package sqlxt

import (
	"reflect"
	"time"
)

type structParser struct {
	fields map[string][]reflect.Value
}

func newStructParser() *structParser {
	return &structParser{map[string][]reflect.Value{}}
}

func (p *structParser) AddField(t *tag, fieldValue reflect.Value) {
	// if ok, we already have such tag
	if field, ok := p.fields[t.string()]; ok {
		p.fields[t.string()] = append(field, fieldValue)
	} else {
		p.fields[t.string()] = []reflect.Value{fieldValue}
	}
}

func (p *structParser) Field(key string) (reflect.Value, bool) {
	fields, ok := p.fields[key]
	if !ok || len(fields) == 0 {
		return reflect.Value{}, false
	}

	p.fields[key] = fields[1:]
	return fields[0], true
}

func (p *structParser) Parse(value reflect.Value) {
	structType := value.Type()

	for i := 0; i < structType.NumField(); i++ {
		fieldValue := value.Field(i)

		// skip unexported fields
		if !fieldValue.CanSet() {
			continue
		}

		// skip non-tagged fields
		tag, ok := parseTag(structType.Field(i))
		if !ok {
			continue
		}
		tag.toLower()

		// if sql.scanner is implemented, add the field.
		if scannerImplemented(fieldValue) {
			p.AddField(tag, fieldValue)
			continue
		}

		// or the field type is time.Time
		switch fieldValue.Interface().(type) {
		case time.Time, *time.Time:
			p.AddField(tag, fieldValue)
			continue
		}

		fieldType := fieldValue.Type()
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}

		switch fieldType.Kind() {
		case reflect.Struct:
			if fieldValue.Kind() == reflect.Ptr && fieldValue.IsNil() {
				fieldValue.Set(reflect.New(fieldType))
				fieldValue = fieldValue.Elem()
			}
			p.Parse(fieldValue)
		default:
			p.AddField(tag, fieldValue)
		}
	}
}
