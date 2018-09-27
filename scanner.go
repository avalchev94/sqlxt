package sqlxt

import (
	"database/sql"
	"fmt"
	"reflect"
)

// Scanner is a type that will Scan your query's result.
type Scanner struct {
	rows *sql.Rows
}

// NewScanner is a function for creating new Scanner.
// Have in mind that Scanner is always successfully created, but
// later Scan method could fail because of problems is 'rows'.
func NewScanner(rows *sql.Rows) *Scanner {
	return &Scanner{rows}
}

// Scan is the 'meat' of the package. It scan the 'rows' input into
// the 'dest' parameter. Dest variable could be:
// - primitive type(string, int, bool, interface{})
// - struct (with or without 'sql' tags);
// - map with key(int, string, interface{});
// - slice in combination with some of the above types;
func (s *Scanner) Scan(dest interface{}) error {
	if s.rows == nil {
		return fmt.Errorf(`sqlxt: sql "rows" is nil`)
	}

	if err := s.rows.Err(); err != nil {
		return err
	}

	builder, err := newBuilder(dest)
	if err != nil {
		return err
	}
	if builder.OneRowExpected() {
		if !s.rows.Next() {
			return sql.ErrNoRows
		}
		return s.scanOneRow(builder)
	}

	return s.scanAllRows(builder)
}

func (s *Scanner) scanAllRows(builder *builder) error {
	rowsCount := 0
	for s.rows.Next() {
		rowBuilder, err := builder.Next()
		if err != nil {
			return err
		}
		err = s.scanOneRow(rowBuilder)
		if err != nil {
			return err
		}

		rowsCount++
	}

	if rowsCount == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Scanner) scanOneRow(builder *builder) error {
	columnTypes, err := s.rows.ColumnTypes()
	if err != nil {
		return err
	}

	params, err := builder.BuildParameters(columnTypes)
	if err != nil {
		return err
	}

	result := reflect.ValueOf(s.rows.Scan).Call(params)
	// database/sql Scan returns only one variable - error
	if !result[0].IsNil() {
		return result[0].Interface().(error)
	}

	return builder.UpdateDestination(params, columnTypes)
}
