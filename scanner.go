package sqlxt

import (
	"database/sql"
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
	if err := s.rows.Err(); err != nil {
		return err
	}

	buffer, err := newBuffer(dest)
	if err != nil {
		return err
	}

	if buffer.OneRowExpected() {
		if !s.rows.Next() {
			return s.rows.Err()
		}
		return s.scanOneRow(buffer)
	}

	return s.scanAllRows(buffer)
}

func (s *Scanner) scanAllRows(buffer *buffer) error {
	for s.rows.Next() {
		rowBuffer, err := buffer.Next()
		if err != nil {
			return err
		}
		err = s.scanOneRow(rowBuffer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Scanner) scanOneRow(buffer *buffer) error {
	columnTypes, err := s.rows.ColumnTypes()
	if err != nil {
		return err
	}

	rowData := make([]reflect.Value, len(columnTypes))
	for i, t := range columnTypes {
		rowData[i] = reflect.New(t.ScanType())
	}

	result := reflect.ValueOf(s.rows.Scan).Call(rowData)
	// database/sql Scan returns only one variable - error
	if !result[0].IsNil() {
		return result[0].Interface().(error)
	}

	columns, err := s.rows.Columns()
	if err != nil {
		return err
	}

	return buffer.AddRow(rowData, columns)
}
