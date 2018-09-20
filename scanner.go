package sqlxt

import (
	"database/sql"
	"reflect"
)

type Scanner struct {
	rows *sql.Rows
}

func NewScanner(rows *sql.Rows) *Scanner {
	return &Scanner{rows}
}

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
	for row := 0; s.rows.Next(); row++ {
		rowBuffer, err := buffer.Index(row)
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

	args := make([]reflect.Value, len(columnTypes))
	for i, t := range columnTypes {
		args[i] = reflect.New(t.ScanType())
	}

	result := reflect.ValueOf(s.rows.Scan).Call(args)
	// database/sql Scan returns only one variable - error
	if !result[0].IsNil() {
		return result[0].Interface().(error)
	}

	columns, err := s.rows.Columns()
	if err != nil {
		return err
	}

	return buffer.MapRow(args, columns)
}
