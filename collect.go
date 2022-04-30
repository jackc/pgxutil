package pgxutil

import (
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
)

func Collect[T any](rows pgx.Rows, scan func(rows pgx.Rows) (T, error)) ([]T, error) {
	defer rows.Close()

	slice := []T{}

	for rows.Next() {
		value, err := scan(rows)
		if err != nil {
			return nil, err
		}
		slice = append(slice, value)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return slice, nil
}

func Scan[T any](rows pgx.Rows) (T, error) {
	var value T
	err := rows.Scan(&value)
	return value, err
}

func ScanAddrOf[T any](rows pgx.Rows) (*T, error) {
	var value T
	err := rows.Scan(&value)
	return &value, err
}

func ScanMap(rows pgx.Rows) (map[string]any, error) {
	var value map[string]any
	err := rows.Scan((*mapRowScanner)(&value))
	return value, err
}

type mapRowScanner map[string]any

func (rs *mapRowScanner) ScanRow(rows pgx.Rows) error {
	values, err := rows.Values()
	if err != nil {
		return err
	}

	*rs = make(mapRowScanner, len(values))

	for i := range values {
		(*rs)[string(rows.FieldDescriptions()[i].Name)] = values[i]
	}

	return nil
}

func ScanStructPos[T any](rows pgx.Rows) (T, error) {
	var value T
	err := rows.Scan(&positionalStructRowScanner{ptrToStruct: &value})
	return value, err
}

func ScanAddrOfStructPos[T any](rows pgx.Rows) (*T, error) {
	var value T
	err := rows.Scan(&positionalStructRowScanner{ptrToStruct: &value})
	return &value, err
}

type positionalStructRowScanner struct {
	ptrToStruct any
}

func (rs *positionalStructRowScanner) ScanRow(rows pgx.Rows) error {
	dst := rs.ptrToStruct
	dstValue := reflect.ValueOf(dst)
	if dstValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dst not a pointer")
	}

	dstElemValue := dstValue.Elem()
	dstElemType := dstElemValue.Type()

	exportedFields := make([]int, 0, dstElemType.NumField())
	for i := 0; i < dstElemType.NumField(); i++ {
		sf := dstElemType.Field(i)
		if sf.PkgPath == "" {
			exportedFields = append(exportedFields, i)
		}
	}

	rowFieldCount := len(rows.RawValues())
	if rowFieldCount > len(exportedFields) {
		return fmt.Errorf("got %d values, but dst struct has only %d fields", rowFieldCount, len(exportedFields))
	}

	scanTargets := make([]any, rowFieldCount)
	for i := 0; i < rowFieldCount; i++ {
		scanTargets[i] = dstElemValue.Field(exportedFields[i]).Addr().Interface()
	}

	return rows.Scan(scanTargets...)
}
