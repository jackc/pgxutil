package pgxutil

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var errNullValue = errors.New("value is null")
var errNotFound = errors.New("no rows in result set")
var errNoColumns = errors.New("no columns in result set")
var errMultipleColumns = errors.New("multiple columns in result set")
var errMultipleRows = errors.New("multiple rows in result set")

type Queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func selectColumn(ctx context.Context, db Queryer, sql string, args []any, rowFn func(pgx.Rows) error) error {
	return selectRows(ctx, db, sql, args, func(rows pgx.Rows) error {
		if len(rows.RawValues()) == 0 {
			rows.Close()
			return errNoColumns
		}
		if len(rows.RawValues()) > 1 {
			rows.Close()
			return errMultipleColumns
		}

		return rowFn(rows)
	})
}

func selectOneRow(ctx context.Context, db Queryer, sql string, args []any, rowFn func(pgx.Rows) error) error {
	rowCount := 0
	err := selectRows(ctx, db, sql, args, func(rows pgx.Rows) error {
		rowCount += 1
		return rowFn(rows)
	})
	if err != nil {
		return err
	}

	if rowCount == 0 {
		return errNotFound
	}
	if rowCount > 1 {
		return errMultipleRows
	}

	return nil
}

func selectRows(ctx context.Context, db Queryer, sql string, args []any, rowFn func(pgx.Rows) error) error {
	rows, _ := db.Query(ctx, sql, args...)

	for rows.Next() {
		err := rowFn(rows)
		if err != nil {
			rows.Close()
			return err
		}
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	return nil
}

// SelectMap selects a single row into a map. An error will be returned if no rows are found.
//
// Deprecated: Prefer SelectRow with pgx.RowToMap.
func SelectMap(ctx context.Context, db Queryer, sql string, args ...any) (map[string]any, error) {
	var v map[string]any
	err := selectOneRow(ctx, db, sql, args, func(rows pgx.Rows) error {
		return rows.Scan((*mapRowScanner)(&v))
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectAllMap selects rows into a map slice.
//
// Deprecated: Prefer Select with pgx.RowToMap.
func SelectAllMap(ctx context.Context, db Queryer, sql string, args ...any) ([]map[string]any, error) {
	var v []map[string]any
	err := selectRows(ctx, db, sql, args, func(rows pgx.Rows) error {
		var m map[string]any
		err := rows.Scan((*mapRowScanner)(&m))
		if err != nil {
			return err
		}

		v = append(v, m)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
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

// SelectStringMap selects a single row into a map where all values are strings. An error will be returned if no rows
// are found.
//
// Deprecated: Prefer SelectRow.
func SelectStringMap(ctx context.Context, db Queryer, sql string, args ...any) (map[string]string, error) {
	var v map[string]string
	args = append([]any{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectOneRow(ctx, db, sql, args, func(rows pgx.Rows) error {
		values := rows.RawValues()
		v = make(map[string]string, len(values))
		for i := range values {
			v[string(rows.FieldDescriptions()[i].Name)] = string(values[i])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectAllStringMap selects rows into a map slice where all values are strings.
//
// Deprecated: Prefer Select.
func SelectAllStringMap(ctx context.Context, db Queryer, sql string, args ...any) ([]map[string]string, error) {
	var v []map[string]string
	args = append([]any{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectRows(ctx, db, sql, args, func(rows pgx.Rows) error {
		values := rows.RawValues()
		m := make(map[string]string, len(values))
		for i := range values {
			m[string(rows.FieldDescriptions()[i].Name)] = string(values[i])
		}

		v = append(v, m)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectStruct selects a single row into struct dst. An error will be returned if no rows are found. The values are
// assigned positionally to the exported struct fields.
//
// Deprecated: Prefer SelectRow with pgx.RowToAddrOfStruct.
func SelectStruct(ctx context.Context, db Queryer, dst any, sql string, args ...any) error {
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

	err := selectOneRow(ctx, db, sql, args, func(rows pgx.Rows) error {
		rowFieldCount := len(rows.RawValues())
		if rowFieldCount > len(exportedFields) {
			return fmt.Errorf("got %d values, but dst struct has only %d fields", rowFieldCount, len(exportedFields))
		}

		scanTargets := make([]any, rowFieldCount)
		for i := 0; i < rowFieldCount; i++ {
			scanTargets[i] = dstElemValue.Field(exportedFields[i]).Addr().Interface()
		}

		return rows.Scan(scanTargets...)
	})
	if err != nil {
		return err
	}

	return nil
}

// SelectAllStruct selects rows into dst. dst must be a slice of struct or pointer to struct. The values are assigned
// positionally to the exported struct fields.
//
// Deprecated: Prefer Select with pgx.RowToAddrOfStruct.
func SelectAllStruct(ctx context.Context, db Queryer, dst any, sql string, args ...any) error {
	ptrSliceValue := reflect.ValueOf(dst)
	if ptrSliceValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dst not a pointer")
	}

	sliceType := ptrSliceValue.Elem().Type()
	if sliceType.Kind() != reflect.Slice {
		return fmt.Errorf("dst not a pointer to slice")
	}

	var isPtr bool
	sliceElemType := sliceType.Elem()
	var structType reflect.Type
	switch sliceElemType.Kind() {
	case reflect.Ptr:
		isPtr = true
		structType = sliceElemType.Elem()
	case reflect.Struct:
		structType = sliceElemType
	default:
		return fmt.Errorf("dst not a pointer to slice of struct or pointer to struct")
	}

	if structType.Kind() != reflect.Struct {
		return fmt.Errorf("dst not a pointer to slice of struct or pointer to struct")
	}

	exportedFields := make([]int, 0, structType.NumField())
	for i := 0; i < structType.NumField(); i++ {
		sf := structType.Field(i)
		if sf.PkgPath == "" {
			exportedFields = append(exportedFields, i)
		}
	}

	sliceValue := reflect.New(sliceType).Elem()

	err := selectRows(ctx, db, sql, args, func(rows pgx.Rows) error {
		rowFieldCount := len(rows.RawValues())
		if rowFieldCount > len(exportedFields) {
			return fmt.Errorf("got %d values, but dst struct has only %d fields", rowFieldCount, len(exportedFields))
		}

		var appendableValue reflect.Value
		var fieldableValue reflect.Value

		if isPtr {
			appendableValue = reflect.New(structType)
			fieldableValue = appendableValue.Elem()
		} else {
			appendableValue = reflect.New(structType).Elem()
			fieldableValue = appendableValue
		}

		scanTargets := make([]any, rowFieldCount)
		for i := 0; i < rowFieldCount; i++ {
			scanTargets[i] = fieldableValue.Field(exportedFields[i]).Addr().Interface()
		}

		err := rows.Scan(scanTargets...)
		if err != nil {
			return err
		}

		sliceValue = reflect.Append(sliceValue, appendableValue)

		return nil
	})
	if err != nil {
		return err
	}

	ptrSliceValue.Elem().Set(sliceValue)

	return nil
}

// SelectValue selects a single T. An error will be returned if no rows are found.
//
// Deprecated: Prefer SelectRow with pgx.RowTo.
func SelectValue[T any](ctx context.Context, db Queryer, sql string, args ...any) (T, error) {
	var v T
	err := selectOneRow(ctx, db, sql, args, func(rows pgx.Rows) error {
		if len(rows.RawValues()) == 0 {
			return errNoColumns
		}
		if len(rows.RawValues()) > 1 {
			return errMultipleColumns
		}

		return rows.Scan(&v)

	})
	if err != nil {
		// Explicitly returning zero value T instead of v because it is possible, though unlikely, that v was partially
		// written before an error occurred and the the caller of SelectValue is not checking the error.
		var zero T
		return zero, err
	}

	return v, nil
}

// SelectColumn selects a column of T.
//
// Deprecated: Prefer Select with pgx.RowTo.
func SelectColumn[T any](ctx context.Context, db Queryer, sql string, args ...any) ([]T, error) {
	column := []T{}

	err := selectRows(ctx, db, sql, args, func(rows pgx.Rows) error {
		if len(rows.RawValues()) == 0 {
			return errNoColumns
		}
		if len(rows.RawValues()) > 1 {
			return errMultipleColumns
		}

		var v T
		err := rows.Scan(&v)
		if err != nil {
			return err
		}

		column = append(column, v)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return column, nil
}
