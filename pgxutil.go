package pgxutil

import (
	"context"
	"errors"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"
)

var errNullValue = errors.New("value is null")
var errNotFound = errors.New("no rows in result set")
var errMultipleRows = errors.New("more than one row found")

type Queryer interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

// SelectString selects a single string. Any PostgreSQL data type can be selected. The text format of the
// selected values will be returned. An error will be returned if no rows are found or a null value is found.
func SelectString(ctx context.Context, db Queryer, sql string, args ...interface{}) (string, error) {
	var v pgtype.GenericText
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	rows, _ := db.Query(ctx, sql, args...)

	rowCount := 0
	for rows.Next() {
		rows.Scan(&v)
		rowCount += 1
	}

	if rows.Err() != nil {
		return "", rows.Err()
	}

	if rowCount == 0 {
		return "", errNotFound
	}
	if rowCount > 1 {
		return "", errMultipleRows
	}

	if v.Status == pgtype.Null {
		return "", errNullValue
	}

	return v.String, nil
}

// SelectByteSlice selects a single byte slice. Any PostgreSQL data type can be selected. The binary format of the
// selected value will be returned.An error will be returned if no rows are found or a null value is found.
func SelectByteSlice(ctx context.Context, db Queryer, sql string, args ...interface{}) ([]byte, error) {
	var v pgtype.GenericBinary
	args = append([]interface{}{pgx.QueryResultFormats{pgx.BinaryFormatCode}}, args...)
	rows, _ := db.Query(ctx, sql, args...)

	rowCount := 0
	for rows.Next() {
		rows.Scan(&v)
		rowCount += 1
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	if rowCount == 0 {
		return nil, errNotFound
	}
	if rowCount > 1 {
		return nil, errMultipleRows
	}

	if v.Status == pgtype.Null {
		return nil, errNullValue
	}

	return v.Bytes, nil
}

// SelectInt64 selects a single int64. Any PostgreSQL value representable as an int64 can be selected. An error will be
// returned if no rows are found or a null value is found.
func SelectInt64(ctx context.Context, db Queryer, sql string, args ...interface{}) (int64, error) {
	var v pgtype.Int8
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	rows, _ := db.Query(ctx, sql, args...)

	rowCount := 0
	for rows.Next() {
		rows.Scan(&v)
		rowCount += 1
	}

	if rows.Err() != nil {
		return 0, rows.Err()
	}

	if rowCount == 0 {
		return 0, errNotFound
	}
	if rowCount > 1 {
		return 0, errMultipleRows
	}

	if v.Status == pgtype.Null {
		return 0, errNullValue
	}

	return v.Int, nil
}

// SelectFloat64 selects a single float64. Any PostgreSQL value representable as an float64 can be selected. However,
// precision is not guaranteed when converting formats (e.g. when selecting a numeric with more precision than a float
// can represent). An error will be returned if no rows are found or a null value is found.
func SelectFloat64(ctx context.Context, db Queryer, sql string, args ...interface{}) (float64, error) {
	var v pgtype.Float8
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	rows, _ := db.Query(ctx, sql, args...)

	rowCount := 0
	for rows.Next() {
		rows.Scan(&v)
		rowCount += 1
	}

	if rows.Err() != nil {
		return 0, rows.Err()
	}

	if rowCount == 0 {
		return 0, errNotFound
	}
	if rowCount > 1 {
		return 0, errMultipleRows
	}

	if v.Status == pgtype.Null {
		return 0, errNullValue
	}

	return v.Float, nil
}

// SelectDecimal selects a single decimal.Decimal. Any PostgreSQL value representable as an decimal can be selected.
// An error will be returned if no rows are found or a null value is found.
func SelectDecimal(ctx context.Context, db Queryer, sql string, args ...interface{}) (decimal.Decimal, error) {
	var v pgtype.GenericText
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	rows, _ := db.Query(ctx, sql, args...)

	rowCount := 0
	for rows.Next() {
		rows.Scan(&v)
		rowCount += 1
	}

	if rows.Err() != nil {
		return decimal.Decimal{}, rows.Err()
	}

	if rowCount == 0 {
		return decimal.Decimal{}, errNotFound
	}
	if rowCount > 1 {
		return decimal.Decimal{}, errMultipleRows
	}

	if v.Status == pgtype.Null {
		return decimal.Decimal{}, errNullValue
	}

	d, err := decimal.NewFromString(v.String)
	if err != nil {
		return decimal.Decimal{}, err
	}

	return d, nil
}
