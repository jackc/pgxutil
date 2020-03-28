package pgxutil

import (
	"context"
	"errors"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

var ErrNullValue = errors.New("value is null")

type QueryRower interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// SelectString selects a single string. Any PostgreSQL data type can be selected. The text format of the
// selected values will be returned. An error will be returned if no rows are found or a null value is found.
func SelectString(ctx context.Context, db QueryRower, sql string, args ...interface{}) (string, error) {
	var v pgtype.GenericText
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := db.QueryRow(ctx, sql, args...).Scan(&v)
	if err != nil {
		return "", err
	}
	if v.Status == pgtype.Null {
		return "", ErrNullValue
	}
	return v.String, nil
}

// SelectByteSlice selects a single byte slice. Any PostgreSQL data type can be selected. The binary format of the
// selected value will be returned.An error will be returned if no rows are found or a null value is found.
func SelectByteSlice(ctx context.Context, db QueryRower, sql string, args ...interface{}) ([]byte, error) {
	var v pgtype.GenericBinary
	args = append([]interface{}{pgx.QueryResultFormats{pgx.BinaryFormatCode}}, args...)
	err := db.QueryRow(ctx, sql, args...).Scan(&v)
	if err != nil {
		return nil, err
	}
	if v.Status == pgtype.Null {
		return nil, ErrNullValue
	}
	return v.Bytes, nil
}

// SelectInt16 selects a single int16. An error will be returned if no rows are found or a null value is found.
func SelectInt16(ctx context.Context, db QueryRower, sql string, args ...interface{}) (int16, error) {
	var v pgtype.Int2
	err := db.QueryRow(ctx, sql, args...).Scan(&v)
	if err != nil {
		return 0, err
	}
	if v.Status == pgtype.Null {
		return 0, ErrNullValue
	}
	return v.Int, nil
}
