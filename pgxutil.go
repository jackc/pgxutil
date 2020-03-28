package pgxutil

import (
	"context"
	"errors"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgtype"
	gofrs "github.com/jackc/pgtype/ext/gofrs-uuid"
	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"
)

var errNullValue = errors.New("value is null")
var errNotFound = errors.New("no rows in result set")
var errNoColumns = errors.New("no columns in result set")
var errMultipleColumns = errors.New("multiple columns in result set")
var errMultipleRows = errors.New("multiple rows in result set")

type Queryer interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

func selectOne(ctx context.Context, db Queryer, sql string, args []interface{}, rowFn func(pgx.Rows) error) error {
	rows, _ := db.Query(ctx, sql, args...)

	rowCount := 0
	for rows.Next() {
		if len(rows.RawValues()) == 0 {
			rows.Close()
			return errNoColumns
		}
		if len(rows.RawValues()) > 1 {
			rows.Close()
			return errMultipleColumns
		}
		if rows.RawValues()[0] == nil {
			rows.Close()
			return errNullValue
		}

		err := rowFn(rows)
		if err != nil {
			rows.Close()
			return err
		}
		rowCount += 1
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	if rowCount == 0 {
		return errNotFound
	}
	if rowCount > 1 {
		return errMultipleRows
	}

	return nil

}

// SelectString selects a single string. Any PostgreSQL data type can be selected. The text format of the
// selected values will be returned. An error will be returned if no rows are found or a null value is found.
func SelectString(ctx context.Context, db Queryer, sql string, args ...interface{}) (string, error) {
	var v string
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectOne(ctx, db, sql, args, func(rows pgx.Rows) error {
		v = string(rows.RawValues()[0])
		return nil
	})
	if err != nil {
		return "", err
	}

	return v, nil
}

// SelectByteSlice selects a single byte slice. Any PostgreSQL data type can be selected. The binary format of the
// selected value will be returned.An error will be returned if no rows are found or a null value is found.
func SelectByteSlice(ctx context.Context, db Queryer, sql string, args ...interface{}) ([]byte, error) {
	var v []byte
	args = append([]interface{}{pgx.QueryResultFormats{pgx.BinaryFormatCode}}, args...)
	err := selectOne(ctx, db, sql, args, func(rows pgx.Rows) error {
		v = rows.RawValues()[0]
		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectInt64 selects a single int64. Any PostgreSQL value representable as an int64 can be selected. An error will be
// returned if no rows are found or a null value is found.
func SelectInt64(ctx context.Context, db Queryer, sql string, args ...interface{}) (int64, error) {
	var v pgtype.Int8
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectOne(ctx, db, sql, args, func(rows pgx.Rows) error {
		return rows.Scan(&v)
	})
	if err != nil {
		return 0, err
	}

	return v.Int, nil
}

// SelectFloat64 selects a single float64. Any PostgreSQL value representable as an float64 can be selected. However,
// precision is not guaranteed when converting formats (e.g. when selecting a numeric with more precision than a float
// can represent). An error will be returned if no rows are found or a null value is found.
func SelectFloat64(ctx context.Context, db Queryer, sql string, args ...interface{}) (float64, error) {
	var v pgtype.Float8
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectOne(ctx, db, sql, args, func(rows pgx.Rows) error {
		return rows.Scan(&v)
	})
	if err != nil {
		return 0, err
	}

	return v.Float, nil
}

// SelectDecimal selects a single decimal.Decimal. Any PostgreSQL value representable as an decimal can be selected.
// An error will be returned if no rows are found or a null value is found.
func SelectDecimal(ctx context.Context, db Queryer, sql string, args ...interface{}) (decimal.Decimal, error) {
	var v pgtype.GenericText
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectOne(ctx, db, sql, args, func(rows pgx.Rows) error {
		return rows.Scan(&v)
	})
	if err != nil {
		return decimal.Decimal{}, err
	}

	d, err := decimal.NewFromString(v.String)
	if err != nil {
		return decimal.Decimal{}, err
	}

	return d, nil
}

// SelectUUID selects a single uuid.UUID. An error will be returned if no rows are found or a null value is found.
func SelectUUID(ctx context.Context, db Queryer, sql string, args ...interface{}) (uuid.UUID, error) {
	var v gofrs.UUID
	err := selectOne(ctx, db, sql, args, func(rows pgx.Rows) error {
		return rows.Scan(&v)
	})
	if err != nil {
		return uuid.Nil, err
	}

	return v.UUID, nil
}

// SelectValue selects a single value of unspecified type. An error will be returned if no rows are found or a null
// value is found.
func SelectValue(ctx context.Context, db Queryer, sql string, args ...interface{}) (interface{}, error) {
	var v interface{}
	err := selectOne(ctx, db, sql, args, func(rows pgx.Rows) error {
		values, err := rows.Values()
		if err != nil {
			return err
		}
		v = values[0]
		return err
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}
