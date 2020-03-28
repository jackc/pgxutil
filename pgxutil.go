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

func selectOneValueNotNull(ctx context.Context, db Queryer, sql string, args []interface{}, rowFn func(pgx.Rows) error) error {
	return selectOneValue(ctx, db, sql, args, func(rows pgx.Rows) error {
		if rows.RawValues()[0] == nil {
			rows.Close()
			return errNullValue
		}

		return rowFn(rows)
	})
}

func selectOneValue(ctx context.Context, db Queryer, sql string, args []interface{}, rowFn func(pgx.Rows) error) error {
	return selectOneRow(ctx, db, sql, args, func(rows pgx.Rows) error {
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

func selectColumnNotNull(ctx context.Context, db Queryer, sql string, args []interface{}, rowFn func(pgx.Rows) error) error {
	return selectRows(ctx, db, sql, args, func(rows pgx.Rows) error {
		if rows.RawValues()[0] == nil {
			rows.Close()
			return errNullValue
		}

		return rowFn(rows)
	})
}

func selectColumn(ctx context.Context, db Queryer, sql string, args []interface{}, rowFn func(pgx.Rows) error) error {
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

func selectOneRow(ctx context.Context, db Queryer, sql string, args []interface{}, rowFn func(pgx.Rows) error) error {
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

func selectRows(ctx context.Context, db Queryer, sql string, args []interface{}, rowFn func(pgx.Rows) error) error {
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

// SelectString selects a single string. Any PostgreSQL data type can be selected. The text format of the
// selected values will be returned. An error will be returned if no rows are found or a null value is found.
func SelectString(ctx context.Context, db Queryer, sql string, args ...interface{}) (string, error) {
	var v string
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectOneValueNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
		v = string(rows.RawValues()[0])
		return nil
	})
	if err != nil {
		return "", err
	}

	return v, nil
}

// SelectStringColumn selects a column of strings. Any PostgreSQL data type can be selected. The text format of the
// selected values will be returned. An error will be returned a null value is found.
func SelectStringColumn(ctx context.Context, db Queryer, sql string, args ...interface{}) ([]string, error) {
	var v []string
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectColumnNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
		v = append(v, string(rows.RawValues()[0]))
		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectByteSlice selects a single byte slice. Any PostgreSQL data type can be selected. The binary format of the
// selected value will be returned. An error will be returned if no rows are found or a null value is found.
func SelectByteSlice(ctx context.Context, db Queryer, sql string, args ...interface{}) ([]byte, error) {
	var v []byte
	args = append([]interface{}{pgx.QueryResultFormats{pgx.BinaryFormatCode}}, args...)
	err := selectOneValueNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
		v = rows.RawValues()[0]
		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectByteSliceColumn selects a column byte slice. Any PostgreSQL data type can be selected. The binary format of the
// selected value will be returned. An error will be returned if a null value is found.
func SelectByteSliceColumn(ctx context.Context, db Queryer, sql string, args ...interface{}) ([][]byte, error) {
	var v [][]byte
	args = append([]interface{}{pgx.QueryResultFormats{pgx.BinaryFormatCode}}, args...)
	err := selectColumnNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
		v = append(v, rows.RawValues()[0])
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
	err := selectOneValueNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
		return rows.Scan(&v)
	})
	if err != nil {
		return 0, err
	}

	return v.Int, nil
}

// SelectInt64Column selects a column of int64. Any PostgreSQL value representable as an int64 can be selected. An error
// will be returned if null value is found.
func SelectInt64Column(ctx context.Context, db Queryer, sql string, args ...interface{}) ([]int64, error) {
	var v []int64
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectColumnNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
		var i8 pgtype.Int8
		err := rows.Scan(&i8)
		if err != nil {
			return err
		}
		v = append(v, i8.Int)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectFloat64 selects a single float64. Any PostgreSQL value representable as an float64 can be selected. However,
// precision is not guaranteed when converting formats (e.g. when selecting a numeric with more precision than a float
// can represent). An error will be returned if no rows are found or a null value is found.
func SelectFloat64(ctx context.Context, db Queryer, sql string, args ...interface{}) (float64, error) {
	var v pgtype.Float8
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectOneValueNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
		return rows.Scan(&v)
	})
	if err != nil {
		return 0, err
	}

	return v.Float, nil
}

// SelectFloat64Column selects a single float64. Any PostgreSQL value representable as an float64 can be selected. However,
// precision is not guaranteed when converting formats (e.g. when selecting a numeric with more precision than a float
// can represent). An error will be returned if no rows are found or a null value is found.
func SelectFloat64Column(ctx context.Context, db Queryer, sql string, args ...interface{}) ([]float64, error) {
	var v []float64
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectColumnNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
		var f8 pgtype.Float8
		err := rows.Scan(&f8)
		if err != nil {
			return err
		}
		v = append(v, f8.Float)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectDecimal selects a single decimal.Decimal. Any PostgreSQL value representable as an decimal can be selected.
// An error will be returned if no rows are found or a null value is found.
func SelectDecimal(ctx context.Context, db Queryer, sql string, args ...interface{}) (decimal.Decimal, error) {
	var v pgtype.GenericText
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectOneValueNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
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

// SelectDecimalColumn selects a column of decimal.Decimal. Any PostgreSQL value representable as an decimal can be
// selected. An error will be returned if a null value is found.
func SelectDecimalColumn(ctx context.Context, db Queryer, sql string, args ...interface{}) ([]decimal.Decimal, error) {
	var v []decimal.Decimal
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
	err := selectColumnNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
		var t pgtype.GenericText
		err := rows.Scan(&t)
		if err != nil {
			return err
		}

		d, err := decimal.NewFromString(t.String)
		if err != nil {
			return err
		}

		v = append(v, d)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectUUID selects a single uuid.UUID. An error will be returned if no rows are found or a null value is found.
func SelectUUID(ctx context.Context, db Queryer, sql string, args ...interface{}) (uuid.UUID, error) {
	var v gofrs.UUID
	err := selectOneValueNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
		return rows.Scan(&v)
	})
	if err != nil {
		return uuid.Nil, err
	}

	return v.UUID, nil
}

// SelectUUID selects a column of uuid.UUID. An error will be returned if a null value is found.
func SelectUUIDColumn(ctx context.Context, db Queryer, sql string, args ...interface{}) ([]uuid.UUID, error) {
	var v []uuid.UUID
	err := selectColumnNotNull(ctx, db, sql, args, func(rows pgx.Rows) error {
		var u gofrs.UUID
		err := rows.Scan(&u)
		if err != nil {
			return err
		}
		v = append(v, u.UUID)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectValue selects a single value of unspecified type. An error will be returned if no rows are found.
func SelectValue(ctx context.Context, db Queryer, sql string, args ...interface{}) (interface{}, error) {
	var v interface{}
	err := selectOneValue(ctx, db, sql, args, func(rows pgx.Rows) error {
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

// SelectValueColumn selects a column of unspecified type.
func SelectValueColumn(ctx context.Context, db Queryer, sql string, args ...interface{}) ([]interface{}, error) {
	var v []interface{}
	err := selectColumn(ctx, db, sql, args, func(rows pgx.Rows) error {
		values, err := rows.Values()
		if err != nil {
			return err
		}
		v = append(v, values[0])
		return err
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectMap selects a single row into a map. An error will be returned if no rows are found.
func SelectMap(ctx context.Context, db Queryer, sql string, args ...interface{}) (map[string]interface{}, error) {
	var v map[string]interface{}
	err := selectOneRow(ctx, db, sql, args, func(rows pgx.Rows) error {
		values, err := rows.Values()
		if err != nil {
			return err
		}

		v = make(map[string]interface{}, len(values))
		for i := range values {
			v[string(rows.FieldDescriptions()[i].Name)] = values[i]
		}

		return err
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectMapColumn selects rows into a map slice.
func SelectMapColumn(ctx context.Context, db Queryer, sql string, args ...interface{}) ([]map[string]interface{}, error) {
	var v []map[string]interface{}
	err := selectRows(ctx, db, sql, args, func(rows pgx.Rows) error {
		values, err := rows.Values()
		if err != nil {
			return err
		}

		m := make(map[string]interface{}, len(values))
		for i := range values {
			m[string(rows.FieldDescriptions()[i].Name)] = values[i]
		}

		v = append(v, m)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return v, nil
}

// SelectStringMap selects a single row into a map where all values are strings. An error will be returned if no rows
// are found.
func SelectStringMap(ctx context.Context, db Queryer, sql string, args ...interface{}) (map[string]string, error) {
	var v map[string]string
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
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

// SelectStringMapColumn selects rows into a map slice where all values are strings.
func SelectStringMapColumn(ctx context.Context, db Queryer, sql string, args ...interface{}) ([]map[string]string, error) {
	var v []map[string]string
	args = append([]interface{}{pgx.QueryResultFormats{pgx.TextFormatCode}}, args...)
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
