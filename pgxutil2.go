// Package pgxutil contains common, extra functionality for pgx.
package pgxutil

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var errTooManyRows = fmt.Errorf("too many rows")

// DB is the interface pgxutil uses to access the database. It is satisfied by *pgx.Conn, pgx.Tx, *pgxpool.Pool, etc.
type DB interface {
	// Begin starts a new pgx.Tx. It may be a true transaction or a pseudo nested transaction implemented by savepoints.
	Begin(ctx context.Context) (pgx.Tx, error)

	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) (br pgx.BatchResults)
}

// Select executes sql with args on db and returns the []T produced by scanFn.
func Select[T any](ctx context.Context, db Queryer, sql string, args []any, scanFn pgx.RowToFunc[T]) ([]T, error) {
	rows, _ := db.Query(ctx, sql, args...)
	collectedRows, err := pgx.CollectRows(rows, scanFn)
	if err != nil {
		return nil, err
	}

	return collectedRows, nil
}

// SelectRow executes sql with args on db and returns the T produced by scanFn. The query should return one row. If no
// rows are found returns an error where errors.Is(pgx.ErrNoRows) is true. Returns an error if more than one row is
// returned.
func SelectRow[T any](ctx context.Context, db Queryer, sql string, args []any, scanFn pgx.RowToFunc[T]) (T, error) {
	rows, _ := db.Query(ctx, sql, args...)
	collectedRow, err := pgx.CollectOneRow(rows, scanFn)
	if err != nil {
		var zero T
		return zero, err
	}

	if rows.CommandTag().RowsAffected() > 1 {
		return collectedRow, errTooManyRows
	}

	return collectedRow, nil
}

// Insert inserts rows into tableName. tableName must be a string or pgx.Identifier.
func Insert(ctx context.Context, db Queryer, tableName any, rows []map[string]any) (pgconn.CommandTag, error) {
	if len(rows) == 0 {
		return pgconn.CommandTag{}, nil
	}

	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		return pgconn.CommandTag{}, fmt.Errorf("Insert invalid tableName: %w", err)
	}

	sql, args := insertSQL(tableIdent, rows, "")
	return exec(ctx, db, sql, args)
}

// InsertReturning inserts rows into tableName with returningClause and returns the []T produced by scanFn. tableName
// must be a string or pgx.Identifier.
func InsertReturning[T any](ctx context.Context, db Queryer, tableName any, rows []map[string]any, returningClause string, scanFn pgx.RowToFunc[T]) ([]T, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		return nil, fmt.Errorf("InsertReturning invalid tableName: %w", err)
	}

	sql, args := insertSQL(tableIdent, rows, returningClause)
	return Select(ctx, db, sql, args, scanFn)
}

// insertSQL builds an insert statement that inserts rows into tableName with returningClause. len(rows) must be > 0.
func insertSQL(tableName pgx.Identifier, rows []map[string]any, returningClause string) (sql string, args []any) {
	b := &strings.Builder{}
	b.WriteString("insert into ")
	if len(tableName) == 1 {
		b.WriteString(sanitizeIdentifier(tableName[0]))
	} else {
		b.WriteString(tableName.Sanitize())
	}
	b.WriteString(" (")

	// Go maps are iterated in random order. The generated SQL should be stable so sort the keys.
	keys := make([]string, 0, len(rows[0]))
	for k := range rows[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		sanitizedKey := sanitizeIdentifier(k)
		b.WriteString(sanitizedKey)
	}

	args = make([]any, 0, len(keys))
	placeholder := int64(1)
	for i, values := range rows {
		if i == 0 {
			b.WriteString(") values (")
		} else {
			b.WriteString("), (")
		}

		for j, key := range keys {
			if j > 0 {
				b.WriteString(", ")
			}
			args = append(args, values[key])
			b.WriteByte('$')
			b.WriteString(strconv.FormatInt(placeholder, 10))
			placeholder++
		}
	}

	b.WriteString(")")

	if returningClause != "" {
		b.WriteString(" returning ")
		b.WriteString(returningClause)
	}

	return b.String(), args
}

// InsertRow inserts values into tableName. tableName must be a string or pgx.Identifier.
func InsertRow(ctx context.Context, db Queryer, tableName any, values map[string]any) error {
	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		return fmt.Errorf("InsertRow invalid tableName: %w", err)
	}

	sql, args := insertRowSQL(tableIdent, values, "")
	_, err = exec(ctx, db, sql, args)
	return err
}

// InsertRowReturning inserts values into tableName with returningClause and returns the T produced by scanFn. tableName
// must be a string or pgx.Identifier.
func InsertRowReturning[T any](ctx context.Context, db Queryer, tableName any, values map[string]any, returningClause string, scanFn pgx.RowToFunc[T]) (T, error) {
	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("InsertRowReturning invalid tableName: %w", err)
	}

	sql, args := insertRowSQL(tableIdent, values, returningClause)
	return SelectRow(ctx, db, sql, args, scanFn)
}

func sanitizeIdentifier(s string) string {
	return pgx.Identifier{s}.Sanitize()
}

func insertRowSQL(tableName pgx.Identifier, values map[string]any, returningClause string) (sql string, args []any) {
	b := &strings.Builder{}
	b.WriteString("insert into ")
	if len(tableName) == 1 {
		b.WriteString(sanitizeIdentifier(tableName[0]))
	} else {
		b.WriteString(tableName.Sanitize())
	}
	b.WriteString(" (")

	// Go maps are iterated in random order. The generated SQL should be stable so sort the keys.
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		sanitizedKey := sanitizeIdentifier(k)
		b.WriteString(sanitizedKey)
	}

	b.WriteString(") values (")
	args = make([]any, len(keys))
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		args[i] = values[k]
		b.WriteByte('$')
		b.WriteString(strconv.FormatInt(int64(i+1), 10))
	}

	b.WriteString(")")

	if returningClause != "" {
		b.WriteString(" returning ")
		b.WriteString(returningClause)
	}

	return b.String(), args
}

// ExecRow executes SQL with args on db. It returns an error unless exactly one row is affected.
func ExecRow(ctx context.Context, db Queryer, sql string, args ...any) (pgconn.CommandTag, error) {
	ct, err := exec(ctx, db, sql, args)
	if err != nil {
		return ct, err
	}
	rowsAffected := ct.RowsAffected()
	if rowsAffected == 0 {
		return ct, pgx.ErrNoRows
	} else if rowsAffected > 1 {
		return ct, errTooManyRows
	}

	return ct, nil
}

// Update updates rows matching whereValues in tableName with setValues. It includes returningClause and returns the []T
// produced by scanFn. tableName must be a string or pgx.Identifier.
func Update(ctx context.Context, db Queryer, tableName any, setValues, whereValues map[string]any) (pgconn.CommandTag, error) {
	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		return pgconn.CommandTag{}, fmt.Errorf("Update invalid tableName: %w", err)
	}

	sql, args := updateSQL(tableIdent, setValues, whereValues, "")
	return exec(ctx, db, sql, args)
}

// UpdateReturning updates rows matching whereValues in tableName with setValues. It includes returningClause and returns the []T
// produced by scanFn. tableName must be a string or pgx.Identifier.
func UpdateReturning[T any](ctx context.Context, db Queryer, tableName any, setValues, whereValues map[string]any, returningClause string, scanFn pgx.RowToFunc[T]) ([]T, error) {
	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		return nil, fmt.Errorf("UpdateReturning invalid tableName: %w", err)
	}

	sql, args := updateSQL(tableIdent, setValues, whereValues, returningClause)
	return Select(ctx, db, sql, args, scanFn)
}

// UpdateRow updates a row matching whereValues in tableName with setValues. Returns an error unless exactly one row is
// updated. tableName must be a string or pgx.Identifier.
func UpdateRow(ctx context.Context, db Queryer, tableName any, setValues, whereValues map[string]any) error {
	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		return fmt.Errorf("UpdateRow invalid tableName: %w", err)
	}

	sql, args := updateSQL(tableIdent, setValues, whereValues, "")
	_, err = ExecRow(ctx, db, sql, args...)
	return err
}

// UpdateRowReturning updates a row matching whereValues in tableName with setValues. It includes returningClause and returns the
// T produced by scanFn. Returns an error unless exactly one row is updated. tableName must be a string or pgx.Identifier.
func UpdateRowReturning[T any](ctx context.Context, db Queryer, tableName any, setValues, whereValues map[string]any, returningClause string, scanFn pgx.RowToFunc[T]) (T, error) {
	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("UpdateRow invalid tableName: %w", err)
	}

	sql, args := updateSQL(tableIdent, setValues, whereValues, returningClause)
	return SelectRow(ctx, db, sql, args, scanFn)
}

func updateSQL(tableName pgx.Identifier, setValues, whereValues map[string]any, returningClause string) (sql string, args []any) {
	b := &strings.Builder{}
	b.WriteString("update ")
	if len(tableName) == 1 {
		b.WriteString(sanitizeIdentifier(tableName[0]))
	} else {
		b.WriteString(tableName.Sanitize())
	}
	b.WriteString(" set ")

	args = make([]any, 0, len(setValues)+len(whereValues))

	// Go maps are iterated in random order. The generated SQL should be stable so sort the setValueKeys.
	setValueKeys := make([]string, 0, len(setValues))
	for k := range setValues {
		setValueKeys = append(setValueKeys, k)
	}
	sort.Strings(setValueKeys)

	for i, k := range setValueKeys {
		if i > 0 {
			b.WriteString(", ")
		}
		sanitizedKey := sanitizeIdentifier(k)
		b.WriteString(sanitizedKey)
		b.WriteString(" = $")
		args = append(args, setValues[k])
		b.WriteString(strconv.FormatInt(int64(len(args)), 10))
	}

	if len(whereValues) > 0 {
		b.WriteString(" where ")

		whereValueKeys := make([]string, 0, len(whereValues))
		for k := range whereValues {
			whereValueKeys = append(whereValueKeys, k)
		}
		sort.Strings(whereValueKeys)

		for i, k := range whereValueKeys {
			if i > 0 {
				b.WriteString(" and ")
			}
			sanitizedKey := sanitizeIdentifier(k)
			b.WriteString(sanitizedKey)
			b.WriteString(" = $")
			args = append(args, whereValues[k])
			b.WriteString(strconv.FormatInt(int64(len(args)), 10))
		}
	}

	if returningClause != "" {
		b.WriteString(" returning ")
		b.WriteString(returningClause)
	}

	return b.String(), args
}

// exec builds Exec-like functionality on top of DB. This allows pgxutil to have the convenience of Exec with needing
// it as part of the DB interface.
func exec(ctx context.Context, db Queryer, sql string, args []any) (pgconn.CommandTag, error) {
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	rows.Close()

	return rows.CommandTag(), rows.Err()
}

func makePgxIdentifier(v any) (pgx.Identifier, error) {
	switch v := v.(type) {
	case string:
		return pgx.Identifier{v}, nil
	case pgx.Identifier:
		return v, nil
	default:
		return nil, fmt.Errorf("expected string or pgx.Identifier, got %T", v)
	}
}
