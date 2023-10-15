package pgxutil

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var errNotFound = errors.New("no rows in result set")
var errNoColumns = errors.New("no columns in result set")
var errMultipleColumns = errors.New("multiple columns in result set")
var errMultipleRows = errors.New("multiple rows in result set")
var errTooManyRows = fmt.Errorf("too many rows")

type Queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

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

// SQLValue is a SQL expression intended for use where a value would normally be expected. It is not escaped or sanitized.
type SQLValue string

// Select executes sql with args on db and returns the []T produced by scanFn.
func Select[T any](ctx context.Context, db Queryer, sql string, args []any, scanFn pgx.RowToFunc[T]) ([]T, error) {
	rows, _ := db.Query(ctx, sql, args...)
	collectedRows, err := pgx.CollectRows(rows, scanFn)
	if err != nil {
		return nil, err
	}

	return collectedRows, nil
}

// QueueSelect queues sql with args into batch. scanFn will be used to populate result when the response to this query
// is received.
func QueueSelect[T any](batch *pgx.Batch, sql string, args []any, scanFn pgx.RowToFunc[T], result *[]T) {
	batch.Queue(sql, args...).Query(func(rows pgx.Rows) error {
		collectedRows, err := pgx.CollectRows(rows, scanFn)
		if err != nil {
			return err
		}
		*result = collectedRows
		return nil
	})
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

// QueueSelectRow queues sql with args into batch. scanFn will be used to populate result when the response to this query
// is received.
func QueueSelectRow[T any](batch *pgx.Batch, sql string, args []any, scanFn pgx.RowToFunc[T], result *T) {
	batch.Queue(sql, args...).Query(func(rows pgx.Rows) error {
		collectedRow, err := pgx.CollectOneRow(rows, scanFn)
		if err != nil {
			return err
		}

		if rows.CommandTag().RowsAffected() > 1 {
			return errTooManyRows
		}

		*result = collectedRow
		return nil
	})
}

// Insert inserts rows into tableName. tableName must be a string or pgx.Identifier. rows can include SQLValue to use a
// raw SQL expression as a value.
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

// QueueInsert queues the insert of rows into tableName. tableName must be a string or pgx.Identifier. rows can include SQLValue to use a
// raw SQL expression as a value.
func QueueInsert(batch *pgx.Batch, tableName any, rows []map[string]any) {
	if len(rows) == 0 {
		// Since nothing is queued, the number of queries in the batch may be lower than expected. Not sure if anything can
		// or should be done about this.
		return
	}

	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		// Panicking is undesirable, but we don't want to have this function return an error or silently ignore the error.
		// Possibly pgx.Batch should be modified to allow queueing an error.
		panic(fmt.Sprintf("QueueInsert invalid tableName: %v", err))
	}

	sql, args := insertSQL(tableIdent, rows, "")
	batch.Queue(sql, args...)
}

// InsertReturning inserts rows into tableName with returningClause and returns the []T produced by scanFn. tableName
// must be a string or pgx.Identifier. rows can include SQLValue to use a raw SQL expression as a value.
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

// QueueInsertReturning queues the insert of rows into tableName with returningClause. tableName must be a string or
// pgx.Identifier. rows can include SQLValue to use a raw SQL expression as a value. scanFn will be used to populate
// result when the response to this query is received.
func QueueInsertReturning[T any](batch *pgx.Batch, tableName any, rows []map[string]any, returningClause string, scanFn pgx.RowToFunc[T], result *[]T) {
	if len(rows) == 0 {
		// Since nothing is queued, the number of queries in the batch may be lower than expected. Not sure if anything can
		// or should be done about this.
		return
	}

	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		// Panicking is undesirable, but we don't want to have this function return an error or silently ignore the error.
		// Possibly pgx.Batch should be modified to allow queueing an error.
		panic(fmt.Sprintf("QueueInsertReturning invalid tableName: %v", err))
	}

	sql, args := insertSQL(tableIdent, rows, returningClause)
	QueueSelect(batch, sql, args, scanFn, result)
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
			if SQLValue, ok := values[key].(SQLValue); ok {
				b.WriteString(string(SQLValue))
			} else {
				args = append(args, values[key])
				b.WriteByte('$')
				b.WriteString(strconv.FormatInt(int64(len(args)), 10))
			}
		}
	}

	b.WriteString(")")

	if returningClause != "" {
		b.WriteString(" returning ")
		b.WriteString(returningClause)
	}

	return b.String(), args
}

// InsertRow inserts values into tableName. tableName must be a string or pgx.Identifier. values can include SQLValue to
// use a raw SQL expression as a value.
func InsertRow(ctx context.Context, db Queryer, tableName any, values map[string]any) error {
	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		return fmt.Errorf("InsertRow invalid tableName: %w", err)
	}

	sql, args := insertRowSQL(tableIdent, values, "")
	_, err = exec(ctx, db, sql, args)
	return err
}

// QueueInsertRow queues the insert of values into tableName. tableName must be a string or pgx.Identifier. values can include SQLValue to
// use a raw SQL expression as a value.
func QueueInsertRow(batch *pgx.Batch, tableName any, values map[string]any) {
	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		// Panicking is undesirable, but we don't want to have this function return an error or silently ignore the error.
		// Possibly pgx.Batch should be modified to allow queueing an error.
		panic(fmt.Sprintf("QueueInsertRow invalid tableName: %v", err))
	}

	sql, args := insertRowSQL(tableIdent, values, "")
	batch.Queue(sql, args...)
}

// InsertRowReturning inserts values into tableName with returningClause and returns the T produced by scanFn. tableName
// must be a string or pgx.Identifier. values can include SQLValue to use a raw SQL expression as a value.
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
	args = make([]any, 0, len(keys))
	for _, k := range keys {
		if len(args) > 0 {
			b.WriteString(", ")
		}
		if SQLValue, ok := values[k].(SQLValue); ok {
			b.WriteString(string(SQLValue))
		} else {
			args = append(args, values[k])
			b.WriteByte('$')
			b.WriteString(strconv.FormatInt(int64(len(args)), 10))
		}
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
// produced by scanFn. tableName must be a string or pgx.Identifier. setValues and whereValues can include SQLValue to
// use a raw SQL expression as a value.
func Update(ctx context.Context, db Queryer, tableName any, setValues, whereValues map[string]any) (pgconn.CommandTag, error) {
	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		return pgconn.CommandTag{}, fmt.Errorf("Update invalid tableName: %w", err)
	}

	sql, args := updateSQL(tableIdent, setValues, whereValues, "")
	return exec(ctx, db, sql, args)
}

// UpdateReturning updates rows matching whereValues in tableName with setValues. It includes returningClause and
// returns the []T produced by scanFn. tableName must be a string or pgx.Identifier. setValues and whereValues can
// include SQLValue to use a raw SQL expression as a value.
func UpdateReturning[T any](ctx context.Context, db Queryer, tableName any, setValues, whereValues map[string]any, returningClause string, scanFn pgx.RowToFunc[T]) ([]T, error) {
	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		return nil, fmt.Errorf("UpdateReturning invalid tableName: %w", err)
	}

	sql, args := updateSQL(tableIdent, setValues, whereValues, returningClause)
	return Select(ctx, db, sql, args, scanFn)
}

// UpdateRow updates a row matching whereValues in tableName with setValues. Returns an error unless exactly one row is
// updated. tableName must be a string or pgx.Identifier. setValues and whereValues can include SQLValue to use a raw
// SQL expression as a value.
func UpdateRow(ctx context.Context, db Queryer, tableName any, setValues, whereValues map[string]any) error {
	tableIdent, err := makePgxIdentifier(tableName)
	if err != nil {
		return fmt.Errorf("UpdateRow invalid tableName: %w", err)
	}

	sql, args := updateSQL(tableIdent, setValues, whereValues, "")
	_, err = ExecRow(ctx, db, sql, args...)
	return err
}

// UpdateRowReturning updates a row matching whereValues in tableName with setValues. It includes returningClause and
// returns the T produced by scanFn. Returns an error unless exactly one row is updated. tableName must be a string or
// pgx.Identifier. setValues and whereValues can include SQLValue to use a raw SQL expression as a value.
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

		if SQLValue, ok := setValues[k].(SQLValue); ok {
			b.WriteString(" = ")
			b.WriteString(string(SQLValue))
		} else {
			b.WriteString(" = $")
			args = append(args, setValues[k])
			b.WriteString(strconv.FormatInt(int64(len(args)), 10))
		}
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

			if SQLValue, ok := whereValues[k].(SQLValue); ok {
				b.WriteString(" = ")
				b.WriteString(string(SQLValue))
			} else {
				b.WriteString(" = $")
				args = append(args, whereValues[k])
				b.WriteString(strconv.FormatInt(int64(len(args)), 10))
			}
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
