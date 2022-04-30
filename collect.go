package pgxutil

import "github.com/jackc/pgx/v5"

func Collect[T any](rows pgx.Rows, dest []T, scan func(rows pgx.Rows) (T, error)) ([]T, error) {
	defer rows.Close()

	for rows.Next() {
		value, err := scan(rows)
		if err != nil {
			return nil, err
		}
		dest = append(dest, value)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return dest, nil
}

func CollectScanValue[T any](rows pgx.Rows) (T, error) {
	var v T
	err := rows.Scan(&v)
	return v, err
}

func CollectScanPointer[T any](rows pgx.Rows) (*T, error) {
	var v T
	err := rows.Scan(&v)
	return &v, err
}

func PSRS[T any](v *T) any {
	return PositionalStructRowScanner(v)
}

func CollectScanValueConvert[T any](f func(v *T) any) func(rows pgx.Rows) (T, error) {
	return func(rows pgx.Rows) (T, error) {
		var v T
		err := rows.Scan(f(&v))
		return v, err
	}
}

func CollectScanPointerConvert[T any](f func(v *T) any) func(rows pgx.Rows) (*T, error) {
	return func(rows pgx.Rows) (*T, error) {
		var v T
		err := rows.Scan(f(&v))
		return &v, err
	}
}
