package pgxutil

import "github.com/jackc/pgx/v5"

func Collect[T any](rows pgx.Rows, dest []T, scanner func(rows pgx.Rows) (T, error)) ([]T, error) {
	for rows.Next() {
		value, err := scanner(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		dest = append(dest, value)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return dest, nil
}
