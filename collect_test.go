package pgxutil_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgxutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollect(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, err := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		require.NoError(t, err)
		numbers, err := pgxutil.Collect(rows, []int32{}, func(rows pgx.Rows) (int32, error) {
			var n int32
			err := rows.Scan(&n)
			return n, err
		})
		require.NoError(t, err)

		assert.Len(t, numbers, 100)
		for i := range numbers {
			assert.Equal(t, int32(i), numbers[i])
		}
	})
}
