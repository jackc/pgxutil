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
		rows, _ := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		numbers, err := pgxutil.Collect(rows, func(rows pgx.Rows) (int32, error) {
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

func TestScan(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		numbers, err := pgxutil.Collect(rows, pgxutil.Scan[int32])
		require.NoError(t, err)

		assert.Len(t, numbers, 100)
		for i := range numbers {
			assert.Equal(t, int32(i), numbers[i])
		}
	})
}

func TestScanAddrOf(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		numbers, err := pgxutil.Collect(rows, pgxutil.ScanAddrOf[int32])
		require.NoError(t, err)

		assert.Len(t, numbers, 100)
		for i := range numbers {
			assert.Equal(t, int32(i), *numbers[i])
		}
	})
}

func TestScanMap(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		slice, err := pgxutil.Collect(rows, pgxutil.ScanMap)
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i]["name"])
			assert.EqualValues(t, i, slice[i]["age"])
		}
	})
}

func TestScanStructPos(t *testing.T) {
	type person struct {
		Name string
		Age  int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		slice, err := pgxutil.Collect(rows, pgxutil.ScanStructPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i].Name)
			assert.EqualValues(t, i, slice[i].Age)
		}
	})
}

func TestScanAddrOfStructPos(t *testing.T) {
	type person struct {
		Name string
		Age  int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, _ := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		slice, err := pgxutil.Collect(rows, pgxutil.ScanAddrOfStructPos[person])
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i].Name)
			assert.EqualValues(t, i, slice[i].Age)
		}
	})
}
