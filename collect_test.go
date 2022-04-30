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

func TestCollectValueScanner(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, err := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		require.NoError(t, err)
		numbers, err := pgxutil.Collect(rows, []int32{}, pgxutil.CollectScanValue[int32])
		require.NoError(t, err)

		assert.Len(t, numbers, 100)
		for i := range numbers {
			assert.Equal(t, int32(i), numbers[i])
		}
	})
}

func TestCollectPointerScanner(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, err := conn.Query(ctx, `select n from generate_series(0, 99) n`)
		require.NoError(t, err)
		numbers, err := pgxutil.Collect(rows, []*int32{}, pgxutil.CollectScanPointer[int32])
		require.NoError(t, err)

		assert.Len(t, numbers, 100)
		for i := range numbers {
			assert.Equal(t, int32(i), *numbers[i])
		}
	})
}

func TestCollectValuesMapRowScanner(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, err := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		require.NoError(t, err)
		slice, err := pgxutil.Collect(rows, []map[string]any{}, func(rows pgx.Rows) (map[string]any, error) {
			var m map[string]any
			err := rows.Scan((*pgxutil.ValuesMapRowScanner)(&m))
			return m, err
		})
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i]["name"])
			assert.EqualValues(t, i, slice[i]["age"])
		}
	})
}

func TestScanValueConvert(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, err := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		require.NoError(t, err)
		slice, err := pgxutil.Collect(rows, []map[string]any{}, pgxutil.CollectScanValueConvert(func(v *map[string]any) any { return (*pgxutil.ValuesMapRowScanner)(v) }))
		require.NoError(t, err)

		assert.Len(t, slice, 10)
		for i := range slice {
			assert.Equal(t, "Joe", slice[i]["name"])
			assert.EqualValues(t, i, slice[i]["age"])
		}
	})
}

func TestScanPointerConvert(t *testing.T) {
	type person struct {
		Name string
		Age  int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, err := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		require.NoError(t, err)
		people, err := pgxutil.Collect(rows, []*person{}, pgxutil.CollectScanPointerConvert(func(v *person) any {
			return pgxutil.PositionalStructRowScanner(v)
		}))
		require.NoError(t, err)

		assert.Len(t, people, 10)
		for i := range people {
			assert.Equal(t, "Joe", people[i].Name)
			assert.Equal(t, int32(i), people[i].Age)
		}
	})
}

func TestScanPointerConvertPSRS(t *testing.T) {
	type person struct {
		Name string
		Age  int32
	}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, err := conn.Query(ctx, `select 'Joe' as name, n as age from generate_series(0, 9) n`)
		require.NoError(t, err)
		people, err := pgxutil.Collect(rows, []*person{}, pgxutil.CollectScanPointerConvert(pgxutil.PSRS[person]))
		require.NoError(t, err)

		assert.Len(t, people, 10)
		for i := range people {
			assert.Equal(t, "Joe", people[i].Name)
			assert.Equal(t, int32(i), people[i].Age)
		}
	})
}
