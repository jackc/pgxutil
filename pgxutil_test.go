package pgxutil_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgxutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withTx(t testing.TB, f func(ctx context.Context, tx pgx.Tx)) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn := connectPG(t, ctx)
	defer closeConn(t, conn)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	f(ctx, tx)
}

func connectPG(t testing.TB, ctx context.Context) *pgx.Conn {
	config, err := pgx.ParseConfig(fmt.Sprintf("database=%s", os.Getenv("TEST_DATABASE")))
	require.NoError(t, err)
	config.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		t.Logf("PostgreSQL %s: %s", n.Severity, n.Message)
	}

	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	return conn
}

func closeConn(t testing.TB, conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
}

func TestSelectOneCommonErrors(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			err    string
			result interface{}
		}{
			{"select 42::float8 where 1=0", "no rows in result set", nil},
			{"select 42::float8 from generate_series(1,2)", "multiple rows in result set", nil},
			{"select", "no columns in result set", nil},
			{"select 1, 2", "multiple columns in result set", nil},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectValue(ctx, tx, tt.sql)
			if tt.err == "" {
				assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			} else {
				assert.EqualErrorf(t, err, tt.err, "%d. %s", i, tt.sql)
			}
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectString(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result string
		}{
			{"select 'Hello, world!'", "Hello, world!"},
			{"select 42", "42"},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectString(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectAllString(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result []string
		}{
			{"select format('Hello %s', n) from generate_series(1,2) n", []string{"Hello 1", "Hello 2"}},
			{"select 'Hello, world!' where false", nil},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectAllString(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectByteSlice(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result []byte
		}{
			{"select 'Hello, world!'", []byte("Hello, world!")},
			{"select 42", []byte{0, 0, 0, 42}},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectByteSlice(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectAllByteSlice(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result [][]byte
		}{
			{"select format('Hello %s', n) from generate_series(1,2) n", [][]byte{[]byte("Hello 1"), []byte("Hello 2")}},
			{"select 'Hello, world!' where false", nil},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectAllByteSlice(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectInt64(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result int64
		}{
			{"select 99999999999::bigint", 99999999999},
			{"select 42::smallint", 42},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectInt64(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectAllInt64(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result []int64
		}{
			{"select generate_series(1,2)", []int64{1, 2}},
			{"select 42 where false", nil},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectAllInt64(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectFloat64(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result float64
		}{
			{"select 1.2345::float8", 1.2345},
			{"select 1.23::float4", 1.23},
			{"select 1.2345::numeric", 1.2345},
			{"select 99999999999::bigint", 99999999999},
			{"select 42::smallint", 42},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectFloat64(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectAllFloat64(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result []float64
		}{
			{"select n + 0.5 from generate_series(1,2) n", []float64{1.5, 2.5}},
			{"select 42.0 where false", nil},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectAllFloat64(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectDecimal(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result string
		}{
			{"select 1.2345::numeric", "1.2345"},
			{"select 1.2345::float8", "1.2345"},
			{"select 1.23::float4", "1.23"},
			{"select 99999999999::bigint", "99999999999"},
			{"select 42::smallint", "42"},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectDecimal(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v.String(), "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectAllDecimal(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result []string
		}{
			{"select n + 0.5 from generate_series(1,2) n", []string{"1.5", "2.5"}},
			{"select 42.0 where false", nil},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectAllDecimal(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			if assert.Equalf(t, len(tt.result), len(v), "%d. %s", i, tt.sql) {
				for j := range v {
					assert.Equalf(t, tt.result[j], v[j].String(), "%d. %s - %d", i, tt.sql, j)
				}
			}
		}
	})
}

func TestSelectUUID(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result uuid.UUID
		}{
			{"select '27fd10c1-bccc-4efd-9fea-093f86c95089'::uuid", uuid.FromStringOrNil("27fd10c1-bccc-4efd-9fea-093f86c95089")},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectUUID(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectAllUUID(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result []uuid.UUID
		}{
			{
				sql: "select format('27fd10c1-bccc-4efd-9fea-093f86c9508%s', n)::uuid from generate_series(1,2) n",
				result: []uuid.UUID{
					uuid.FromStringOrNil("27fd10c1-bccc-4efd-9fea-093f86c95081"),
					uuid.FromStringOrNil("27fd10c1-bccc-4efd-9fea-093f86c95082"),
				},
			},
			{
				sql:    "select '27fd10c1-bccc-4efd-9fea-093f86c95089'::uuid where false",
				result: nil,
			},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectAllUUID(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectValue(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result interface{}
		}{
			{"select 'Hello'", "Hello"},
			{"select 42", int32(42)},
			{"select 1.23::float4", float32(1.23)},
			{"select null::float4", nil},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectValue(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectAllValue(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result []interface{}
		}{
			{"select n from generate_series(1,2) n", []interface{}{int32(1), int32(2)}},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectAllValue(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectMap(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result map[string]interface{}
		}{
			{"select 'Adam' as name, 72 as height", map[string]interface{}{"name": "Adam", "height": int32(72)}},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectMap(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectAllMap(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result []map[string]interface{}
		}{
			{
				sql: "select n as a, n+1 as b from generate_series(1,2) n",
				result: []map[string]interface{}{
					{"a": int32(1), "b": int32(2)},
					{"a": int32(2), "b": int32(3)},
				},
			},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectAllMap(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectStringMap(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result map[string]string
		}{
			{"select 'Adam' as name, 72 as height", map[string]string{"name": "Adam", "height": "72"}},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectStringMap(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectAllStringMap(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result []map[string]string
		}{
			{
				sql: "select n as a, n+1 as b from generate_series(1,2) n",
				result: []map[string]string{
					{"a": "1", "b": "2"},
					{"a": "2", "b": "3"},
				},
			},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectAllStringMap(ctx, tx, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectStructPositionalMapping(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		type person struct {
			Name   string
			Height int32
		}

		tests := []struct {
			sql      string
			expected person
		}{
			{"select 'Adam', 72", person{Name: "Adam", Height: 72}},
		}
		for i, tt := range tests {
			var actual person
			err := pgxutil.SelectStruct(ctx, tx, &actual, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.expected, actual, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectAllStructPointerPositionalMapping(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		type person struct {
			Name   string
			Height int32
		}

		tests := []struct {
			sql      string
			expected []*person
		}{
			{"select 'Adam', 72 union all select 'Bill', 65", []*person{
				{Name: "Adam", Height: 72},
				{Name: "Bill", Height: 65},
			}},
		}
		for i, tt := range tests {
			var actual []*person
			err := pgxutil.SelectAllStruct(ctx, tx, &actual, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.expected, actual, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectAllStructPositionalMapping(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		type person struct {
			Name   string
			Height int32
		}

		tests := []struct {
			sql      string
			expected []person
		}{
			{"select 'Adam', 72 union all select 'Bill', 65", []person{
				{Name: "Adam", Height: 72},
				{Name: "Bill", Height: 65},
			}},
		}
		for i, tt := range tests {
			var actual []person
			err := pgxutil.SelectAllStruct(ctx, tx, &actual, tt.sql)
			assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			assert.Equalf(t, tt.expected, actual, "%d. %s", i, tt.sql)
		}
	})
}

func BenchmarkSelectRow(b *testing.B) {
	ctx := context.Background()
	conn := connectPG(b, ctx)
	defer closeConn(b, conn)

	type person struct {
		FirstName string
		LastName  string
		Height    int32
	}

	sql := "select 'Adam' as first_name, 'Smith' as last_name, 72 as height"

	validateStruct := func(b *testing.B, dst *person, err error) {
		if err != nil {
			b.Fatal(err)
		}
		if dst.FirstName != "Adam" {
			b.Fatal("unexpected value read")
		}
		if dst.LastName != "Smith" {
			b.Fatal("unexpected value read")
		}
		if dst.Height != 72 {
			b.Fatal("unexpected value read")
		}
	}

	b.Run("Scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var dst person
			err := conn.QueryRow(ctx, sql).Scan(&dst.FirstName, &dst.LastName, &dst.Height)
			validateStruct(b, &dst, err)
		}
	})

	b.Run("SelectMap", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dst, err := pgxutil.SelectMap(ctx, conn, sql)
			if err != nil {
				b.Fatal(err)
			}
			if dst["first_name"] != "Adam" {
				b.Fatal("unexpected value read")
			}
			if dst["last_name"] != "Smith" {
				b.Fatal("unexpected value read")
			}
			if dst["height"] != int32(72) {
				b.Fatal("unexpected value read")
			}
		}
	})

	b.Run("SelectStruct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var dst person
			err := pgxutil.SelectStruct(ctx, conn, &dst, sql)
			validateStruct(b, &dst, err)
		}
	})
}

func BenchmarkSelectAllRows(b *testing.B) {
	ctx := context.Background()
	conn := connectPG(b, ctx)
	defer closeConn(b, conn)

	type person struct {
		FirstName string
		LastName  string
		Height    int32
	}

	sql := "select 'Adam' as first_name, 'Smith' as last_name, 72 as height from generate_series(1, 10)"

	validateStruct := func(b *testing.B, dst *person) {
		if dst.FirstName != "Adam" {
			b.Fatal("unexpected value read")
		}
		if dst.LastName != "Smith" {
			b.Fatal("unexpected value read")
		}
		if dst.Height != 72 {
			b.Fatal("unexpected value read")
		}
	}

	b.Run("Scan Streaming", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var p person
			rows, err := conn.Query(ctx, sql)
			if err != nil {
				b.Fatal(err)
			}

			for rows.Next() {
				err = rows.Scan(&p.FirstName, &p.LastName, &p.Height)
				if err != nil {
					b.Fatal(err)
				}
				validateStruct(b, &p)
			}
		}
	})

	b.Run("Scan Collect Pointer to Struct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var people []*person
			rows, err := conn.Query(ctx, sql)
			if err != nil {
				b.Fatal(err)
			}

			for rows.Next() {
				var p person
				err = rows.Scan(&p.FirstName, &p.LastName, &p.Height)
				if err != nil {
					b.Fatal(err)
				}

				people = append(people, &p)
			}

			if rows.Err() != nil {
				b.Fatal(rows.Err())
			}

			for _, p := range people {
				validateStruct(b, p)
			}
		}
	})

	b.Run("Scan Collect Struct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var people []person
			rows, err := conn.Query(ctx, sql)
			if err != nil {
				b.Fatal(err)
			}

			for rows.Next() {
				var p person
				err = rows.Scan(&p.FirstName, &p.LastName, &p.Height)
				if err != nil {
					b.Fatal(err)
				}

				people = append(people, p)
			}

			if rows.Err() != nil {
				b.Fatal(rows.Err())
			}

			for i := range people {
				validateStruct(b, &people[i])
			}
		}
	})

	b.Run("SelectAllMap", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			people, err := pgxutil.SelectAllMap(ctx, conn, sql)
			if err != nil {
				b.Fatal(err)
			}

			for _, p := range people {
				if p["first_name"] != "Adam" {
					b.Fatal("unexpected value read")
				}
				if p["last_name"] != "Smith" {
					b.Fatal("unexpected value read")
				}
				if p["height"] != int32(72) {
					b.Fatal("unexpected value read")
				}
			}
		}
	})

	b.Run("SelectAllStruct Pointer to Struct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var people []*person
			err := pgxutil.SelectAllStruct(ctx, conn, &people, sql)
			if err != nil {
				b.Fatal(err)
			}

			for _, p := range people {
				validateStruct(b, p)
			}
		}
	})

	b.Run("SelectAllStruct Struct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var people []person
			err := pgxutil.SelectAllStruct(ctx, conn, &people, sql)
			if err != nil {
				b.Fatal(err)
			}

			for i := range people {
				validateStruct(b, &people[i])
			}
		}
	})
}

func TestInsert(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		_, err := tx.Exec(ctx, `create temporary table t (id serial primary key, name text, height int)`)
		require.NoError(t, err)
		returningRow, err := pgxutil.Insert(ctx, tx, "t", map[string]interface{}{"name": "Adam", "height": 72})
		require.NoError(t, err)

		assert.Equal(t, int32(1), returningRow["id"])
		assert.Equal(t, "Adam", returningRow["name"])
		assert.Equal(t, int32(72), returningRow["height"])

		selectedRow, err := pgxutil.SelectMap(ctx, tx, "select * from t where id=$1", returningRow["id"])
		require.NoError(t, err)
		assert.Equal(t, returningRow, selectedRow)
	})
}

func TestUpdateWithoutWhere(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		_, err := tx.Exec(ctx, `create temporary table t (id serial primary key, name text, height int)`)
		require.NoError(t, err)
		row1, err := pgxutil.Insert(ctx, tx, "t", map[string]interface{}{"name": "Adam", "height": 72})
		require.NoError(t, err)
		row2, err := pgxutil.Insert(ctx, tx, "t", map[string]interface{}{"name": "Bill", "height": 68})
		require.NoError(t, err)

		updateCount, err := pgxutil.Update(ctx, tx, "t", map[string]interface{}{"height": 99}, nil)
		require.NoError(t, err)
		assert.EqualValues(t, 2, updateCount)

		ids := []int32{row1["id"].(int32), row2["id"].(int32)}
		for i, id := range ids {
			selectedRow, err := pgxutil.SelectMap(ctx, tx, "select * from t where id=$1", id)
			require.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, int32(99), selectedRow["height"], "%d", i)
		}
	})
}

func TestUpdateWithWhere(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		_, err := tx.Exec(ctx, `create temporary table t (id serial primary key, name text, height int)`)
		require.NoError(t, err)
		row1, err := pgxutil.Insert(ctx, tx, "t", map[string]interface{}{"name": "Adam", "height": 72})
		require.NoError(t, err)
		row2, err := pgxutil.Insert(ctx, tx, "t", map[string]interface{}{"name": "Bill", "height": 68})
		require.NoError(t, err)

		updateCount, err := pgxutil.Update(ctx, tx, "t", map[string]interface{}{"height": 99}, map[string]interface{}{"id": row1["id"]})
		require.NoError(t, err)
		assert.Equal(t, int64(1), updateCount)

		freshRow1, err := pgxutil.SelectMap(ctx, tx, "select * from t where id=$1", row1["id"])
		require.NoError(t, err)
		assert.Equal(t, row1["id"], freshRow1["id"])
		assert.Equal(t, row1["name"], freshRow1["name"])
		assert.Equal(t, int32(99), freshRow1["height"])

		freshRow2, err := pgxutil.SelectMap(ctx, tx, "select * from t where id=$1", row2["id"])
		require.NoError(t, err)
		assert.Equal(t, row2, freshRow2)
	})
}
