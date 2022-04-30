package pgxutil_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/jackc/pgxutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var defaultConnTestRunner pgxtest.ConnTestRunner

func init() {
	defaultConnTestRunner = pgxtest.DefaultConnTestRunner()
	defaultConnTestRunner.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config, err := pgx.ParseConfig(os.Getenv("TEST_DATABASE"))
		require.NoError(t, err)
		config.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
			t.Logf("PostgreSQL %s: %s", n.Severity, n.Message)
		}
		return config
	}
}

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

func TestSelectMap(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			result map[string]any
		}{
			{"select 'Adam' as name, 72 as height", map[string]any{"name": "Adam", "height": int32(72)}},
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
			result []map[string]any
		}{
			{
				sql: "select n as a, n+1 as b from generate_series(1,2) n",
				result: []map[string]any{
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

	b.Run("Scan Manually Collect Pointer to Struct", func(b *testing.B) {
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

	b.Run("Scan Manually Collect Struct", func(b *testing.B) {
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

	b.Run("Scan Collect Pointer to Struct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(ctx, sql)
			people, err := pgxutil.Collect(rows, func(rows pgx.Rows) (*person, error) {
				var p person
				err := rows.Scan(&p.FirstName, &p.LastName, &p.Height)
				return &p, err
			})
			if err != nil {
				b.Fatal(err)
			}

			for _, p := range people {
				validateStruct(b, p)
			}
		}
	})

	b.Run("Scan Collect Struct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(ctx, sql)
			people, err := pgxutil.Collect(rows, func(rows pgx.Rows) (person, error) {
				var p person
				err := rows.Scan(&p.FirstName, &p.LastName, &p.Height)
				return p, err
			})
			if err != nil {
				b.Fatal(err)
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

	b.Run("Collect ScanMap", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(ctx, sql)
			people, err := pgxutil.Collect(rows, pgxutil.ScanMap)
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

	b.Run("Collect ScanAddrOfStructPos", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, _ := conn.Query(ctx, sql)
			people, err := pgxutil.Collect(rows, pgxutil.ScanAddrOfStructPos[person])
			if err != nil {
				b.Fatal(err)
			}

			for _, p := range people {
				validateStruct(b, p)
			}
		}
	})
}

func TestInsert(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		_, err := tx.Exec(ctx, `create temporary table t (id serial primary key, name text, height int)`)
		require.NoError(t, err)
		returningRow, err := pgxutil.Insert(ctx, tx, "t", map[string]any{"name": "Adam", "height": 72})
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
		row1, err := pgxutil.Insert(ctx, tx, "t", map[string]any{"name": "Adam", "height": 72})
		require.NoError(t, err)
		row2, err := pgxutil.Insert(ctx, tx, "t", map[string]any{"name": "Bill", "height": 68})
		require.NoError(t, err)

		updateCount, err := pgxutil.Update(ctx, tx, "t", map[string]any{"height": 99}, nil)
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
		row1, err := pgxutil.Insert(ctx, tx, "t", map[string]any{"name": "Adam", "height": 72})
		require.NoError(t, err)
		row2, err := pgxutil.Insert(ctx, tx, "t", map[string]any{"name": "Bill", "height": 68})
		require.NoError(t, err)

		updateCount, err := pgxutil.Update(ctx, tx, "t", map[string]any{"height": 99}, map[string]any{"id": row1["id"]})
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

func TestSelectValue(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		{
			v, err := pgxutil.SelectValue[bool](ctx, tx, "select true")
			assert.NoError(t, err)
			assert.Equal(t, true, v)
		}
		{
			v, err := pgxutil.SelectValue[bool](ctx, tx, "select false")
			assert.NoError(t, err)
			assert.Equal(t, false, v)
		}
		{
			v, err := pgxutil.SelectValue[int32](ctx, tx, "select 42")
			assert.NoError(t, err)
			assert.Equal(t, int32(42), v)
		}
		{
			v, err := pgxutil.SelectValue[pgtype.Int4](ctx, tx, "select null::int4")
			assert.NoError(t, err)
			assert.Equal(t, pgtype.Int4{}, v)
		}
		{
			v, err := pgxutil.SelectValue[float64](ctx, tx, "select 1.23::float8")
			assert.NoError(t, err)
			assert.Equal(t, float64(1.23), v)
		}
		{
			v, err := pgxutil.SelectValue[string](ctx, tx, "select 'foo'::text")
			assert.NoError(t, err)
			assert.Equal(t, "foo", v)
		}
		{
			v, err := pgxutil.SelectValue[[]byte](ctx, tx, `select '\x01020304'::bytea`)
			assert.NoError(t, err)
			assert.Equal(t, []byte{1, 2, 3, 4}, v)
		}
		{
			v, err := pgxutil.SelectValue[string](ctx, tx, "select '25f6ef51-6795-4ca8-b2df-bafabd36ba23'::uuid")
			assert.NoError(t, err)
			assert.Equal(t, "25f6ef51-6795-4ca8-b2df-bafabd36ba23", v)
		}
		{
			v, err := pgxutil.SelectValue[any](ctx, tx, "select 1.23::float8")
			assert.NoError(t, err)
			assert.Equal(t, float64(1.23), v)
		}
	})
}

func TestSelectValueErrors(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			err    string
			result any
		}{
			{"select 42::float8 where 1=0", "no rows in result set", nil},
			{"select 42::float8 from generate_series(1,2)", "multiple rows in result set", nil},
			{"select", "no columns in result set", nil},
			{"select 1, 2", "multiple columns in result set", nil},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectValue[any](ctx, tx, tt.sql)
			if tt.err == "" {
				assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			} else {
				assert.EqualErrorf(t, err, tt.err, "%d. %s", i, tt.sql)
			}
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectColumn(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		{
			c, err := pgxutil.SelectColumn[string](ctx, tx, "select format('Hello %s', n) from generate_series(1,2) n")
			assert.NoError(t, err)
			assert.Equal(t, []string{"Hello 1", "Hello 2"}, c)
		}
		{
			c, err := pgxutil.SelectColumn[string](ctx, tx, "select 'Hello, world!' where false")
			assert.NoError(t, err)
			assert.Equal(t, []string{}, c)
		}
	})
}

func TestSelectColumnErrors(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			err    string
			result any
		}{
			{"select", "no columns in result set", []any(nil)},
			{"select 1, 2", "multiple columns in result set", []any(nil)},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectColumn[any](ctx, tx, tt.sql)
			if tt.err == "" {
				assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			} else {
				assert.EqualErrorf(t, err, tt.err, "%d. %s", i, tt.sql)
			}
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}
