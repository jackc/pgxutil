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
	"github.com/jackc/pgx/v5/pgxpool"
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

func TestDBIsImplementedByPoolConnAndTx(t *testing.T) {
	t.Parallel()

	var db pgxutil.DB
	var pool *pgxpool.Pool
	var conn *pgx.Conn
	var tx pgx.Tx

	db = pool
	db = conn
	db = tx
	_ = db
}

func TestSelect(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		people, err := pgxutil.Select(ctx, conn, `select n, 'John', 42 from generate_series(1,3) n`, nil, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 3)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 42, people[0].Age)
		require.EqualValues(t, 2, people[1].ID)
		require.Equal(t, "John", people[1].Name)
		require.EqualValues(t, 42, people[1].Age)
		require.EqualValues(t, 3, people[2].ID)
		require.Equal(t, "John", people[2].Name)
		require.EqualValues(t, 42, people[2].Age)
	})
}

func TestSelectNoRows(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		people, err := pgxutil.Select(ctx, conn, `select 1, 'John', 42 where false`, nil, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 0)
	})
}

func TestQueueSelect(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		batch := &pgx.Batch{}

		var people []*Person
		pgxutil.QueueSelect(batch, `select n, 'John', 42 from generate_series(1,3) n`, nil, pgx.RowToAddrOfStructByPos[Person], &people)
		require.Nil(t, people)

		err := conn.SendBatch(ctx, batch).Close()
		require.NoError(t, err)
		require.Len(t, people, 3)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 42, people[0].Age)
		require.EqualValues(t, 2, people[1].ID)
		require.Equal(t, "John", people[1].Name)
		require.EqualValues(t, 42, people[1].Age)
		require.EqualValues(t, 3, people[2].ID)
		require.Equal(t, "John", people[2].Name)
		require.EqualValues(t, 42, people[2].Age)
	})
}

func TestQueueSelectNoRows(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		batch := &pgx.Batch{}

		var people []*Person
		pgxutil.QueueSelect(batch, `select 1, 'John', 42 where false`, nil, pgx.RowToAddrOfStructByPos[Person], &people)
		require.Nil(t, people)

		err := conn.SendBatch(ctx, batch).Close()
		require.NoError(t, err)
		require.Len(t, people, 0)
	})
}

func TestSelectRow(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		person, err := pgxutil.SelectRow(ctx, conn, `select 1, 'John', 42`, nil, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.EqualValues(t, 1, person.ID)
		require.Equal(t, "John", person.Name)
		require.EqualValues(t, 42, person.Age)
	})
}

func TestSelectRowNoRows(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		person, err := pgxutil.SelectRow(ctx, conn, `select 1, 'John', 42 where false`, nil, pgx.RowToAddrOfStructByPos[Person])
		require.ErrorIs(t, err, pgx.ErrNoRows)
		require.Nil(t, person)
	})
}

func TestQueueSelectRow(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		batch := &pgx.Batch{}

		var person *Person
		pgxutil.QueueSelectRow(batch, `select 1, 'John', 42`, nil, pgx.RowToAddrOfStructByPos[Person], &person)
		require.Nil(t, person)

		err := conn.SendBatch(ctx, batch).Close()
		require.NoError(t, err)
		require.EqualValues(t, 1, person.ID)
		require.Equal(t, "John", person.Name)
		require.EqualValues(t, 42, person.Age)
	})
}

func TestQueueSelectRowNoRows(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		batch := &pgx.Batch{}

		var person *Person
		pgxutil.QueueSelectRow(batch, `select 1, 'John', 42 where false`, nil, pgx.RowToAddrOfStructByPos[Person], &person)
		require.Nil(t, person)

		err := conn.SendBatch(ctx, batch).Close()
		require.ErrorIs(t, err, pgx.ErrNoRows)
		require.Nil(t, person)
	})
}

func TestInsert(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		_, err = pgxutil.Insert(ctx, conn, pgx.Identifier{"t"}, []map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}})
		require.NoError(t, err)

		people, err := pgxutil.Select(ctx, conn, `select * from t order by id`, nil, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 42, people[0].Age)
		require.EqualValues(t, 2, people[1].ID)
		require.Equal(t, "Jane", people[1].Name)
		require.EqualValues(t, 40, people[1].Age)
	})
}

func TestQueueInsert(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		batch := &pgx.Batch{}
		pgxutil.QueueInsert(batch, pgx.Identifier{"t"}, []map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}})
		err = conn.SendBatch(ctx, batch).Close()
		require.NoError(t, err)

		people, err := pgxutil.Select(ctx, conn, `select * from t order by id`, nil, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 42, people[0].Age)
		require.EqualValues(t, 2, people[1].ID)
		require.Equal(t, "Jane", people[1].Name)
		require.EqualValues(t, 40, people[1].Age)
	})
}

func TestInsertReturning(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		people, err := pgxutil.InsertReturning(ctx, conn, pgx.Identifier{"t"}, []map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}}, "*", pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 2)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 42, people[0].Age)
		require.EqualValues(t, 2, people[1].ID)
		require.Equal(t, "Jane", people[1].Name)
		require.EqualValues(t, 40, people[1].Age)

		people, err = pgxutil.Select(ctx, conn, `select * from t order by id`, nil, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 42, people[0].Age)
		require.EqualValues(t, 2, people[1].ID)
		require.Equal(t, "Jane", people[1].Name)
		require.EqualValues(t, 40, people[1].Age)
	})
}

func TestQueueInsertReturning(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		batch := &pgx.Batch{}
		var people []*Person
		pgxutil.QueueInsertReturning(
			batch,
			pgx.Identifier{"t"},
			[]map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}},
			"*",
			pgx.RowToAddrOfStructByPos[Person],
			&people,
		)
		err = conn.SendBatch(ctx, batch).Close()
		require.NoError(t, err)
		require.Len(t, people, 2)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 42, people[0].Age)
		require.EqualValues(t, 2, people[1].ID)
		require.Equal(t, "Jane", people[1].Name)
		require.EqualValues(t, 40, people[1].Age)

		people, err = pgxutil.Select(ctx, conn, `select * from t order by id`, nil, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 42, people[0].Age)
		require.EqualValues(t, 2, people[1].ID)
		require.Equal(t, "Jane", people[1].Name)
		require.EqualValues(t, 40, people[1].Age)
	})
}

func TestInsertSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		testName  string
		tableName pgx.Identifier
		rows      []map[string]any
		sql       string
		args      []any
	}{
		{
			testName:  "Single row",
			tableName: pgx.Identifier{"people"},
			rows:      []map[string]any{{"name": "Adam", "sex": "male"}},
			sql:       `insert into "people" ("name", "sex") values ($1, $2) returning *`,
			args:      []any{"Adam", "male"},
		},
		{
			testName:  "Multiple rows",
			tableName: pgx.Identifier{"people"},
			rows:      []map[string]any{{"name": "Adam", "sex": "male"}, {"name": "Eve", "sex": "female"}, {"name": "Cain", "sex": "male"}, {"name": "Abel", "sex": "male"}},
			sql:       `insert into "people" ("name", "sex") values ($1, $2), ($3, $4), ($5, $6), ($7, $8) returning *`,
			args:      []any{"Adam", "male", "Eve", "female", "Cain", "male", "Abel", "male"},
		},
		{
			testName:  "Schema qualified table",
			tableName: pgx.Identifier{"public", "people"},
			rows:      []map[string]any{{"name": "Adam", "sex": "male"}},
			sql:       `insert into "public"."people" ("name", "sex") values ($1, $2) returning *`,
			args:      []any{"Adam", "male"},
		},
		{
			testName:  "Table with special characters",
			tableName: pgx.Identifier{"Bible Characters"},
			rows:      []map[string]any{{"name": "Adam", "sex": "male"}},
			sql:       `insert into "Bible Characters" ("name", "sex") values ($1, $2) returning *`,
			args:      []any{"Adam", "male"},
		},
		{
			testName:  "Column with special characters",
			tableName: pgx.Identifier{"people"},
			rows:      []map[string]any{{"Complete Name": "Adam", "sex": "male"}},
			sql:       `insert into "people" ("Complete Name", "sex") values ($1, $2) returning *`,
			args:      []any{"Adam", "male"},
		},
		{
			testName:  "SQLValue",
			tableName: pgx.Identifier{"people"},
			rows:      []map[string]any{{"name": "Adam", "sex": "male", "updated_at": pgxutil.SQLValue("now()")}},
			sql:       `insert into "people" ("name", "sex", "updated_at") values ($1, $2, now()) returning *`,
			args:      []any{"Adam", "male"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			sql, args := pgxutil.Private_insertSQL(tt.tableName, tt.rows, "*")
			assert.Equal(t, tt.sql, sql)
			assert.Equal(t, tt.args, args)
		})
	}
}

func TestInsertRow(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		err = pgxutil.InsertRow(ctx, conn, pgx.Identifier{"t"}, map[string]any{"name": "John", "age": 42})
		require.NoError(t, err)

		person, err := pgxutil.SelectRow(ctx, conn, `select * from t where id = $1`, []any{1}, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.EqualValues(t, 1, person.ID)
		require.Equal(t, "John", person.Name)
		require.EqualValues(t, 42, person.Age)
	})
}

func TestQueueInsertRow(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		batch := &pgx.Batch{}
		pgxutil.QueueInsertRow(batch, pgx.Identifier{"t"}, map[string]any{"name": "John", "age": 42})
		err = conn.SendBatch(ctx, batch).Close()
		require.NoError(t, err)

		person, err := pgxutil.SelectRow(ctx, conn, `select * from t where id = $1`, []any{1}, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.EqualValues(t, 1, person.ID)
		require.Equal(t, "John", person.Name)
		require.EqualValues(t, 42, person.Age)
	})
}

func TestInsertRowReturning(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		person, err := pgxutil.InsertRowReturning(ctx, conn, pgx.Identifier{"t"}, map[string]any{"name": "John", "age": 42}, "*", pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.EqualValues(t, 1, person.ID)
		require.Equal(t, "John", person.Name)
		require.EqualValues(t, 42, person.Age)

		person, err = pgxutil.SelectRow(ctx, conn, `select * from t where id = $1`, []any{person.ID}, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.EqualValues(t, 1, person.ID)
		require.Equal(t, "John", person.Name)
		require.EqualValues(t, 42, person.Age)
	})
}

func TestQueueInsertRowReturning(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		batch := &pgx.Batch{}
		var person *Person
		pgxutil.QueueInsertRowReturning(batch, pgx.Identifier{"t"}, map[string]any{"name": "John", "age": 42}, "*", pgx.RowToAddrOfStructByPos[Person], &person)
		err = conn.SendBatch(ctx, batch).Close()
		require.NoError(t, err)
		require.EqualValues(t, 1, person.ID)
		require.Equal(t, "John", person.Name)
		require.EqualValues(t, 42, person.Age)

		person, err = pgxutil.SelectRow(ctx, conn, `select * from t where id = $1`, []any{person.ID}, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.EqualValues(t, 1, person.ID)
		require.Equal(t, "John", person.Name)
		require.EqualValues(t, 42, person.Age)
	})
}

func TestInsertRowSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		testName  string
		tableName pgx.Identifier
		values    map[string]any
		sql       string
		args      []any
	}{
		{
			testName:  "Normal",
			tableName: pgx.Identifier{"people"},
			values:    map[string]any{"name": "Adam", "sex": "male"},
			sql:       `insert into "people" ("name", "sex") values ($1, $2) returning *`,
			args:      []any{"Adam", "male"},
		},
		{
			testName:  "Schema qualified table",
			tableName: pgx.Identifier{"public", "people"},
			values:    map[string]any{"name": "Adam", "sex": "male"},
			sql:       `insert into "public"."people" ("name", "sex") values ($1, $2) returning *`,
			args:      []any{"Adam", "male"},
		},
		{
			testName:  "Table with special characters",
			tableName: pgx.Identifier{"Bible Characters"},
			values:    map[string]any{"name": "Adam", "sex": "male"},
			sql:       `insert into "Bible Characters" ("name", "sex") values ($1, $2) returning *`,
			args:      []any{"Adam", "male"},
		},
		{
			testName:  "Column with special characters",
			tableName: pgx.Identifier{"people"},
			values:    map[string]any{"Complete Name": "Adam", "sex": "male"},
			sql:       `insert into "people" ("Complete Name", "sex") values ($1, $2) returning *`,
			args:      []any{"Adam", "male"},
		},
		{
			testName:  "SQLValue",
			tableName: pgx.Identifier{"people"},
			values:    map[string]any{"name": "Adam", "sex": "male", "updated_at": pgxutil.SQLValue("now()")},
			sql:       `insert into "people" ("name", "sex", "updated_at") values ($1, $2, now()) returning *`,
			args:      []any{"Adam", "male"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			sql, args := pgxutil.Private_insertRowSQL(tt.tableName, tt.values, "*")
			assert.Equal(t, tt.sql, sql)
			assert.Equal(t, tt.args, args)
		})
	}
}

func TestExecRow(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		people, err := pgxutil.InsertReturning(ctx, conn, pgx.Identifier{"t"}, []map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}}, "*", pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 2)
		require.EqualValues(t, 1, people[0].ID)
		require.EqualValues(t, 2, people[1].ID)

		ct, err := pgxutil.ExecRow(ctx, conn, "update t set name = 'Bill' where id = $1", -1)
		require.ErrorIs(t, err, pgx.ErrNoRows)
		require.Equal(t, "UPDATE 0", ct.String())

		ct, err = pgxutil.ExecRow(ctx, conn, "update t set name = 'Bill'")
		require.ErrorContains(t, err, "too many rows")
		require.Equal(t, "UPDATE 2", ct.String())
	})
}

func TestQueueExecRow(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		people, err := pgxutil.InsertReturning(ctx, conn, pgx.Identifier{"t"}, []map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}}, "*", pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 2)
		require.EqualValues(t, 1, people[0].ID)
		require.EqualValues(t, 2, people[1].ID)

		batch := &pgx.Batch{}
		pgxutil.QueueExecRow(batch, "update t set name = 'Bill' where id = $1", -1)
		err = conn.SendBatch(ctx, batch).Close()
		require.ErrorIs(t, err, pgx.ErrNoRows)

		batch = &pgx.Batch{}
		pgxutil.QueueExecRow(batch, "update t set name = 'Bill'")
		err = conn.SendBatch(ctx, batch).Close()
		require.ErrorContains(t, err, "too many rows")
	})
}

func TestUpdateSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		testName    string
		tableName   pgx.Identifier
		setValues   map[string]any
		whereValues map[string]any
		sql         string
		args        []any
	}{
		{
			testName:  "Update all rows",
			tableName: pgx.Identifier{"products"},
			setValues: map[string]any{"color": "green"},
			sql:       `update "products" set "color" = $1 returning *`,
			args:      []any{"green"},
		},
		{
			testName:  "Update multiple columns",
			tableName: pgx.Identifier{"products"},
			setValues: map[string]any{"color": "green", "size": "large"},
			sql:       `update "products" set "color" = $1, "size" = $2 returning *`,
			args:      []any{"green", "large"},
		},
		{
			testName:  "Schema qualified table",
			tableName: pgx.Identifier{"store", "products"},
			setValues: map[string]any{"color": "green"},
			sql:       `update "store"."products" set "color" = $1 returning *`,
			args:      []any{"green"},
		},
		{
			testName:  "Table with special characters",
			tableName: pgx.Identifier{"for sale products"},
			setValues: map[string]any{"color": "green"},
			sql:       `update "for sale products" set "color" = $1 returning *`,
			args:      []any{"green"},
		},
		{
			testName:  "Column with special characters",
			tableName: pgx.Identifier{"products"},
			setValues: map[string]any{"color": "green", "American size": "large"},
			sql:       `update "products" set "American size" = $1, "color" = $2 returning *`,
			args:      []any{"large", "green"},
		},
		{
			testName:    "Update some rows",
			tableName:   pgx.Identifier{"products"},
			setValues:   map[string]any{"color": "green"},
			whereValues: map[string]any{"color": "red"},
			sql:         `update "products" set "color" = $1 where "color" = $2 returning *`,
			args:        []any{"green", "red"},
		},
		{
			testName:    "Set with SQLValue",
			tableName:   pgx.Identifier{"products"},
			setValues:   map[string]any{"color": "green", "updated_at": pgxutil.SQLValue("now()")},
			whereValues: map[string]any{"color": "red"},
			sql:         `update "products" set "color" = $1, "updated_at" = now() where "color" = $2 returning *`,
			args:        []any{"green", "red"},
		},
		{
			testName:    "Where with SQLValue",
			tableName:   pgx.Identifier{"products"},
			setValues:   map[string]any{"color": "green"},
			whereValues: map[string]any{"color": "red", "updated_at": pgxutil.SQLValue("now()")},
			sql:         `update "products" set "color" = $1 where "color" = $2 and "updated_at" = now() returning *`,
			args:        []any{"green", "red"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			sql, args := pgxutil.Private_updateSQL(tt.tableName, tt.setValues, tt.whereValues, "*")
			assert.Equal(t, tt.sql, sql)
			assert.Equal(t, tt.args, args)
		})
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		ct, err := pgxutil.Update(ctx, conn, pgx.Identifier{"t"}, map[string]any{"age": 42}, nil)
		require.NoError(t, err)
		require.EqualValues(t, 0, ct.RowsAffected())

		_, err = pgxutil.Insert(ctx, conn, pgx.Identifier{"t"}, []map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}})
		require.NoError(t, err)

		ct, err = pgxutil.Update(ctx, conn, pgx.Identifier{"t"}, map[string]any{"age": 70}, map[string]any{"name": "John"})
		require.NoError(t, err)
		require.EqualValues(t, 1, ct.RowsAffected())

		people, err := pgxutil.Select(ctx, conn, `select * from t where age = $1`, []any{70}, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 1)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 70, people[0].Age)
	})
}

func TestQueueUpdate(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		batch := &pgx.Batch{}
		var ct pgconn.CommandTag
		pgxutil.QueueUpdate(batch, pgx.Identifier{"t"}, map[string]any{"age": 42}, nil, &ct)
		err = conn.SendBatch(ctx, batch).Close()
		require.NoError(t, err)
		require.EqualValues(t, 0, ct.RowsAffected())

		_, err = pgxutil.Insert(ctx, conn, pgx.Identifier{"t"}, []map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}})
		require.NoError(t, err)

		batch = &pgx.Batch{}
		pgxutil.QueueUpdate(batch, pgx.Identifier{"t"}, map[string]any{"age": 70}, map[string]any{"name": "John"}, &ct)
		err = conn.SendBatch(ctx, batch).Close()
		require.NoError(t, err)
		require.EqualValues(t, 1, ct.RowsAffected())

		people, err := pgxutil.Select(ctx, conn, `select * from t where age = $1`, []any{70}, pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 1)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 70, people[0].Age)
	})
}

func TestUpdateReturning(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		people, err := pgxutil.UpdateReturning(ctx, conn, pgx.Identifier{"t"}, map[string]any{"age": 42}, nil, "*", pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 0)

		people, err = pgxutil.InsertReturning(ctx, conn, pgx.Identifier{"t"}, []map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}}, "*", pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 2)

		people, err = pgxutil.UpdateReturning(ctx, conn, pgx.Identifier{"t"}, map[string]any{"age": 70}, map[string]any{"name": "John"}, "*", pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 1)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 70, people[0].Age)
	})
}

func TestQueueUpdateReturning(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		batch := &pgx.Batch{}
		var people []*Person
		pgxutil.QueueUpdateReturning(batch, pgx.Identifier{"t"}, map[string]any{"age": 42}, nil, "*", pgx.RowToAddrOfStructByPos[Person], &people)
		err = conn.SendBatch(ctx, batch).Close()
		require.NoError(t, err)
		require.Len(t, people, 0)

		people, err = pgxutil.InsertReturning(ctx, conn, pgx.Identifier{"t"}, []map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}}, "*", pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 2)

		batch = &pgx.Batch{}
		pgxutil.QueueUpdateReturning(batch, pgx.Identifier{"t"}, map[string]any{"age": 70}, map[string]any{"name": "John"}, "*", pgx.RowToAddrOfStructByPos[Person], &people)
		err = conn.SendBatch(ctx, batch).Close()
		require.NoError(t, err)
		require.Len(t, people, 1)
		require.EqualValues(t, 1, people[0].ID)
		require.Equal(t, "John", people[0].Name)
		require.EqualValues(t, 70, people[0].Age)
	})
}

func TestUpdateRow(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		err = pgxutil.UpdateRow(ctx, conn, pgx.Identifier{"t"}, map[string]any{"age": 42}, nil)
		require.ErrorIs(t, err, pgx.ErrNoRows)

		people, err := pgxutil.InsertReturning(ctx, conn, pgx.Identifier{"t"}, []map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}}, "*", pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 2)

		err = pgxutil.UpdateRow(ctx, conn, pgx.Identifier{"t"}, map[string]any{"age": 70}, map[string]any{"name": "John"})
		require.NoError(t, err)

		err = pgxutil.UpdateRow(ctx, conn, pgx.Identifier{"t"}, map[string]any{"age": 70}, nil)
		require.ErrorContains(t, err, "too many rows")
	})
}

func TestUpdateRowReturning(t *testing.T) {
	t.Parallel()

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create temporary table t (
	id int primary key generated by default as identity,
	name text not null,
	age int
)`)
		require.NoError(t, err)

		type Person struct {
			ID   int32
			Name string
			Age  int32
		}

		person, err := pgxutil.UpdateRowReturning(ctx, conn, pgx.Identifier{"t"}, map[string]any{"age": 42}, nil, "*", pgx.RowToAddrOfStructByPos[Person])
		require.ErrorIs(t, err, pgx.ErrNoRows)
		require.Nil(t, person)

		people, err := pgxutil.InsertReturning(ctx, conn, pgx.Identifier{"t"}, []map[string]any{{"name": "John", "age": 42}, {"name": "Jane", "age": 40}}, "*", pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.Len(t, people, 2)

		person, err = pgxutil.UpdateRowReturning(ctx, conn, pgx.Identifier{"t"}, map[string]any{"age": 70}, map[string]any{"name": "John"}, "*", pgx.RowToAddrOfStructByPos[Person])
		require.NoError(t, err)
		require.NotNil(t, person)
		require.EqualValues(t, 1, person.ID)
		require.Equal(t, "John", person.Name)
		require.EqualValues(t, 70, person.Age)

		_, err = pgxutil.UpdateRowReturning(ctx, conn, pgx.Identifier{"t"}, map[string]any{"age": 70}, nil, "*", pgx.RowToAddrOfStructByPos[Person])
		require.ErrorContains(t, err, "too many rows")
	})
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
