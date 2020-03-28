package pgxutil_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

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

func TestSelectString(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			err    string
			result string
		}{
			{"select 'Hello, world!'", "", "Hello, world!"},
			{"select 42", "", "42"},
			{"select null::text", "value is null", ""},
			{"select 'Hello, world!' where 1=0", "no rows in result set", ""},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectString(ctx, tx, tt.sql)
			if tt.err == "" {
				assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			} else {
				assert.EqualErrorf(t, err, tt.err, "%d. %s", i, tt.sql)
			}
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectByteSlice(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			err    string
			result []byte
		}{
			{"select 'Hello, world!'", "", []byte("Hello, world!")},
			{"select 42", "", []byte{0, 0, 0, 42}},
			{"select null::text", "value is null", nil},
			{"select 'Hello, world!' where 1=0", "no rows in result set", nil},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectByteSlice(ctx, tx, tt.sql)
			if tt.err == "" {
				assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			} else {
				assert.EqualErrorf(t, err, tt.err, "%d. %s", i, tt.sql)
			}
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectInt64(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			err    string
			result int64
		}{
			{"select 99999999999::bigint", "", 99999999999},
			{"select 42::smallint", "", 42},
			{"select null::bigint", "value is null", 0},
			{"select 42::bigint where 1=0", "no rows in result set", 0},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectInt64(ctx, tx, tt.sql)
			if tt.err == "" {
				assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			} else {
				assert.EqualErrorf(t, err, tt.err, "%d. %s", i, tt.sql)
			}
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}

func TestSelectFloat64(t *testing.T) {
	t.Parallel()
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		tests := []struct {
			sql    string
			err    string
			result float64
		}{
			{"select 1.2345::float8", "", 1.2345},
			{"select 1.23::float4", "", 1.23},
			{"select 1.2345::numeric", "", 1.2345},
			{"select 99999999999::bigint", "", 99999999999},
			{"select 42::smallint", "", 42},
			{"select null::float8", "value is null", 0},
			{"select 42::float8 where 1=0", "no rows in result set", 0},
		}
		for i, tt := range tests {
			v, err := pgxutil.SelectFloat64(ctx, tx, tt.sql)
			if tt.err == "" {
				assert.NoErrorf(t, err, "%d. %s", i, tt.sql)
			} else {
				assert.EqualErrorf(t, err, tt.err, "%d. %s", i, tt.sql)
			}
			assert.Equalf(t, tt.result, v, "%d. %s", i, tt.sql)
		}
	})
}
