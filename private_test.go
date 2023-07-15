package pgxutil

import (
	"github.com/jackc/pgx/v5"
)

func Private_sanitizeIdentifier(s string) string {
	return sanitizeIdentifier(s)
}

func Private_insertRowSQL(tableName pgx.Identifier, values map[string]any, returningClause string) (sql string, args []any) {
	return insertRowSQL(tableName, values, returningClause)
}

func Private_insertSQL(tableName pgx.Identifier, rows []map[string]any, returningClause string) (sql string, args []any) {
	return insertSQL(tableName, rows, returningClause)
}

func Private_updateSQL(tableName pgx.Identifier, setValues, whereValues map[string]any, returningClause string) (sql string, args []any) {
	return updateSQL(tableName, setValues, whereValues, returningClause)
}
