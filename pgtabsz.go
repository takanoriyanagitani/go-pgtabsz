package pgtabsz

import (
	"context"
	"database/sql"
	"strings"
)

const GetTableSizeQuery string = `
	SELECT
		c.oid::TEXT                                     AS oid,
		n.nspname::TEXT                                 AS table_schema,
		c.reltuples::REAL                               AS row_estimate,
		c.relname::TEXT                                 AS table_name,
		PG_TOTAL_RELATION_SIZE(c.oid)::BIGINT           AS total_bytes,
		PG_INDEXES_SIZE(c.oid)::BIGINT                  AS index_bytes,
		PG_TOTAL_RELATION_SIZE(c.reltoastrelid)::BIGINT AS toast_bytes
	FROM pg_class c
	LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE
		c.relkind = 'r'
		AND n.nspname LIKE $1::TEXT -- schema name
		AND c.relname = ANY($2::TEXT[]) -- table names
`

const GetTablesQuery string = `
	SELECT
		table_schema::TEXT AS table_schema,
		table_name::TEXT   AS table_name
	FROM information_schema.tables
	WHERE
		table_schema LIKE $1::TEXT
		AND table_name LIKE $2::TEXT
`

type TableSizeInfo struct {
	OID         string          `db:"oid"`
	TableSchema string          `db:"table_schema"`
	RowEstimate float32         `db:"row_estimate"`
	TableName   string          `db:"table_name"`
	TotalBytes  int64           `db:"total_bytes"`
	IndexBytes  int64           `db:"index_bytes"`
	ToastBytes  sql.Null[int64] `db:"toast_bytes"`
}

type TableInfo struct {
	TableSchema string `db:"table_schema"`
	TableName   string `db:"table_name"`
}

type TablesInput struct {
	SchemaPattern string
	TablePattern  string
}

var TablesInputDefault TablesInput = TablesInput{
	SchemaPattern: "%",
	TablePattern:  "%",
}

type TableSizeInput struct {
	SchemaPattern string

	// Note: nil or empty array will get no info.
	TableNames []string
}

func (s TableSizeInput) WithTableNamesString(
	names string,
	sep string,
) TableSizeInput {
	var splited []string = strings.Split(names, sep)
	s.TableNames = splited
	return s
}

var TableSizeInputDefault = TableSizeInput{
	SchemaPattern: "%",
	TableNames:    nil,
}

type IO[T any] func(context.Context) (T, error)

type TablesSource func(TablesInput) IO[[]TableInfo]

type TableSizesSource func(TableSizeInput) IO[[]TableSizeInfo]

func Bind[T, U any](
	i IO[T],
	m func(T) IO[U],
) IO[U] {
	return func(ctx context.Context) (u U, e error) {
		t, e := i(ctx)
		if nil != e {
			return
		}
		return m(t)(ctx)
	}
}

func Lift[T, U any](
	pure func(T) (U, error),
) func(T) IO[U] {
	return func(t T) IO[U] {
		return func(_ context.Context) (U, error) {
			return pure(t)
		}
	}
}

type Void struct{}

var Empty Void

func All[T any](ios ...IO[T]) IO[[]T] {
	return func(ctx context.Context) ([]T, error) {
		var ret []T = make([]T, 0, len(ios))
		for _, i := range ios {
			t, e := i(ctx)
			if nil != e {
				return nil, e
			}
			ret = append(ret, t)
		}
		return ret, nil
	}
}
