package pgx2tabsz

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	tsz "github.com/takanoriyanagitani/go-pgtabsz"
)

type Pool struct{ *pgxpool.Pool }

type Rows struct{ pgx.Rows }

func (r Rows) ToTableInfo(_ context.Context) ([]tsz.TableInfo, error) {
	return pgx.CollectRows(r.Rows, pgx.RowToStructByName[tsz.TableInfo])
}

func (r Rows) ToSize(_ context.Context) ([]tsz.TableSizeInfo, error) {
	return pgx.CollectRows(r.Rows, pgx.RowToStructByName[tsz.TableSizeInfo])
}

func (p Pool) toTableRows(i tsz.TablesInput) tsz.IO[pgx.Rows] {
	return func(ctx context.Context) (pgx.Rows, error) {
		var schPat string = i.SchemaPattern
		var tabPat string = i.TablePattern
		return p.Pool.Query(ctx, tsz.GetTablesQuery, schPat, tabPat)
	}
}

func (p Pool) ToTables(i tsz.TablesInput) tsz.IO[[]tsz.TableInfo] {
	return tsz.Bind(
		p.toTableRows(i),
		func(rows pgx.Rows) tsz.IO[[]tsz.TableInfo] {
			return func(ctx context.Context) ([]tsz.TableInfo, error) {
				return Rows{rows}.ToTableInfo(ctx)
			}
		},
	)
}

func (p Pool) toSizeRows(i tsz.TableSizeInput) tsz.IO[pgx.Rows] {
	return func(ctx context.Context) (pgx.Rows, error) {
		var sch string = i.SchemaPattern
		var tbl []string = i.TableNames
		return p.Pool.Query(ctx, tsz.GetTableSizeQuery, sch, tbl)
	}
}

func (p Pool) ToTableSizes(i tsz.TableSizeInput) tsz.IO[[]tsz.TableSizeInfo] {
	return tsz.Bind(
		p.toSizeRows(i),
		func(rows pgx.Rows) tsz.IO[[]tsz.TableSizeInfo] {
			return func(ctx context.Context) ([]tsz.TableSizeInfo, error) {
				return Rows{rows}.ToSize(ctx)
			}
		},
	)
}

func (p Pool) AsTablesSource() tsz.TablesSource { return p.ToTables }

func (p Pool) AsSizeSource() tsz.TableSizesSource { return p.ToTableSizes }
