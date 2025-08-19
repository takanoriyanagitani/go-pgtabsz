package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	tsz "github.com/takanoriyanagitani/go-pgtabsz"
	tsx "github.com/takanoriyanagitani/go-pgtabsz/repo/pgx2tabsz"
)

var pool tsz.IO[tsx.Pool] = func(ctx context.Context) (tsx.Pool, error) {
	p, e := pgxpool.New(ctx, "")
	return tsx.Pool{Pool: p}, e
}

var tabSource tsz.IO[tsz.TablesSource] = tsz.Bind(
	pool,
	tsz.Lift(func(p tsx.Pool) (tsz.TablesSource, error) {
		return p.AsTablesSource(), nil
	}),
)

var szSource tsz.IO[tsz.TableSizesSource] = tsz.Bind(
	pool,
	tsz.Lift(func(p tsx.Pool) (tsz.TableSizesSource, error) {
		return p.AsSizeSource(), nil
	}),
)

func envkey2var(key string) tsz.IO[string] {
	return func(_ context.Context) (string, error) {
		val, ok := os.LookupEnv(key)
		if !ok {
			return "", fmt.Errorf("environment variable %s is not set", key)
		}
		return val, nil
	}
}

var env2tabinput tsz.IO[tsz.TablesInput] = tsz.Bind(
	envkey2var("ENV_SCHEMA_PATTERN"),
	func(s string) tsz.IO[tsz.TablesInput] {
		return tsz.Bind(
			envkey2var("ENV_TABLE_PATTERN"),
			tsz.Lift(func(t string) (tsz.TablesInput, error) {
				return tsz.TablesInput{
					SchemaPattern: s,
					TablePattern:  t,
				}, nil
			}),
		)
	},
)

var env2szinput tsz.IO[tsz.TableSizeInput] = tsz.Bind(
	envkey2var("ENV_SCHEMA_PATTERN"),
	func(s string) tsz.IO[tsz.TableSizeInput] {
		return tsz.Bind(
			envkey2var("ENV_TABLE_NAMES"),
			tsz.Lift(func(t string) (tsz.TableSizeInput, error) {
				ti := tsz.TableSizeInput{SchemaPattern: s}.
					WithTableNamesString(t, ",")
				return ti, nil
			}),
		)
	},
)

var tables tsz.IO[[]tsz.TableInfo] = tsz.Bind(
	tabSource,
	func(t tsz.TablesSource) tsz.IO[[]tsz.TableInfo] {
		return tsz.Bind(env2tabinput, t)
	},
)

var sizes tsz.IO[[]tsz.TableSizeInfo] = tsz.Bind(
	szSource,
	func(t tsz.TableSizesSource) tsz.IO[[]tsz.TableSizeInfo] {
		return tsz.Bind(env2szinput, t)
	},
)

func printTableInfo(ti tsz.TableInfo) tsz.IO[tsz.Void] {
	return func(_ context.Context) (tsz.Void, error) {
		fmt.Printf("%v\n", ti)
		return tsz.Empty, nil
	}
}

func printSizeInfo(si tsz.TableSizeInfo) tsz.IO[tsz.Void] {
	return func(_ context.Context) (tsz.Void, error) {
		fmt.Printf("%v\n", si)
		return tsz.Empty, nil
	}
}

func printTables(t []tsz.TableInfo) tsz.IO[tsz.Void] {
	return func(ctx context.Context) (tsz.Void, error) {
		for _, ti := range t {
			_, e := printTableInfo(ti)(ctx)
			if nil != e {
				return tsz.Empty, e
			}
		}
		return tsz.Empty, nil
	}
}

func printSizes(s []tsz.TableSizeInfo) tsz.IO[tsz.Void] {
	return func(ctx context.Context) (tsz.Void, error) {
		for _, si := range s {
			_, e := printSizeInfo(si)(ctx)
			if nil != e {
				return tsz.Empty, e
			}
		}
		return tsz.Empty, nil
	}
}

var sub tsz.IO[tsz.Void] = func(ctx context.Context) (tsz.Void, error) {
	var it tsz.IO[tsz.Void] = tsz.Bind(tables, printTables)
	var is tsz.IO[tsz.Void] = tsz.Bind(sizes, printSizes)
	var all tsz.IO[[]tsz.Void] = tsz.All(it, is)
	_, e := all(ctx)
	return tsz.Empty, e
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
