#!/bin/sh

export ENV_SCHEMA_PATTERN=public
export ENV_TABLE_PATTERN=tab%

export ENV_TABLE_NAMES=tab1,tab2,tab

export PGUSER=postgres
export PGPORT=5433
export PGHOST=127.0.0.1

./pgtabsz
