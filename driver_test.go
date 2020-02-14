package rdsdataapi_test

import (
	"database/sql"
	"net/url"
	"os"
	"testing"

	_ "github.com/advanderveer/rds-data-api"
)

func envCfgOrSkip(tb testing.TB) url.Values {
	cfg := url.Values{}
	cfg.Add("SecretARN", os.Getenv("DATA_API_SECRET_ARN"))
	cfg.Add("ResourceARN", os.Getenv("DATA_API_RESOURCE_ARN"))
	if cfg.Get("ResourceARN") == "" || cfg.Get("SecretARN") == "" {
		tb.Skipf("please provide a database to test against with the DATA_API_RESOURCE_ARN and DATA_API_SECRET_ARN environment variable")
	}

	return cfg
}

func TestDriverQuery(t *testing.T) {
	limit := 10

	cfg := envCfgOrSkip(t)
	cfg.Add("Database", "mysql")

	db, err := sql.Open("rds-data-api", cfg.Encode())
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	rows, err := db.Query("select table_catalog, table_schema, table_name from information_schema.tables limit :n", sql.Named("n", limit))
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to ask for columns: %v", err)
	}

	if len(cols) != 3 {
		t.Fatalf("expected this nr of columns, got: %v", len(cols))
	}

	var n int
	for rows.Next() {
		var (
			tableCatalog string
			tableSchema  string
			tableName    string
		)

		if err := rows.Scan(&tableCatalog, &tableSchema, &tableName); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		if tableCatalog == "" || tableSchema == "" || tableName == "" {
			t.Fatalf("each column should have been scanned")
		}

		n++
	}

	if n != limit {
		t.Fatalf("expected to have scanned the limit nr of rows, got: %v", n)
	}
}

func TestDriverExec(t *testing.T) {
	cfg := envCfgOrSkip(t)
	cfg.Add("Database", "mysql")

	db, err := sql.Open("rds-data-api", cfg.Encode())
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	res, err := db.Exec("CREATE DATABASE IF NOT EXISTS bar;")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	res, err = db.Exec("CREATE TABLE IF NOT EXISTS bar.foo (id serial PRIMARY KEY);")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	res, err = db.Exec("INSERT INTO bar.foo VALUES ();")
	if err != nil {
		t.Fatalf("failed to insert into table: %v", err)
	}

	aff, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get affected rows: %v", err)
	}

	if aff != 1 {
		t.Fatalf("expected these nr of rows to be affected, got: %d", aff)
	}

	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("failed to create last insert id: %v", err)
	}

	if id != 1 {
		t.Fatalf("expected lastInsertID to succeed with this id, got: %v", id)
	}

	res, err = db.Exec("DROP TABLE IF EXISTS bar.foo;")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	aff, err = res.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get affected rows: %v", err)
	}

	// NOTE: is this a with AWS, do more research if it should be 1
	if aff != 0 {
		t.Fatalf("expected these nr of rows to be affected, got: %d", aff)
	}
}

func TestDriverTxIsolationAndCommit(t *testing.T) {
	cfg := envCfgOrSkip(t)
	cfg.Add("Database", "mysql")

	db, err := sql.Open("rds-data-api", cfg.Encode())
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	// tx1
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to start tx: %v", err)
	}

	_, err = tx.Exec("CREATE DATABASE IF NOT EXISTS bar;")
	if err != nil {
		t.Fatalf("failed to exec in tx: %v", err)
	}

	_, err = tx.Exec("CREATE TABLE IF NOT EXISTS bar.foo (id serial PRIMARY KEY);")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = tx.Exec("INSERT INTO bar.foo VALUES ();")
	if err != nil {
		t.Fatalf("failed to insert with tx: %v", err)
	}

	rows, err := db.Query("SELECT * FROM bar.foo LIMIT 1")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	var n int
	for rows.Next() {
		n++
	}

	if n != 0 {
		t.Fatalf("should have this amount of rows before commit, got: %d", n)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	rows, err = db.Query("SELECT * FROM bar.foo LIMIT 1")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	for rows.Next() {
		n++
	}

	if n != 1 {
		t.Fatalf("should have this amount of rows after commit, got: %d", n)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS bar.foo;")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
}

func TestDriverTxRollback(t *testing.T) {

	cfg := envCfgOrSkip(t)
	cfg.Add("Database", "mysql")

	db, err := sql.Open("rds-data-api", cfg.Encode())
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	// setup database
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS bar;")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS bar.foo (id serial PRIMARY KEY);")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// create a transaction that inserts, but roll back
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to create tx")
	}

	_, err = tx.Exec("INSERT INTO bar.foo VALUES ();")
	if err != nil {
		t.Fatalf("failed to insert with tx: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// assert no rows were inserted
	rows, err := db.Query("SELECT * FROM bar.foo LIMIT 1")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	var n int
	for rows.Next() {
		n++
	}

	if n != 0 {
		t.Fatalf("should have this amount of rows after rollback, got: %d", n)
	}

	// clean up
	_, err = db.Exec("DROP TABLE IF EXISTS bar.foo;")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
}
