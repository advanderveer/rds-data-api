package rdsdataapi

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	rdsds "github.com/aws/aws-sdk-go/service/rdsdataservice"
)

func init() {
	sql.Register("rds-data-api", &Driver{})
}

type Driver struct{}

func (d *Driver) Open(s string) (_ driver.Conn, err error) {
	return Open(s)
}

// Conn is a connection to a database. It is not used concurrently by multiple goroutines.
type Conn struct {
	closed         bool                  // whether the conn has been blosed
	databaseName   string                // name of the database on which queries will be performed
	resourceARN    string                // the aws resource accesses with this conn
	secretARN      string                // the aws secret that provides access to the resource
	rdsDataService *rdsds.RDSDataService // AWS RDS data service API
}

func Open(q string) (_ driver.Conn, err error) {
	cfg, err := url.ParseQuery(q)
	if err != nil {
		return nil, fmt.Errorf("failed to parse conn string as url query: %w", err) // @TODO test
	}

	sess := session.New()
	// @TODO don't hardcode region, but does that mean we need other configs as well?

	c := &Conn{
		databaseName:   cfg.Get("Database"),
		resourceARN:    cfg.Get("ResourceARN"),
		secretARN:      cfg.Get("SecretARN"),
		rdsDataService: rdsds.New(sess, aws.NewConfig().WithRegion("eu-west-1")),
	}

	if c.resourceARN == "" || c.secretARN == "" || c.databaseName == "" {
		return nil, fmt.Errorf("required configuration value 'Database', 'ResourceARN' or 'SecretARN' are missing") // @TODO test
	}

	return c, err
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement,
// it must not store the context within the statement itself.
func (c *Conn) PrepareContext(ctx context.Context, query string) (_ driver.Stmt, err error) {
	return &Stmt{query: query, conn: c}, nil
}

// BeginTx starts and returns a new transaction.
// If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
//
// This must check opts.Isolation to determine if there is a set
// isolation level. If the driver does not support a non-default
// level and one is set or if there is a non-default isolation level
// that is not supported, an error must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only
// value is true to either set the read-only transaction property if supported
// or return an error if it is not supported.
func (c *Conn) BeginTx(ctx context.Context, opts sql.TxOptions) (_ driver.Tx, err error) {
	if c.rdsDataService == nil {
		return nil, fmt.Errorf("connection already closed") //@TODO test
	}

	panic("not implemented, yet")
	return
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *Conn) Close() (err error) { c.rdsDataService = nil; return }

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, err error) {
	out, err := c.execute(ctx, query, args)
	if err != nil {
		return nil, err
	}

	return &Result{output: out}, nil
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, err error) {
	out, err := c.execute(ctx, query, args)
	if err != nil {
		return nil, err
	}

	return &Rows{output: out}, nil
}

func (c *Conn) execute(ctx context.Context, query string, args []driver.NamedValue) (out *rdsds.ExecuteStatementOutput, err error) {
	params := make([]*rdsds.SqlParameter, len(args))
	for i, arg := range args {
		if arg.Name == "" {
			return nil, fmt.Errorf("support named SQL arguments are supported in query")
		}

		var f rdsds.Field
		switch t := arg.Value.(type) {
		case string:
			f = rdsds.Field{StringValue: aws.String(t)}
		case []byte:
			f = rdsds.Field{BlobValue: t}
		case bool:
			f = rdsds.Field{BooleanValue: &t}
		case float64:
			f = rdsds.Field{DoubleValue: &t}
		case int64:
			f = rdsds.Field{LongValue: &t}
		default:
			return nil, fmt.Errorf("supports string, []byte, bool, float64 or int64 for argument '%s', got: %T, ", arg.Name, arg.Value)
		}

		params[i] = &rdsds.SqlParameter{
			Name:  aws.String(arg.Name),
			Value: &f,
		}
	}

	if out, err = c.rdsDataService.ExecuteStatementWithContext(ctx, &rdsds.ExecuteStatementInput{
		// ContinueAfterTimeout:  aws.Bool(false), @TODO allow this to be configurable

		IncludeResultMetadata: aws.Bool(true), //must be set to true for row iteration
		Parameters:            params,
		Database:              aws.String(c.databaseName),
		ResourceArn:           aws.String(c.resourceARN),
		SecretArn:             aws.String(c.secretARN),
		Sql:                   aws.String(query),
	}); err != nil {
		return nil, fmt.Errorf("failed to execute statement: %w", err)
	}

	return
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *Conn) Begin() (driver.Tx, error) {
	panic("not implemented, use BeginTx instead")
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	panic("not implemented, use PrepareContext instead")
}

// Stmt is a prepared statement. It is bound to a Conn and not used by multiple goroutines concurrently.
type Stmt struct {
	conn   *Conn
	query  string
	closed bool
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (s *Stmt) Close() (err error) { s.closed = true; return }

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *Stmt) NumInput() int {

	// @TODO This is a limitation of this driver. The AWS API doesn't provide
	// us with an function that parses and returns the correct value. Creating
	// one ourselves is risky as this is security critical logic.
	return -1
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (_ driver.Rows, err error) {
	if s.closed {
		return nil, fmt.Errorf("statement already closed") //@TODO test
	}

	return s.conn.QueryContext(ctx, s.query, args)
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (_ driver.Result, err error) {
	if s.closed {
		return nil, fmt.Errorf("statement already closed") //@TODO test
	}

	return s.conn.ExecContext(ctx, s.query, args)
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	panic("not implemented, use ExecContext instead")
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	panic("not implemented, use QueryContext instead")
}

// Rows is an iterator over an executed query's results.
type Rows struct {
	output *rdsds.ExecuteStatementOutput
	closed bool
	pos    int
}

// Close closes the rows iterator.
func (r *Rows) Close() error { r.closed = true; return nil }

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *Rows) Columns() (cols []string) {
	cols = make([]string, len(r.output.ColumnMetadata))
	for i, c := range r.output.ColumnMetadata {
		cols[i] = aws.StringValue(c.Name)
	}

	return
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *Rows) Next(dest []driver.Value) (err error) {
	if r.closed {
		return fmt.Errorf("rows already closed") //@TODO test
	}

	if r.pos == len(r.output.Records) {
		return io.EOF
	}

	// read and increment, so decode errors don't cause infinite iteration
	row := r.output.Records[r.pos]
	r.pos++

	for i, field := range row {
		dest[i], err = decodeField(field)
		if err != nil {
			return fmt.Errorf("failed to decode field value: %w", err) //@TODO test
		}
	}

	return nil
}

// Result is the result of a query execution.
type Result struct{ output *rdsds.ExecuteStatementOutput }

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *Result) LastInsertId() (id int64, err error) {
	// @TODO implement: postgres doesn't suppor this, mysql does
	return
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *Result) RowsAffected() (n int64, err error) {
	return aws.Int64Value(r.output.NumberOfRecordsUpdated), nil
}

func decodeField(f *rdsds.Field) (v interface{}, err error) {
	switch {
	case f.BlobValue != nil:
		return f.BlobValue, nil
	case f.BooleanValue != nil:
		return *f.BooleanValue, nil
	case f.DoubleValue != nil:
		return *f.DoubleValue, nil
	case f.IsNull != nil:
		return nil, nil
	case f.LongValue != nil:
		return *f.LongValue, nil
	case f.StringValue != nil:
		return *f.StringValue, nil
	default:
		return nil, fmt.Errorf("field has no defined value")
	}

	return
}
