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
	transactionID  string                // the id of a transaction if one was started
}

func Open(q string) (_ driver.Conn, err error) {
	cfg, err := url.ParseQuery(q)
	if err != nil {
		return nil, fmt.Errorf("failed to parse conn string as url query: %w", err) // @TODO test
	}

	sess := session.New()

	c := &Conn{
		databaseName: cfg.Get("Database"),
		resourceARN:  cfg.Get("ResourceARN"),
		secretARN:    cfg.Get("SecretARN"),

		// @TODO don't hardcode region, but does that mean user need to be able to pass other configs as well?
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
	if c.rdsDataService == nil {
		return nil, fmt.Errorf("connection already closed") //@TODO test
	}

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

	if c.transactionID != "" {
		return nil, fmt.Errorf("a transaction already started") //@TODO test
	}

	var out *rdsds.BeginTransactionOutput
	if out, err = c.rdsDataService.BeginTransactionWithContext(ctx, &rdsds.BeginTransactionInput{
		// Schema: @TODO add schema support
		Database:    aws.String(c.databaseName),
		ResourceArn: aws.String(c.resourceARN),
		SecretArn:   aws.String(c.secretARN),
	}); err != nil {
		return nil, fmt.Errorf("failed to being transaction: %w", err)
	}

	c.transactionID = aws.StringValue(out.TransactionId)
	return c, nil
}

func (c *Conn) Commit() (err error) {
	if c.transactionID == "" {
		return fmt.Errorf("no open transaction to commit") //@TODO test
	}

	// @TODO do we want to allow the user the option to configure a timeout?
	ctx := context.Background()

	if _, err = c.rdsDataService.CommitTransactionWithContext(ctx, &rdsds.CommitTransactionInput{
		TransactionId: aws.String(c.transactionID),
		ResourceArn:   aws.String(c.resourceARN),
		SecretArn:     aws.String(c.secretARN),
	}); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	c.transactionID = ""
	return
}

func (c *Conn) Rollback() (err error) {
	if c.transactionID == "" {
		return fmt.Errorf("no open transaction to rollback") //@TODO test
	}

	// @TODO do we want to allow the user the option to configure a timeout here?
	ctx := context.Background()

	if _, err = c.rdsDataService.RollbackTransactionWithContext(ctx, &rdsds.RollbackTransactionInput{
		TransactionId: aws.String(c.transactionID),
		ResourceArn:   aws.String(c.resourceARN),
		SecretArn:     aws.String(c.secretARN),
	}); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	c.transactionID = ""
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

func toParams(args []driver.NamedValue) (params []*rdsds.SqlParameter, err error) {
	params = make([]*rdsds.SqlParameter, len(args))
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

	return
}

func (c *Conn) execute(ctx context.Context, query string, args []driver.NamedValue) (out *rdsds.ExecuteStatementOutput, err error) {
	params, err := toParams(args)
	if err != nil {
		return nil, err
	}

	in := &rdsds.ExecuteStatementInput{
		// ResultSetOptions @TODO allow the user to configure this
		// Schema @TODO allow the user to pass a schema this
		// ContinueAfterTimeout:  aws.Bool(false), @TODO allow this to be configurable
		IncludeResultMetadata: aws.Bool(true), //must be set to true for row iteration
		Parameters:            params,
		Database:              aws.String(c.databaseName),
		ResourceArn:           aws.String(c.resourceARN),
		SecretArn:             aws.String(c.secretARN),
		Sql:                   aws.String(query),
	}

	if c.transactionID != "" {
		in.SetTransactionId(c.transactionID)
	}

	if out, err = c.rdsDataService.ExecuteStatementWithContext(ctx, in); err != nil {
		return nil, fmt.Errorf("failed to execute statement: %w", err)
	}

	return
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), sql.TxOptions{})
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
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
	if len(r.output.GeneratedFields) != 1 {
		return -1, fmt.Errorf("LastInsertId not supported by postgres engine AND demands the exec to return exactly one generated field, got: %d", len(r.output.GeneratedFields))
	}

	f := r.output.GeneratedFields[0]
	if f.LongValue == nil {
		return -1, fmt.Errorf("generated field is not a non-nil long value")
	}

	return aws.Int64Value(f.LongValue), nil
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

type Stmt struct {
	query   string
	conn    *Conn
	closed  bool
	sets    [][]*rdsds.SqlParameter
	updates []*rdsds.UpdateResult
}

func (s *Stmt) Close() (err error) {
	if s.closed {
		return fmt.Errorf("already closed") //@TODO test
	}

	// @TODO document limitation of this
	ctx := context.Background()

	in := &rdsds.BatchExecuteStatementInput{
		Database:      aws.String(s.conn.databaseName),
		ParameterSets: s.sets,
		ResourceArn:   aws.String(s.conn.resourceARN),
		// Schema @TODO allow the user to pass a schema this
		SecretArn: aws.String(s.conn.secretARN),
		Sql:       aws.String(s.query),
	}

	if s.conn.transactionID != "" {
		in.SetTransactionId(s.conn.transactionID)
	}

	var out *rdsds.BatchExecuteStatementOutput
	if out, err = s.conn.rdsDataService.BatchExecuteStatementWithContext(ctx, in); err != nil {
		return fmt.Errorf("failed to execute batch statement: %w", err) //@TODO test
	}

	s.updates = out.UpdateResults
	s.closed = true
	return nil
}

func (s *Stmt) NumInput() int {
	return -1 // AWS Doesn't expose the query parsing so we cannot help the user here.
}

func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (_ driver.Result, err error) {
	if s.closed {
		return nil, fmt.Errorf("already closed") //@TODO test
	}

	params, err := toParams(args)
	if err != nil {
		return nil, err
	}

	s.sets = append(s.sets, params)
	return &StmtResult{stmt: s, i: len(s.sets) - 1}, nil
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (_ driver.Rows, err error) {
	if s.closed {
		return nil, fmt.Errorf("already closed") //@TODO test
	}

	return nil, fmt.Errorf("this driver cannot return any usefull results for prepared query statements.")
}

func (s *Stmt) Exec(args []driver.Value) (_ driver.Result, err error) {
	panic("not implemented, use ExecContext")
}

func (s *Stmt) Query(args []driver.Value) (_ driver.Rows, err error) {
	panic("not implemented, use QueryContext")
}

type StmtResult struct {
	stmt *Stmt
	i    int
}

func (r *StmtResult) LastInsertId() (id int64, err error) {
	gfields := r.stmt.updates[r.i].GeneratedFields
	if len(gfields) != 1 {
		return -1, fmt.Errorf("LastInsertId not supported by postgres engine AND demands the exec to return exactly one generated field, got: %d", len(gfields))
	}

	f := gfields[0]
	if f.LongValue == nil {
		return -1, fmt.Errorf("generated field is not a non-nil long value")
	}

	return aws.Int64Value(f.LongValue), nil
}

func (r *StmtResult) RowsAffected() (n int64, err error) {
	panic("not yet implemented")
}
