# rds-data-api
SQL Driver for the AWS RDS Data API 

## Limitations
- The driver cannot sanity check the nr of parameters in a query
- The driver doesn't support ordinal query arguments (named only)
- No streaming support
- IncludeResultMetadata is always set to true with 1MB of data limit
- result.LastInsertID() not supported for aurora postgres, instead use https://www.postgresql.org/docs/10/dml-returning.html
  this is a limitation from AWS: https://godoc.org/github.com/aws/aws-sdk-go/service/rdsdataservice#ExecuteStatementOutput
- result.RowAffected returns 0 on non empty table, implementation error?
- Prepared statements are not supported (maybe expose batchExecute?)

## TODO
- [x] Get basic db.Exec and db.Query working
- [x] add a cloudformation for setting up a testig mysql database
- [x] test mysql last inserted id function
- [x] implement Tx, Commit and Rollback 
- [ ] figure out how to perform prepared statements
- [ ] remove repetition in tests
- [ ] use https://godoc.org/database/sql/driver#ResultNoRows correctly
- [ ] Can we pass this test: `https://github.com/bradfitz/go-sql-test`
- [ ] Can we include the AWS library in such a way that we don't have to publish new versions for every new release
- [ ] The package and module name are a bit verbose, maybe shortend to `rdsda`?
- [ ] Write Function docs
- [ ] Remove fmt dependency, rather use custom errors
- [ ] Double check if our atomic close check works as expected
- [ ] Write error path test cases
- [ ] implement the driver.ColumnType methods on "Rows"