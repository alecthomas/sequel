package sequel

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var (
	scannerType   = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	timeType      = reflect.TypeOf(time.Time{})
	byteSliceType = reflect.TypeOf([]byte{})

	// Cache of row builders.
	rowBuilderCache = map[reflect.Type]*builder{}
	rowBuilderLock  sync.Mutex
)

// Interface common to raw database connection and transactions.
//
// See DB or Transaction for documentation.
type Interface interface {
	Upsert(table string, keys []string, rows ...interface{}) (sql.Result, error)
	Expand(query string, args ...interface{}) (string, []interface{}, error)
	Exec(query string, args ...interface{}) (res sql.Result, err error)
	Select(slice interface{}, query string, args ...interface{}) (err error)
	SelectOne(ref interface{}, query string, args ...interface{}) error
	SelectScalar(value interface{}, query string, args ...interface{}) (err error)
	SelectInt(query string, args ...interface{}) (value int, err error)
	SelectString(query string, args ...interface{}) (value string, err error)
}

// Option for modifying the behaviour of Sequel.
type Option func(db *DB)

// DB over an existing sql.DB.
type DB struct {
	DB *sql.DB
	queryable
}

var _ Interface = &DB{}

// Open a database connection.
func Open(driver, dsn string, options ...Option) (*DB, error) {
	_, ok := dialects[driver]
	if !ok {
		return nil, errors.Errorf("unsupported SQL driver %q", driver)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open SQL connection")
	}
	return New(driver, db, options...)
}

// New creates a new Sequel mapper from an existing DB connection.
func New(driver string, db *sql.DB, options ...Option) (*DB, error) {
	dialect, ok := dialects[driver]
	if !ok {
		return nil, errors.Errorf("unsupported SQL driver %q", driver)
	}
	sqldb := &DB{
		DB: db,
		queryable: queryable{
			db:      db,
			dialect: dialect,
		},
	}
	for _, opt := range options {
		opt(sqldb)
	}
	return sqldb, nil
}

// Close underlying database connection.
func (q *DB) Close() error {
	return q.DB.Close()
}

// Begin a new transaction.
func (q *DB) Begin() (*Transaction, error) {
	tx, err := q.DB.Begin()
	if err != nil {
		return nil, errors.Wrap(err, "failed to open transaction")
	}
	return &Transaction{
		Tx:        tx,
		queryable: queryable{db: tx, dialect: q.dialect},
	}, nil
}

// A Transaction wraps an underlying sql.Tx.
type Transaction struct {
	Tx *sql.Tx
	queryable
}

var _ Interface = &Transaction{}

// Commit transaction.
func (t *Transaction) Commit() error {
	return t.Tx.Commit()
}

// Rollback transaction.
func (t *Transaction) Rollback() error {
	return t.Tx.Rollback()
}

// CommitOrRollbackOnError is a convenience method that can be used on a named error return value to rollback if an
// error occurs or commit if no error occurs.
//
// eg.
//
// 		func myFunc(mapper *DB) (err error) {
// 			tx, err := mapper.Begin()
// 			if err != nil {
// 				return err
// 			}
// 			defer tx.CommitOrRollbackOnError(&err)
//
// 			// Do a whole bunch of work.
//
// 			return nil
// 		}
func (t *Transaction) CommitOrRollbackOnError(err *error) {
	if *err == nil {
		*err = t.Tx.Commit()
	} else if rberr := t.Tx.Rollback(); rberr != nil {
		*err = rberr
	}
}

// Operations common between sql.DB and sql.Tx.
type commonOps interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type queryable struct {
	db      commonOps
	dialect dialect
}

// Expand query and args using Sequel's expansion rules.
//
// The resulting query and args can be used directly with any sql.DB.
func (q *queryable) Expand(query string, args ...interface{}) (string, []interface{}, error) {
	builder, err := makeRowBuilderForSliceOfInterface(args)
	if err != nil {
		return "", nil, err
	}
	return q.dialect.expand(builder, query, args)
}

// Exec an SQL statement and ignore the result.
func (q *queryable) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	builder, err := makeRowBuilderForSliceOfInterface(args)
	if err != nil {
		return nil, err
	}
	query, args, err = q.dialect.expand(builder, query, args)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to expand query %q", query)
	}
	// TODO: Can we parse column names out of the statement, and reflect the same out of args, to be more type safe?
	result, err := q.db.Exec(query, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute %q", query)
	}
	return result, nil
}

// Upsert rows.
//
// Existing rows will be updated and new rows will be inserted.
//
// "keys" must be the list of column names that will trigger a unique constraint violation if an UPDATE is to occur.
func (q *queryable) Upsert(table string, keys []string, rows ...interface{}) (sql.Result, error) {
	if len(rows) == 0 {
		return nil, errors.Errorf("no rows to update")
	}
	arg, t := q.typeForMutationRows(rows...)
	builder, err := makeRowBuilderForType(t)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to map type %s", t)
	}
	sql := q.dialect.upsert(table, keys, builder)
	return q.Exec(sql, arg)
}

func (q *queryable) typeForMutationRows(rows ...interface{}) (arg interface{}, t reflect.Type) {
	arg = rows
	t = reflect.TypeOf(rows[0])
	if len(rows) == 1 {
		if t.Kind() == reflect.Slice {
			t = t.Elem()
		}
		arg = reflect.ValueOf(rows[0]).Interface()
	}
	return
}

// Select issues a query, and accumulates the returned rows into slice.
//
// The shape and names of the query must match the shape and field names of the slice elements.
func (q *queryable) Select(slice interface{}, query string, args ...interface{}) (err error) {
	builder, err := makeRowBuilderForSlice(slice)
	if err != nil {
		return errors.Wrapf(err, "failed to map slice %T", slice)
	}
	rows, columns, mapping, err := q.prepareSelect(builder, query, args...)
	if err != nil {
		return errors.Wrapf(err, "failed to prepare select %q", query)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return errors.Wrap(err, "failed to retrieve result column types")
	}
	out := reflect.ValueOf(slice).Elem()
	for rows.Next() {
		el, values := builder.build(columns, types)
		err = rows.Scan(values...)
		if err != nil {
			return errors.Wrap(err, mapping)
		}
		out = reflect.Append(out, el)
	}
	reflect.ValueOf(slice).Elem().Set(out)
	return nil
}

// SelectOne issues a query and selects a single row into ref.
//
// Will return sql.ErrNoRows if no rows are returned.
func (q *queryable) SelectOne(ref interface{}, query string, args ...interface{}) error {
	builder, err := makeRowBuilder(ref)
	if err != nil {
		return errors.Wrapf(err, "failed to map type %T", ref)
	}
	rows, columns, mapping, err := q.prepareSelect(builder, query, args...)
	if err != nil {
		return errors.Wrapf(err, "failed to prepare select %q", query)
	}
	defer rows.Close()
	if !rows.Next() {
		return sql.ErrNoRows
	}
	values := builder.fill(ref, columns)
	err = rows.Scan(values...)
	if err != nil {
		return errors.Wrap(err, mapping)
	}
	if rows.Next() {
		return errors.Errorf("more than one row returned from %q", query)
	}
	return nil
}

func (q *queryable) prepareSelect(builder *builder, query string, args ...interface{}) (rows *sql.Rows, columns []string, mapping string, err error) {
	query, args, err = q.dialect.expand(builder, query, args)
	if err != nil {
		return nil, nil, "", errors.Wrapf(err, "failed to expand query %q", query)
	}
	rows, err = q.db.Query(query, args...)
	if err != nil {
		return nil, nil, "", errors.Wrapf(err, "%q (mapping to fields %s)", query, strings.Join(builder.fields, ", "))
	}
	columns, err = rows.Columns()
	if err != nil {
		_ = rows.Close()
		return nil, nil, "", errors.Wrap(err, "failed to retrieve columns")
	}
	mapping = fmt.Sprintf("(%s) -> (%s)", strings.Join(columns, ","), strings.Join(builder.fields, ","))

	// Strict checks.
	fieldMap := map[string]bool{}
	for _, field := range builder.fields {
		fieldMap[field] = true
	}
	for _, column := range columns {
		if !fieldMap[column] {
			_ = rows.Close()
			return nil, nil, "", errors.Errorf("no field in (%s) maps to result column %q", strings.Join(builder.fields, ", "), column)
		}
	}
	if len(columns) != len(builder.fields) {
		_ = rows.Close()
		return nil, nil, "", errors.Errorf("invalid mapping %s", mapping)
	}
	return rows, columns, mapping, nil
}

// SelectScalar selects a single column row into value.
func (q *queryable) SelectScalar(value interface{}, query string, args ...interface{}) (err error) {
	query, args, err = q.dialect.expand(nil, query, args)
	if err != nil {
		return errors.Wrapf(err, "failed to expand query %q", query)
	}
	row := q.db.QueryRow(query, args...)
	return row.Scan(value)
}

// SelectInt selects a single column row into an integer and returns it.
func (q *queryable) SelectInt(query string, args ...interface{}) (value int, err error) {
	return value, q.SelectScalar(&value, query, args)
}

// SelectString selects a single column row into a string and returns it.
func (q *queryable) SelectString(query string, args ...interface{}) (value string, err error) {
	return value, q.SelectScalar(&value, query, args)
}
