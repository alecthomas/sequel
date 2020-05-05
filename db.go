package sequel

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

// Interface common to raw database connection and transactions.
//
// See DB or Transaction for documentation.
type Interface interface {
	Insert(table string, rows ...interface{}) ([]int64, error)
	Upsert(table string, keys []string, rows ...interface{}) (sql.Result, error)
	Expand(query string, withManaged bool, args ...interface{}) (string, []interface{}, error)
	Exec(query string, args ...interface{}) (res sql.Result, err error)
	Update(query string, args ...interface{}) (affected int64, err error)
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
	return NewFromDriver(driver, db, options...)
}

// New attempts to auto-detect the underlying SQL driver through sniffing.
func New(db *sql.DB, options ...Option) (*DB, error) {
	for _, dialect := range dialects {
		if dialect.Detect(db) {
			return NewFromDriver(dialect.Name(), db, options...)
		}
	}
	return nil, errors.New("could not detect SQL driver")
}

// NewFromDriver creates a new Sequel mapper from an existing DB connection.
func NewFromDriver(driver string, db *sql.DB, options ...Option) (*DB, error) {
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
type sqlOps interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type queryable struct {
	db      sqlOps
	dialect dialect
}

// Expand query and args using Sequel's expansion rules.
//
// The resulting query and args can be used directly with any sql.DB.
//
// If "withManaged" is true then any ** expansion will include fields managed by the database,
// eg. auto-increment or on-update columns.
//
// Returns the expanded query and args, or an error.
func (q *queryable) Expand(query string, withManaged bool, args ...interface{}) (string, []interface{}, error) {
	return expand(q.dialect, withManaged, nil, query, args)
}

// Exec an SQL statement and ignore the result.
func (q *queryable) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	query, args, err = expand(q.dialect, true, nil, query, args)
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

// Update executes an SQL statement and returns the number of rows affected.
func (q *queryable) Update(query string, args ...interface{}) (affected int64, err error) {
	result, err := q.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Insert rows.
//
// It accepts a list of rows ("Insert(table, rows)"), or a vararg sequence
// ("Insert(table, row0, row1, row2)"). Column names are reflected from the first row.
//
// Any fields marked with "managed" will not be set during insertion.
//
// Will return IDs of generated rows if applicable, or nil if not supported.
// Finally, for structs with PKs, those PKs will be updated.
func (q *queryable) Insert(table string, rows ...interface{}) ([]int64, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	// Sanity checks.
	if len(rows) == 1 {
		v := indirectValue(reflect.ValueOf(rows[0]))
		switch v.Kind() {
		case reflect.Slice:
			if v.Len() == 0 {
				return nil, nil
			}
		case reflect.Struct:
		default:
			return nil, errors.Errorf("unexpected a slice or struct but got %T", rows)
		}
	}
	return q.dialect.Insert(q.db, table, rows)
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
	arg, _, t, _ := typeForMutationRows(rows...)
	builder, err := makeRowBuilderForType(t)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to map type %s", t)
	}
	query := q.dialect.Upsert(table, keys, builder)
	query, args, err := expand(q.dialect, true, builder, query, []interface{}{arg})
	if err != nil {
		return nil, err
	}
	return q.db.Exec(query, args...)
}

func typeForMutationRows(rows ...interface{}) (arg interface{}, count int, t reflect.Type, slice reflect.Value) {
	arg = rows
	count = len(rows)
	t = reflect.TypeOf(rows[0])
	slice = reflect.ValueOf(rows)
	if len(rows) == 1 {
		if t.Kind() == reflect.Slice {
			slice = reflect.ValueOf(rows[0])
			t = t.Elem()
		}
		first := reflect.ValueOf(rows[0])
		arg = first.Interface()
		if first.Kind() == reflect.Slice {
			count = first.Len()
		}
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
	defer rows.Close()
	_, err = rows.ColumnTypes()
	if err != nil {
		return errors.Wrap(err, "failed to retrieve result column types")
	}
	out := reflect.ValueOf(slice).Elem()
	addrElem := out.Type().Elem().Kind() == reflect.Ptr
	for rows.Next() {
		el, values := builder.build(columns)
		err = rows.Scan(values...)
		if err != nil {
			return errors.Wrap(err, mapping)
		}
		if addrElem {
			el = el.Addr()
		}
		out = reflect.Append(out, el)
	}
	reflect.ValueOf(slice).Elem().Set(out)
	return rows.Err()
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
	return rows.Err()
}

func (q *queryable) prepareSelect(builder *builder, query string, args ...interface{}) (rows *sql.Rows, columns []string, mapping string, err error) {
	query, args, err = expand(q.dialect, true, builder, query, args)
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
	query, args, err = expand(q.dialect, true, nil, query, args)
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
