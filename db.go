package sequel

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var (
	scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

	// Cache of row builders.
	rowBuilderCache = map[reflect.Type]*builder{}
	rowBuilderLock  sync.Mutex
)

type builder struct {
	fields []string
	build  func(columns []string) (reflect.Value, []interface{})
}

// DB over an existing sql.DB.
type DB struct {
	DB *sql.DB
	queryable
}

// Open a database connection.
//
// The corresponding dialect will be automatically detected if possible.
func Open(driver, dsn string) (*DB, error) {
	dialect, ok := dialects[driver]
	if !ok {
		return nil, fmt.Errorf("unsupported SQL driver %q", driver)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	return &DB{DB: db, queryable: queryable{db: db, dialect: dialect}}, nil
}

// Close underlying database connection.
func (q *DB) Close() error {
	return q.DB.Close()
}

// Begin a new transaction.
func (q *DB) Begin() (*Transaction, error) {
	tx, err := q.DB.Begin()
	if err != nil {
		return nil, err
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
func (t *Transaction) CommitOrRollbackOnError(err *error) error {
	if *err == nil {
		return t.Tx.Commit()
	}
	return t.Tx.Rollback()
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
	return q.dialect.expand(query, args)
}

// Exec an SQL statement and ignore the result.
func (q *queryable) Exec(query string, args ...interface{}) (err error) {
	query, args, err = q.dialect.expand(query, args)
	if err != nil {
		return err
	}
	// TODO: Can we parse column names out of the statement, and reflect the same out of args, to be more type safe?
	_, err = q.db.Exec(query, args...)
	return err
}

// Select issues a query, and accumulates the returned rows into slice.
//
// The shape and names of the query must match the shape and field names of the slice elements.
func (q *queryable) Select(slice interface{}, query string, args ...interface{}) (err error) {
	builder, err := q.makeRowBuilder(slice)
	if err != nil {
		return err
	}
	query, args, err = q.dialect.expand(query, args)
	if err != nil {
		return err
	}
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return fmt.Errorf("%s; %q (mapping to fields %s)", err, query, strings.Join(builder.fields, ","))
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	fieldMap := map[string]bool{}
	for _, field := range builder.fields {
		fieldMap[field] = true
	}
	for _, column := range columns {
		if !fieldMap[column] {
			return fmt.Errorf("no field (from %s) maps to result column %q", strings.Join(builder.fields, ","), column)
		}
	}
	mapping := fmt.Sprintf("(%s) -> (%s)", strings.Join(columns, ","), strings.Join(builder.fields, ","))
	if len(columns) != len(builder.fields) {
		return fmt.Errorf("invalid mapping %s", mapping)
	}
	out := reflect.ValueOf(slice).Elem()
	for rows.Next() {
		el, values := builder.build(columns)
		err = rows.Scan(values...)
		if err != nil {
			return fmt.Errorf("%s; %s", mapping, err)
		}
		out = reflect.Append(out, el)
	}
	reflect.ValueOf(slice).Elem().Set(out)
	return nil
}

// SelectScalar selects a single column row into value.
func (q *queryable) SelectScalar(value interface{}, query string, args ...interface{}) (err error) {
	query, args, err = q.dialect.expand(query, args)
	if err != nil {
		return err
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

// Creates a function that can efficiently construct field references for use with sql.Rows.Scan(...).
func (q *queryable) makeRowBuilder(slice interface{}) (*builder, error) {
	t := reflect.TypeOf(slice)
	rowBuilderLock.Lock()
	defer rowBuilderLock.Unlock()
	if builder, ok := rowBuilderCache[t]; ok {
		return builder, nil
	}

	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Slice || t.Elem().Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected a pointer to a slice of structs but got %T", slice)
	}
	t = t.Elem().Elem()
	fields, err := q.collectFieldIndexes(t)
	if err != nil {
		return nil, err
	}
	fieldMap := map[string]field{}
	fieldNames := []string{}
	for _, field := range fields {
		fieldNames = append(fieldNames, field.name)
		fieldMap[field.name] = field
	}
	return &builder{
		fields: fieldNames,
		build: func(columns []string) (reflect.Value, []interface{}) {
			out := make([]interface{}, len(fields))
			v := reflect.New(t).Elem()
			for i, column := range columns {
				out[i] = v.FieldByIndex(fieldMap[column].index).Addr().Interface()
			}
			return v, out
		},
	}, nil
}

type field struct {
	name  string
	index []int
}

func (q *queryable) collectFieldIndexes(t reflect.Type) ([]field, error) {
	out := []field{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		ft := f.Type

		if ft.Implements(scannerType) {
			out = append(out, field{
				name:  fieldName(f),
				index: []int{i},
			})
			continue
		}

		switch ft.Kind() {
		case reflect.Struct:
			sub, err := q.collectFieldIndexes(ft)
			if err != nil {
				return nil, err
			}
			for _, field := range sub {
				field.index = append([]int{i}, field.index...)
				out = append(out, field)
			}

		case reflect.Slice, reflect.Array:
			return nil, fmt.Errorf("can't select into a slice")

		default:
			out = append(out, field{
				name:  fieldName(f),
				index: []int{i},
			})
		}
	}

	return out, nil
}

func fieldName(f reflect.StructField) string {
	if tag, ok := f.Tag.Lookup("db"); ok {
		return tag
	}
	return strings.ToLower(f.Name)
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, s := range a {
		if b[i] != s {
			return false
		}
	}
	return true
}

func indirect(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		return indirect(v.Elem())
	}
	return v
}
