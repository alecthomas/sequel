package sequel

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

var (
	scannerType   = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	timeType      = reflect.TypeOf(time.Time{})
	byteSliceType = reflect.TypeOf([]byte{})

	// Cache of row builders.
	rowBuilderCache = map[reflect.Type]*builder{}
	rowBuilderLock  sync.Mutex
)

type builder struct {
	fields []string
	build  func(columns []string) (reflect.Value, []interface{})
	fill   func(v interface{}, columns []string) []interface{}
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

// New creates a new Sequel mapper from an existing DB connection.
func New(driver string, db *sql.DB) (*DB, error) {
	dialect, ok := dialects[driver]
	if !ok {
		return nil, fmt.Errorf("unsupported SQL driver %q", driver)
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
func (t *Transaction) CommitOrRollbackOnError(err *error) {
	if *err == nil {
		*err = t.Tx.Commit()
	}
	if rberr := t.Tx.Rollback(); rberr != nil {
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
	return q.dialect.expand(query, args)
}

// Exec an SQL statement and ignore the result.
func (q *queryable) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	query, args, err = q.dialect.expand(query, args)
	if err != nil {
		return nil, err
	}
	// TODO: Can we parse column names out of the statement, and reflect the same out of args, to be more type safe?
	return q.db.Exec(query, args...)
}

// Select issues a query, and accumulates the returned rows into slice.
//
// The shape and names of the query must match the shape and field names of the slice elements.
func (q *queryable) Select(slice interface{}, query string, args ...interface{}) (err error) {
	builder, err := q.makeRowBuilderForSlice(slice)
	if err != nil {
		return err
	}
	rows, columns, mapping, err := q.prepareSelect(builder, query, args...)
	if err != nil {
		return err
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

// SelectOne issues a query and selects a single row into ref.
//
// Will return sql.ErrNoRows if no rows are returned.
func (q *queryable) SelectOne(ref interface{}, query string, args ...interface{}) error {
	builder, err := q.makeRowBuilder(ref)
	if err != nil {
		return err
	}
	rows, columns, mapping, err := q.prepareSelect(builder, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		return sql.ErrNoRows
	}
	values := builder.fill(ref, columns)
	err = rows.Scan(values...)
	if err != nil {
		return fmt.Errorf("%s; %s", mapping, err)
	}
	if rows.Next() {
		return fmt.Errorf("more than one row returned")
	}
	return nil
}

func (q *queryable) prepareSelect(builder *builder, query string, args ...interface{}) (rows *sql.Rows, columns []string, mapping string, err error) {
	query, args, err = q.dialect.expand(query, args)
	if err != nil {
		return nil, nil, "", err
	}
	rows, err = q.db.Query(query, args...)
	if err != nil {
		return nil, nil, "", fmt.Errorf("%s; %q (mapping to fields %s)", err, query, strings.Join(builder.fields, ", "))
	}
	columns, err = rows.Columns()
	if err != nil {
		_ = rows.Close()
		return nil, nil, "", err
	}
	fieldMap := map[string]bool{}
	for _, field := range builder.fields {
		fieldMap[field] = true
	}
	for _, column := range columns {
		if !fieldMap[column] {
			_ = rows.Close()
			return nil, nil, "", fmt.Errorf("no field in (%s) maps to result column %q", strings.Join(builder.fields, ", "), column)
		}
	}
	mapping = fmt.Sprintf("(%s) -> (%s)", strings.Join(columns, ","), strings.Join(builder.fields, ","))
	if len(columns) != len(builder.fields) {
		_ = rows.Close()
		return nil, nil, "", fmt.Errorf("invalid mapping %s", mapping)
	}
	return rows, columns, mapping, nil
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
func (q *queryable) makeRowBuilder(v interface{}) (*builder, error) {
	t := reflect.TypeOf(v)
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("can only scan into pointer to struct, not %s", t)
	}
	return q.makeRowBuilderForType(t.Elem())
}

// Creates a function that can efficiently construct field references for use with sql.Rows.Scan(...).
func (q *queryable) makeRowBuilderForSlice(slice interface{}) (*builder, error) {
	t := reflect.TypeOf(slice)
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Slice || t.Elem().Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected a pointer to a slice of structs but got %T", slice)
	}
	t = t.Elem().Elem()
	return q.makeRowBuilderForType(t)
}

func (q *queryable) makeRowBuilderForType(t reflect.Type) (*builder, error) {
	rowBuilderLock.Lock()
	defer rowBuilderLock.Unlock()
	if builder, ok := rowBuilderCache[t]; ok {
		return builder, nil
	}

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
		fill: func(v interface{}, columns []string) (out []interface{}) {
			rv := reflect.ValueOf(v).Elem()
			out = make([]interface{}, len(fields))
			for i, column := range columns {
				out[i] = rv.FieldByIndex(fieldMap[column].index).Addr().Interface()
			}
			return
		},
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

		if ft.Implements(scannerType) || ft == timeType || ft == byteSliceType {
			out = append(out, field{
				name:  fieldName(f),
				index: []int{i},
			})
			continue
		}

		switch ft.Kind() {
		case reflect.Struct:
			if !f.Anonymous {
				return nil, fmt.Errorf("struct field \"%s %s\" must implement sql.Scanner to be mapped to a field", f.Name, ft)
			}
			sub, err := q.collectFieldIndexes(ft)
			if err != nil {
				return nil, err
			}
			for _, field := range sub {
				field.index = append([]int{i}, field.index...)
				out = append(out, field)
			}

		case reflect.Slice, reflect.Array:
			return nil, fmt.Errorf("can't select into slice field \"%s %s\"", f.Name, ft)

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
