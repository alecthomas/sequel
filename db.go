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
	Insert(table string, rows ...interface{}) (sql.Result, error)
	Upsert(table string, rows ...interface{}) (sql.Result, error)
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

// Unsafe disables strict checking of column mappings.
func Unsafe() Option {
	return func(db *DB) {
		db.strict = false
	}
}

// DB over an existing sql.DB.
type DB struct {
	DB *sql.DB
	queryable
}

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
			strict:  true,
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
	strict  bool
}

// Insert rows.
//
// If one value is provided and it itself is a slice, each element in the slice will be a
// row to insert. Otherwise each argument will be a row to insert.
//
// This is a convenience function for automatically generating the appropriate column
// names.
func (q *queryable) Insert(table string, rows ...interface{}) (sql.Result, error) {
	if len(rows) == 0 {
		return nil, errors.Errorf("no rows to insert")
	}
	arg, t := q.typeForMutationRows(rows...)
	builder, err := q.makeRowBuilderForType(t)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to map type %s", t)
	}
	sql := fmt.Sprintf(`INSERT INTO %s (%s) VALUES ?`, table, strings.Join(builder.fields, ", "))
	return q.Exec(sql, arg)
}

// Upsert rows.
//
// The given struct must have a field tagged as a primary key.
//
// Existing rows will be updated and new rows will be inserted.
func (q *queryable) Upsert(table string, rows ...interface{}) (sql.Result, error) {
	if len(rows) == 0 {
		return nil, errors.Errorf("no rows to update")
	}
	arg, t := q.typeForMutationRows(rows...)
	builder, err := q.makeRowBuilderForType(t)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to map type %s", t)
	}
	if builder.pk == "" {
		return nil, errors.Errorf("cannot update table %q from struct %T without a PK field tagged `db:\"<name>,pk\"`", table, rows[0])
	}
	sql := q.dialect.upsert(table, builder)
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
		return nil, errors.Wrapf(err, "failed to expand query %q", query)
	}
	// TODO: Can we parse column names out of the statement, and reflect the same out of args, to be more type safe?
	result, err := q.db.Exec(query, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute %q", query)
	}
	return result, nil
}

// Select issues a query, and accumulates the returned rows into slice.
//
// The shape and names of the query must match the shape and field names of the slice elements.
func (q *queryable) Select(slice interface{}, query string, args ...interface{}) (err error) {
	builder, err := q.makeRowBuilderForSlice(slice)
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
	builder, err := q.makeRowBuilder(ref)
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
	query, args, err = q.dialect.expand(query, args)
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
	if !q.strict {
		return rows, columns, mapping, nil
	}

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
	query, args, err = q.dialect.expand(query, args)
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

// Creates a function that can efficiently construct field references for use with sql.Rows.Scan(...).
func (q *queryable) makeRowBuilder(v interface{}) (*builder, error) {
	t := reflect.TypeOf(v)
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return nil, errors.Errorf("can only scan into pointer to struct, not %s", t)
	}
	return q.makeRowBuilderForType(t.Elem())
}

// Creates a function that can efficiently construct field references for use with sql.Rows.Scan(...).
func (q *queryable) makeRowBuilderForSlice(slice interface{}) (*builder, error) {
	t := reflect.TypeOf(slice)
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Slice || t.Elem().Elem().Kind() != reflect.Struct {
		return nil, errors.Errorf("expected a pointer to a slice of structs but got %T", slice)
	}
	t = t.Elem().Elem()
	return q.makeRowBuilderForType(t)
}

func (q *queryable) makeRowBuilderForType(t reflect.Type) (*builder, error) {
	if t.Kind() != reflect.Struct {
		return nil, errors.Errorf("can only build rows for structs not %s", t)
	}
	rowBuilderLock.Lock()
	defer rowBuilderLock.Unlock()
	if builder, ok := rowBuilderCache[t]; ok {
		return builder, nil
	}

	fields, err := q.collectFieldIndexes(t)
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect field indexes")
	}
	fieldMap := map[string]field{}
	fieldNames := []string{}
	pk := ""
	for _, field := range fields {
		if field.pk {
			pk = field.name
		}
		fieldNames = append(fieldNames, field.name)
		fieldMap[field.name] = field
	}
	return &builder{pk: pk, t: t, fields: fieldNames, fieldMap: fieldMap}, nil
}

func (q *queryable) collectFieldIndexes(t reflect.Type) ([]field, error) {
	out := []field{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		ft := f.Type

		if ft == timeType || ft == byteSliceType || ft.Implements(scannerType) || reflect.PtrTo(ft).Implements(scannerType) {
			name, pk := parseField(f)
			out = append(out, field{
				name:  name,
				pk:    pk,
				index: []int{i},
			})
			continue
		}

		switch ft.Kind() {
		case reflect.Struct:
			if !f.Anonymous {
				return nil, errors.Errorf("struct field \"%s %s\" must implement sql.Scanner to be mapped to a field", f.Name, ft)
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
			return nil, errors.Errorf("can't select into slice field \"%s %s\"", f.Name, ft)

		default:
			name, pk := parseField(f)
			out = append(out, field{
				name:  name,
				pk:    pk,
				index: []int{i},
			})
		}
	}

	return out, nil
}

func parseField(f reflect.StructField) (name string, pk bool) {
	name = strings.ToLower(f.Name)
	tag, ok := f.Tag.Lookup("db")
	if !ok {
		return
	}
	parts := strings.Split(tag, ",")
	if parts[0] != "" {
		name = parts[0]
	}
	for _, part := range parts[1:] {
		if part == "pk" {
			pk = true
		}
	}
	return
}

type field struct {
	name  string
	pk    bool
	index []int
}

type builder struct {
	pk       string
	t        reflect.Type
	fields   []string
	fieldMap map[string]field
}

func (b *builder) fill(v interface{}, columns []string) (out []interface{}) {
	rv := reflect.ValueOf(v).Elem()
	out = make([]interface{}, len(b.fields))
	for i, column := range columns {
		out[i] = rv.FieldByIndex(b.fieldMap[column].index).Addr().Interface()
	}
	return
}

func (b *builder) build(columns []string, types []*sql.ColumnType) (reflect.Value, []interface{}) {
	out := make([]interface{}, len(columns))
	v := reflect.New(b.t).Elem()
	for i, column := range columns {
		field, ok := b.fieldMap[column]
		if ok {
			out[i] = v.FieldByIndex(field.index).Addr().Interface()
			continue
		}

		// Should only hit this in unsafe mode.
		switch types[i].DatabaseTypeName() {
		case "VARCHAR", "TEXT", "NVARCHAR", "STRING", "CHARACTER", "VARYING CHARACTER", "NCHAR", "NATIVE CHARACTER", "CLOB":
			out[i] = new(string)
		case "NUMERIC", "DECIMAL", "INT", "BIGINT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "UNSIGNED BIG INT", "INT2", "INT8":
			out[i] = new(int64)
		case "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION":
			out[i] = new(float64)
		case "BOOL":
			out[i] = new(bool)
		case "DATE", "DATETIME":
			out[i] = &time.Time{}
		case "BYTE":
			b := []byte{}
			out[i] = &b
		default:
			panic("unsupported missing field type " + types[i].DatabaseTypeName())
		}
	}
	return v, out
}
