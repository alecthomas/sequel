package sequel

import (
	"reflect"
	"strings"
	"time"

	"database/sql"
	"github.com/pkg/errors"
)

// Creates a function that can efficiently construct field references for use with sql.Rows.Scan(...).
func makeRowBuilder(v interface{}) (*builder, error) {
	t := reflect.TypeOf(v)
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return nil, errors.Errorf("can only scan into pointer to struct, not %s", t)
	}
	return makeRowBuilderForType(t.Elem())
}

// Creates a function that can efficiently construct field references for use with sql.Rows.Scan(...).
func makeRowBuilderForSlice(slice interface{}) (*builder, error) {
	t := reflect.TypeOf(slice)
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Slice || t.Elem().Elem().Kind() != reflect.Struct {
		return nil, errors.Errorf("expected a pointer to a slice of structs but got %T", slice)
	}
	t = t.Elem().Elem()
	return makeRowBuilderForType(t)
}

func makeRowBuilderForType(t reflect.Type) (*builder, error) {
	if t.Kind() != reflect.Struct {
		return nil, errors.Errorf("can only build rows for structs not %s", t)
	}
	rowBuilderLock.Lock()
	defer rowBuilderLock.Unlock()
	if builder, ok := rowBuilderCache[t]; ok {
		return builder, nil
	}

	fields, err := collectFieldIndexes(t)
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

func collectFieldIndexes(t reflect.Type) ([]field, error) {
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
			sub, err := collectFieldIndexes(ft)
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
