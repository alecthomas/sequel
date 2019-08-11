package sequel

import (
	"database/sql"
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
	rowBuilderLock  sync.RWMutex
)

// Creates a function that can efficiently construct field references for use with sql.Rows.Scan(...).
func makeRowBuilder(v interface{}, withManaged bool) (*builder, error) {
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

func makeRowBuilderForSliceOfInterface(slice []interface{}) (*builder, error) {
	if len(slice) == 0 {
		return nil, nil
	}
	v := reflect.ValueOf(slice[0])
	if v.Kind() == reflect.Slice {
		return makeRowBuilderForType(v.Index(0).Type())
	} else if v.Kind() == reflect.Struct {
		return makeRowBuilderForType(v.Type())
	}
	return nil, nil
}

func indirectType(t reflect.Type) reflect.Type {
	switch t.Kind() {
	case reflect.Ptr, reflect.Interface:
		return indirectType(t.Elem())
	}
	return t
}

func indirectValue(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		return indirectValue(v.Elem())
	}
	return v
}

func makeRowBuilderForType(t reflect.Type) (*builder, error) {
	t = indirectType(t)
	if t.Kind() != reflect.Struct {
		return nil, errors.Errorf("can only build rows for structs not %s", t)
	}
	rowBuilderLock.RLock()
	if builder, ok := rowBuilderCache[t]; ok {
		rowBuilderLock.RUnlock()
		return builder, nil
	}
	rowBuilderLock.RUnlock()

	// Upgrade and check it again :\
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
	b := &builder{
		t:        t,
		fields:   fieldNames,
		fieldMap: fieldMap,
		pk:       pk,
	}
	rowBuilderCache[t] = b
	return b, nil
}

func collectFieldIndexes(t reflect.Type) ([]field, error) {
	out := []field{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		ft := f.Type

		if ft == timeType || ft == byteSliceType || ft.Implements(scannerType) || reflect.PtrTo(ft).Implements(scannerType) {
			fld, err := parseField(f, []int{i})
			if err != nil {
				return nil, err
			}
			out = append(out, fld)
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
			fld, err := parseField(f, []int{i})
			if err != nil {
				return nil, err
			}
			out = append(out, fld)
		}
	}

	return out, nil
}

func parseField(f reflect.StructField, index []int) (field, error) {
	name := strings.ToLower(strings.Join(camelCase(f.Name), "_"))
	tag, ok := f.Tag.Lookup("db")
	out := field{name: name, index: index}
	if !ok {
		return out, nil
	}

	parts := strings.Split(tag, ",")
	if parts[0] != "" {
		out.name = parts[0]
	}
	for _, part := range parts[1:] {
		switch part {
		case "managed":
			out.managed = true
		case "pk":
			out.pk = true
		default:
			return field{}, errors.Errorf("field %s: invalid tag attribute %q", f.Name, part)
		}
	}
	return out, nil
}

type field struct {
	name    string
	index   []int
	managed bool
	pk      bool
}

type builder struct {
	pk       string
	t        reflect.Type
	fields   []string
	fieldMap map[string]field
}

func (b *builder) filteredFields(withManaged bool) []string {
	out := make([]string, 0, len(b.fields))
	for _, field := range b.fields {
		if withManaged || !b.fieldMap[field].managed {
			out = append(out, field)
		}
	}
	return out
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
		if !ok {
			panic("unmapped field" + column)
		}
		out[i] = v.FieldByIndex(field.index).Addr().Interface()
	}
	return v, out
}
