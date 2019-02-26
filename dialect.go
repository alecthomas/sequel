package sequel

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

var (
	lexerRegex = regexp.MustCompile("(\\?)|(\\*\\*)|(\\*)|(\"(?:\\.|[^\"])*\"|'(?:\\.|[^'])'|`(?:\\.|[^`])`|[^$*?\"']+)")

	dialects = map[string]dialect{
		"mysql":    mysqlDialect,
		"postgres": pqDialect,
		"sqlite3":  sqliteDialect,
	}

	// Dialects.
	mysqlDialect = dialect{
		name:       "mysql",
		sequential: func(int) string { return "?" },
		upsert:     mysqlUpsert,
	}
	pqDialect = dialect{
		name:       "postgres",
		sequential: func(n int) string { return fmt.Sprintf("$%d", n+1) },
		upsert:     pqUpsert,
	}
	sqliteDialect = dialect{
		name:       "sqlite3",
		sequential: func(int) string { return "?" },
		upsert:     pqUpsert,
	}
)

func mysqlUpsert(table string, keys []string, builder *builder) string {
	set := []string{}
	for _, field := range builder.fields {
		set = append(set, fmt.Sprintf("%s = VALUE(%s)", field, field))
	}
	return fmt.Sprintf(`
				INSERT INTO %s (%s) VALUES ?
				ON DUPLICATE KEY UPDATE %s
				`, table, strings.Join(builder.fields, ", "), strings.Join(set, ", "))
}

func pqUpsert(table string, keys []string, builder *builder) string {
	set := []string{}
	for _, field := range builder.fields {
		set = append(set, fmt.Sprintf("%s = excluded.%s", field, field))
	}
	return fmt.Sprintf(`
				INSERT INTO %s (%s) VALUES ?
				ON CONFLICT (%s)
				DO UPDATE SET %s
				`, table, strings.Join(builder.fields, ", "), strings.Join(keys, ", "), strings.Join(set, ", "))
}

// A dialect knows how to map Sequel placeholders to dialect-specific placeholders.
//
// 		"SELECT * FROM users WHERE id = ? OR name = ?"
type dialect struct {
	name       string
	sequential func(n int) string // Sequential placeholder.
	upsert     func(table string, keys []string, builder *builder) string
}

// Expand a query and arguments using the Sequel recursive expansion rules.
func (d *dialect) expand(builder *builder, query string, args []interface{}) (string, []interface{}, error) {
	// Fragments of text making up the final statement.
	w := &strings.Builder{}
	out := []interface{}{}
	argi := 0
	outIndex := 0
	for _, match := range lexerRegex.FindAllStringSubmatch(query, -1) {
		switch {
		case match[1] == "?":
			// Placeholder - perform parameter expansion.
			if argi >= len(args) {
				return "", nil, errors.Errorf("placeholder %d is out of range", argi)
			}
			// Newly seen argument, expand and cache it.
			arg := args[argi]
			v := reflect.ValueOf(arg)
			parameterArgs, err := d.expandParameter(true, w, &outIndex, v)
			if err != nil {
				return "", nil, err
			}
			out = append(out, parameterArgs...)
			argi++
		case match[2] == "**":
			if builder == nil {
				w.WriteString("*")
			} else {
				// Wildcard - expand all column names.
				w.WriteString(strings.Join(builder.fields, ", "))
			}
		default:
			// Text fragment, output it.
			w.WriteString(match[0])
		}

	}
	return w.String(), out, nil
}

// Expand a single parameter.
//
// Parentheses will enclose struct fields and slice elements unless "root" is true.
func (d *dialect) expandParameter(wrap bool, w *strings.Builder, index *int, v reflect.Value) (out []interface{}, err error) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.String, reflect.Float32, reflect.Float64:
		w.WriteString(d.sequential(*index))
		*index++
		return []interface{}{v.Interface()}, nil

	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				w.WriteString(", ")
			}
			children, err := d.expandParameter(wrap, w, index, v.Index(i))
			if err != nil {
				return nil, err
			}
			out = append(out, children...)
		}

	case reflect.Struct:
		if _, ok := v.Interface().(driver.Valuer); ok {
			w.WriteString(d.sequential(*index))
			*index++
			return []interface{}{v.Interface()}, nil
		}

		if wrap {
			w.WriteString("(")
		}
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			ft := t.Field(i)
			if i > 0 {
				w.WriteString(", ")
			}
			children, err := d.expandParameter(!ft.Anonymous, w, index, v.Field(i))
			if err != nil {
				return nil, err
			}
			out = append(out, children...)
		}
		if wrap {
			w.WriteString(")")
		}

	case reflect.Ptr:
		if v.IsNil() {
			w.WriteString(d.sequential(*index))
			*index++
			return []interface{}{nil}, nil
		}
		out, err = d.expandParameter(wrap, w, index, v.Elem())
		if err != nil {
			return nil, err
		}

	case reflect.Interface:
		out, err = d.expandParameter(wrap, w, index, v.Elem())
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.Errorf("unsupported parameter %s", v.Type())
	}
	return out, nil
}
