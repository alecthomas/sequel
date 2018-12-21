package sequel

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

var (
	lexerRegex = regexp.MustCompile(`(\?)|("(?:\\.|[^"])*"|'(?:\\.|[^'])'|[^$?"']+)`)

	dialects = map[string]dialect{
		"mysql":    mysqlDialect,
		"postgres": pqDialect,
		"sqlite3":  sqliteDialect,
	}

	// Dialects.
	mysqlDialect = dialect{
		name:       "mysql",
		sequential: func(int) string { return "?" },
	}
	pqDialect = dialect{
		name:       "postgres",
		sequential: func(n int) string { return fmt.Sprintf("$%d", n+1) },
	}
	sqliteDialect = dialect{
		name:       "sqlite3",
		sequential: func(int) string { return "?" },
	}
)

// A dialect knows how to map Sequel placeholders to dialect-specific placeholders.
//
// 		"SELECT * FROM users WHERE id = ? OR name = ?"
type dialect struct {
	name       string
	sequential func(n int) string // Sequential placeholder.
}

// Expand a query and arguments using the Sequel recursive expansion rules.
func (d *dialect) expand(query string, args []interface{}) (string, []interface{}, error) {
	// Fragments of text making up the final statement.
	w := &strings.Builder{}
	out := []interface{}{}
	argi := 0
	outIndex := 0
	for _, match := range lexerRegex.FindAllStringSubmatch(query, -1) {
		// Text fragment, output it.
		if match[1] == "" {
			w.WriteString(match[2])
			continue
		}

		// Placeholder - perform parameter expansion.
		if match[1] == "?" {
			if argi >= len(args) {
				return "", nil, fmt.Errorf("placeholder %d is out of range", argi)
			}
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
		return nil, fmt.Errorf("unsupported parameter %s", v.Type())
	}
	return out, nil
}
