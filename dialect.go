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
	lexerRegex = regexp.MustCompile(
		"(\\?)|" +
			"(\\*\\*)|" +
			"(\\*)|" +
			"(\"(?:\\.|[^\"])*\")|" +
			"('(?:\\.|[^'])*')|" +
			"(`(?:\\.|[^`])*`)|" +
			"([^$*?\"']+)")

	dialects = map[string]dialect{
		"mysql":    mysqlDialect,
		"postgres": pqDialect,
		"sqlite3":  sqliteDialect,
	}

	// Dialects.
	mysqlDialect = dialect{
		name:       "mysql",
		quoteID:    quoteBacktick,
		sequential: func(int) string { return "?" },
		upsert:     mysqlUpsert,
	}
	pqDialect = dialect{
		name:       "postgres",
		quoteID:    quoteBacktick,
		sequential: func(n int) string { return fmt.Sprintf("$%d", n+1) },
		upsert:     pqUpsert,
	}
	sqliteDialect = dialect{
		name:       "sqlite3",
		quoteID:    quoteBacktick,
		sequential: func(int) string { return "?" },
		upsert:     pqUpsert,
	}
)

func quoteBacktick(s string) string {
	s = strings.ReplaceAll(s, "`", "``")
	return "`" + s + "`"
}

func mysqlUpsert(table string, keys []string, builder *builder) string {
	set := []string{}
	for _, field := range builder.filteredFields(true) {
		set = append(set, fmt.Sprintf("%s = VALUE(%s)",
			quoteBacktick(field), quoteBacktick(field)))
	}
	return fmt.Sprintf(`
				INSERT INTO %s (%s) VALUES ?
				ON DUPLICATE KEY UPDATE %s
				`,
		quoteBacktick(table),
		quoteAndJoinIDs(quoteBacktick, builder.filteredFields(true)),
		quoteAndJoinIDs(quoteBacktick, set))
}

func pqUpsert(table string, keys []string, builder *builder) string {
	set := []string{}
	for _, field := range builder.filteredFields(true) {
		set = append(set, fmt.Sprintf("%s = excluded.%s",
			quoteBacktick(field), quoteBacktick(field)))
	}
	return fmt.Sprintf(`
			INSERT INTO %s (%s) VALUES ?
			ON CONFLICT (%s)
			DO UPDATE SET %s
			`,
		quoteBacktick(table),
		quoteAndJoinIDs(quoteBacktick, builder.filteredFields(true)),
		quoteAndJoinIDs(quoteBacktick, keys), strings.Join(set, ", "))
}

func quoteAndJoinIDs(quoteID func(s string) string, ids []string) string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = quoteID(id)
	}
	return strings.Join(out, ", ")
}

// A dialect knows how to map Sequel placeholders to dialect-specific placeholders.
//
// 		"SELECT * FROM users WHERE id = ? OR name = ?"
type dialect struct {
	name       string
	quoteID    func(string) string
	sequential func(n int) string // Sequential placeholder.
	upsert     func(table string, keys []string, builder *builder) string
}

// Expand a query and arguments using the Sequel recursive expansion rules.
func (d *dialect) expand(withManaged bool, builder *builder, query string, args []interface{}) (string, []interface{}, error) {
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
			parameterArgs, err := d.expandParameter(withManaged, true, w, &outIndex, v)
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
				w.WriteString(quoteAndJoinIDs(d.quoteID, builder.fields))
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
func (d *dialect) expandParameter(withManaged, wrap bool, w *strings.Builder, index *int, v reflect.Value) (out []interface{}, err error) {
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
			children, err := d.expandParameter(withManaged, wrap, w, index, v.Index(i))
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
		builder, err := makeRowBuilderForType(t)
		if err != nil {
			return nil, err
		}
		for i, name := range builder.filteredFields(withManaged) {
			if i > 0 {
				w.WriteString(", ")
			}
			field := builder.fieldMap[name]
			fv := v.FieldByIndex(field.index)
			children, err := d.expandParameter(withManaged, false, w, index, fv)
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
		out, err = d.expandParameter(withManaged, wrap, w, index, v.Elem())
		if err != nil {
			return nil, err
		}

	case reflect.Interface:
		out, err = d.expandParameter(withManaged, wrap, w, index, v.Elem())
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.Errorf("unsupported parameter %s", v.Type())
	}
	return out, nil
}
