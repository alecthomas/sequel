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
		"mysql":    &mysqlDialect{},
		"postgres": &pqDialect{ansiDialect{name: "postgres"}},
		"sqlite3":  &ansiDialect{name: "sqlite3"},
	}
)

// A dialect knows how to perform SQL dialect specific operations.
//
// eg. ? expansion
//
// 		"SELECT * FROM users WHERE id = ? OR name = ?"
type dialect interface {
	Name() string
	QuoteID(s string) string
	Placeholder(n int) string
	Upsert(table string, keys []string, builder *builder) string
	Insert(ops sqlOps, table string, builder *builder) ([]int64, error)
}

type mysqlDialect struct{}

func (m *mysqlDialect) Name() string             { return "mysql" }
func (m *mysqlDialect) QuoteID(s string) string  { return quoteBacktick(s) }
func (m *mysqlDialect) Placeholder(n int) string { return "?" }
func (m *mysqlDialect) Upsert(table string, keys []string, builder *builder) string {
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

func (m *mysqlDialect) Insert(ops sqlOps, table string, builder *builder) ([]int64, error) {
	panic("implement me")
}

type ansiDialect struct {
	name string
}

func (a *ansiDialect) Name() string             { return a.name }
func (a *ansiDialect) QuoteID(s string) string  { return quoteBacktick(s) }
func (a *ansiDialect) Placeholder(n int) string { return "?" }
func (a *ansiDialect) Upsert(table string, keys []string, builder *builder) string {
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

func (a *ansiDialect) Insert(ops sqlOps, table string, builder *builder) ([]int64, error) {
	panic("implement me")
}

type pqDialect struct {
	ansiDialect
}

func (p *pqDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n+1) }

func quoteBacktick(s string) string {
	s = strings.ReplaceAll(s, "`", "``")
	return "`" + s + "`"
}

func quoteAndJoinIDs(quoteID func(s string) string, ids []string) string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = quoteID(id)
	}
	return strings.Join(out, ", ")
}

// Expand a query and arguments using the Sequel recursive expansion rules.
func expand(d dialect, withManaged bool, builder *builder, query string, args []interface{}) (string, []interface{}, error) {
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
			parameterArgs, err := expandParameter(d, withManaged, true, w, &outIndex, v)
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
				w.WriteString(quoteAndJoinIDs(d.QuoteID, builder.fields))
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
func expandParameter(d dialect, withManaged, wrap bool, w *strings.Builder, index *int, v reflect.Value) (out []interface{}, err error) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.String, reflect.Float32, reflect.Float64:
		w.WriteString(d.Placeholder(*index))
		*index++
		return []interface{}{v.Interface()}, nil

	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				w.WriteString(", ")
			}
			children, err := expandParameter(d, withManaged, wrap, w, index, v.Index(i))
			if err != nil {
				return nil, err
			}
			out = append(out, children...)
		}

	case reflect.Struct:
		if _, ok := v.Interface().(driver.Valuer); ok {
			w.WriteString(d.Placeholder(*index))
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
			children, err := expandParameter(d, withManaged, false, w, index, fv)
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
			w.WriteString(d.Placeholder(*index))
			*index++
			return []interface{}{nil}, nil
		}
		out, err = expandParameter(d, withManaged, wrap, w, index, v.Elem())
		if err != nil {
			return nil, err
		}

	case reflect.Interface:
		out, err = expandParameter(d, withManaged, wrap, w, index, v.Elem())
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.Errorf("unsupported parameter %s", v.Type())
	}
	return out, nil
}
