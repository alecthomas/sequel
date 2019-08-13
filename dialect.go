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
		"mysql": func() dialect {
			d := &mysqlDialect{lastInsertMixin{idIsFirst: true}}
			d.d = d
			return d
		}(),
		"postgres": &pqDialect{ansiDialect{name: "postgres"}},
		"sqlite3": func() dialect {
			d := &sqliteDialect{ansiDialect{name: "sqlite3"}, lastInsertMixin{}}
			d.d = d
			return d
		}(),
	}
)

// A dialect knows how to perform SQL dialect specific operations.
//
// eg. ? expansion
//
// 		"SELECT * FROM users WHERE id = ? OR name = ?"
type dialect interface {
	Name() string
	// Quote a table or column identifier.
	QuoteID(s string) string
	// Return the dialect-specific placeholder string for parameter "n".
	Placeholder(n int) string
	// Constructs an upsert statement.
	//
	// Must return a statement with a single ? where values will be inserted.
	Upsert(table string, keys []string, builder *builder) string
	// Insert rows, returning the IDs inserted.
	Insert(ops sqlOps, table string, rows []interface{}) ([]int64, error)
}

type lastInsertMixin struct {
	d         dialect
	idIsFirst bool // MySQL returns the FIRST inserted ID ... because why wouldn't it.
}

func (l *lastInsertMixin) Insert(ops sqlOps, table string, rows []interface{}) ([]int64, error) {
	arg, count, t, slice := typeForMutationRows(rows...)
	builder, err := makeRowBuilderForType(t)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to map type %s", t)
	}
	elem := slice.Index(0)
	if elem.Kind() == reflect.Interface {
		elem = elem.Elem()
	}
	if builder.pk != "" && elem.Kind() == reflect.Struct {
		return nil, errors.Errorf("can't set PK on value %s, must be *%s", elem.Type(), elem.Type())
	}
	query := fmt.Sprintf(`INSERT INTO %s (%s) VALUES ?`,
		l.d.QuoteID(table),
		quoteAndJoinIDs(l.d.QuoteID, builder.filteredFields(false)))
	query, args, err := expand(l.d, false, builder, query, []interface{}{arg})
	if err != nil {
		return nil, err
	}
	result, err := ops.Exec(query, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute %q", query)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to count affected rows")
	}
	if affected != int64(count) {
		return nil, errors.Errorf("affected rows %d did not match row count of %d", affected, count)
	}
	lastID, err := result.LastInsertId()
	if err != nil {
		return nil, nil
	}
	ids := make([]int64, 0, count)
	if l.idIsFirst {
		for i := 0; i < count; i++ {
			ids = append(ids, int64(i)+lastID)
		}
	} else {
		base := lastID - int64(count)
		for i := 0; i < count; i++ {
			id := base + 1 + int64(i)
			ids = append(ids, id)
		}
	}

	// Set IDs on the rows.
	if builder.pk != "" {
		for i := 0; i < slice.Len(); i++ {
			f := builder.fieldMap[builder.pk]
			row := indirectValue(slice.Index(i))
			rf := row.FieldByIndex(f.index)
			rf.SetInt(ids[i])
		}
	}

	return ids, nil
}

type mysqlDialect struct {
	lastInsertMixin
}

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

type sqliteDialect struct {
	ansiDialect
	lastInsertMixin
}

type pqDialect struct{ ansiDialect }

func (p *pqDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n+1) }
func (p *pqDialect) Insert(ops sqlOps, table string, rows []interface{}) ([]int64, error) {
	panic("not implemented")
}

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
	if _, ok := v.Interface().(driver.Valuer); ok {
		w.WriteString(d.Placeholder(*index))
		*index++
		return []interface{}{v.Interface()}, nil
	}

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
