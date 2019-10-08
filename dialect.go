package sequel

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
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
		"postgres": func() dialect {
			p := &pqDialect{ansiUpsertMixin{}}
			p.ansiUpsertMixin.d = p
			return p
		}(),
		"sqlite3": func() dialect {
			d := &sqliteDialect{}
			d.ansiUpsertMixin.d = d
			d.lastInsertMixin.d = d
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
	// nolint: gosec
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
		set = append(set, fmt.Sprintf("%s=VALUES(%s)",
			quoteBacktick(field), quoteBacktick(field)))
	}
	// nolint: gosec
	return fmt.Sprintf(`
			INSERT INTO %s (%s) VALUES ?
			ON DUPLICATE KEY UPDATE %s
		`,
		quoteBacktick(table),
		quoteAndJoinIDs(quoteBacktick, builder.filteredFields(true)),
		strings.Join(set, ","))
}

type ansiUpsertMixin struct {
	d dialect
}

func (a *ansiUpsertMixin) Upsert(table string, keys []string, builder *builder) string {
	set := []string{}
	for _, field := range builder.filteredFields(true) {
		// nolint: gosec
		set = append(set, fmt.Sprintf("%s = EXCLUDED.%s",
			a.d.QuoteID(field), a.d.QuoteID(field)))
	}
	// nolint: gosec
	return fmt.Sprintf(`
			INSERT INTO %s (%s) VALUES ?
			ON CONFLICT (%s)
			DO UPDATE SET %s
		`,
		a.d.QuoteID(table),
		quoteAndJoinIDs(a.d.QuoteID, builder.filteredFields(true)),
		quoteAndJoinIDs(a.d.QuoteID, keys), strings.Join(set, ", "))
}

type sqliteDialect struct {
	ansiUpsertMixin
	lastInsertMixin
}

var _ dialect = &sqliteDialect{}

func (s *sqliteDialect) Name() string           { return "sqlite" }
func (*sqliteDialect) QuoteID(s string) string  { return quoteBacktick(s) }
func (*sqliteDialect) Placeholder(n int) string { return "?" }

type pqDialect struct{ ansiUpsertMixin }

var _ dialect = &pqDialect{}

func (p *pqDialect) Name() string             { return "postgres" }
func (p *pqDialect) QuoteID(s string) string  { return strconv.Quote(s) }
func (p *pqDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n+1) }

func (p *pqDialect) Insert(ops sqlOps, table string, rows []interface{}) ([]int64, error) {
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
	// nolint: gosec
	query := fmt.Sprintf(`INSERT INTO %s (%s) VALUES ?`,
		p.QuoteID(table),
		quoteAndJoinIDs(p.QuoteID, builder.filteredFields(false)))

	if builder.pk != "" {
		query += fmt.Sprintf(` RETURNING %s`, p.QuoteID(builder.pk))
	}
	query, args, err := expand(p, false, builder, query, []interface{}{arg})
	if err != nil {
		return nil, err
	}
	outRows, err := ops.Query(query, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute %q", query)
	}
	defer outRows.Close()

	if builder.pk == "" {
		return nil, nil
	}

	i := 0
	f := builder.fieldMap[builder.pk]
	ids := make([]int64, 0, count)
	for outRows.Next() {
		var id int64
		err = outRows.Scan(&id)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan inserted ID")
		}
		ids = append(ids, id)
		row := indirectValue(slice.Index(i))
		rf := row.FieldByIndex(f.index)
		rf.SetInt(ids[i])
		i++
	}
	return ids, outRows.Err()
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
//
// If "builder" is provided it will be used to interpolate any `**` placeholders.
// If it is not provided, the matching positional argument will be used.
func expand(d dialect, withManaged bool, b *builder, query string, args []interface{}) (string, []interface{}, error) {
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
			paramBuilder := b
			if paramBuilder == nil {
				var err error
				paramBuilder, err = makeRowBuilderForType(reflect.TypeOf(args[argi]))
				if err != nil {
					return "", nil, err
				}
			}
			// Wildcard - expand all column names.
			w.WriteString(quoteAndJoinIDs(d.QuoteID, paramBuilder.fields))

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
func expandParameter(d dialect, withManaged, wrap bool, w *strings.Builder, index *int, v reflect.Value) ([]interface{}, error) { // nolint: interfacer
	if _, ok := v.Interface().(driver.Valuer); ok {
		w.WriteString(d.Placeholder(*index))
		*index++
		return []interface{}{v.Interface()}, nil
	}

	var out []interface{}

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
			out = append(out, fv.Interface())
			w.WriteString(d.Placeholder(*index))
			*index++
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
		var err error
		out, err = expandParameter(d, withManaged, wrap, w, index, v.Elem())
		if err != nil {
			return nil, err
		}

	case reflect.Interface:
		var err error
		out, err = expandParameter(d, withManaged, wrap, w, index, v.Elem())
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.Errorf("unsupported parameter %s", v.Type())
	}
	return out, nil
}
