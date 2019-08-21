# Sequel - A Go <-> SQL mapping package

## Why?

I wanted a very thin mapping between SQL and Go that provides:

1. `SELECT` into arbitrary `struct`s.
2. Query parameters populated from arbitrary Go types - structs, slices, etc.
3. Normalised sequential placeholders (`?`) (support for positional placeholders will hopefully come later).
4. Try to be as safe as possible.

I did not want:

1. A query DSL - we already know SQL.
2. Migration support - there are much better external tools for this.

## Tutorial / example

Open a DB connection:

```go
db, err := sequel.Open("mysql", "root@/database")
```

Insert some users:

```go
type dbUser struct {
	ID int            `db:",managed"`
    Created time.Time `db:",managed"`
	Name string
	Email string
}

users := []dbUser{
    {Name: "Moe", Email: "moe@stooges.com"},
    {Name: "Larry", Email: "larry@stooges.com"},
    {Name: "Curly", Email: "curly@stooges.com"},
}
_, err = db.Exec("INSERT INTO users ** VALUES ?", users)
```

Selecting uses a similar approach:

```go
users := []dbUser{}
err = db.Select(&users, `
    SELECT * FROM users WHERE id IN (
        SELECT user_id FROM group_members WHERE group_id = ?
    )
`, groupID)
```

## Directives

### Selectors

### Placeholders

## Placeholder expansion rules

Each placeholder symbol `?` in a query string maps 1:1 to a corresponding argument in the `Select()` or `Exec()` call.

The additional placeholder `**` will expand to the set of *unmanaged* fields in your data model.

Arguments are expanded recursively. Structs map to a parentheses-enclosed, comma-separated list. Slices map to a comma-separated list.

Value                                           | Placeholder | Corresponding expansion
------------------------------------------------|-------------|-------------------------
`struct{A, B, C string}{"A", "B", "C"}`         | `?`         | `(?, ?, ?)`
`[]string{"A", "B"}`                            | `?`         | `?, ?`
`[]struct{A, B string}{{"A", "B"}, {"C", "D"}}` | `?`         | `(?, ?), (?, ?)`
`struct{A, B, C string}{"A", "B", "C"}`         | `**`        | `a, b, c`

## Struct tag format

Struct fields may be tagged with `db:"..."` to control how Sequel maps fields. The tag has the following
syntax:

    db:"[<name>][,<option>,...]"
    
To omit a field from mapping use:

    db:"-"
    
If a field name is not explicitly provided the lower-snake-case mapping of the Go field name will be used.
eg. `MyIDField` -> `my_id_field`.
    
Tag option    | Meaning
--------------|----------------------------------------
`managed`     | Field is managed by the database. This informs `Insert()` which fields should not be propagated.
`pk`          | Field is the primary key. `pk` fields will be set after `Insert()`. Auto-increment `pk` fields should also be tagged as `managed`.

## Insert

It accepts a list of rows (`Insert(table, rows)`), or a vararg 
sequence (`Insert(table, row0, row1, row2)`). Column names are reflected from the first row.

## Upsert

`Upsert()` varargs have the same syntax as `Insert()`, however in addition it requires a list of 
columns to use as the unique constraint check.

## Dealing with schema changes

For minimum disruption, best practice for schema changes (in general, not specifically with Sequel) is
to write DDL that does not require corresponding DML changes. This means having sane default values for 
new columns, and the schema change should be applied prior to code deployment. For column removal, 
code should be modified and deployed prior to schema changes.
 
Some queries are problematic in the face of column additions, in particular the use of `SELECT *`. 
If an additional column exists in the schema but does not exist in your model, the result rows will 
fail to deserialise.

There are two options here. 

1. Explicitly list columns in your query.
2. Use `**`. This automates the approach of explicitly listing column names.

## Examples

### A simple select with parameters populated from a struct

```go
selector := struct{Name, Email string}{"Moe", "moe@stooges.com"}
err := db.Select(&users, `SELECT * FROM users WHERE (name, email) = ?`, selector)
```

### A complex multi-row select

For example, given a query like this:

```sql
SELECT * FROM users WHERE (name, email) IN
    ("Moe", "moe@stooges.com"),
    ("Larry", "larry@stooges.com"),
    ("Curly", "curly@stooges.com")
```

Sequel allows the equivalent query with dynamic inputs to be expressed like so. First, with the input data:

```go
// For the purposes of this example this is a static list, but in "real" code this would typically be the result
// of another query, or user-provided.
matches := []struct{Name, Email string}{
    {"Moe", "moe@stooges.com"},
    {"Larry", "larry@stooges.com"},
    {"Curly", "curly@stooges.com"},
}
```

The Sequel query to match all rows with those columns is this:

```go
err := db.Select(&users, `SELECT * FROM users WHERE (name, email) IN ?`, matches)
```

Which is equivalent to the following SQLx code:

```go
placeholders := []string{}
args := []interface{}
for _, match := range matches {
    placeholders = append(placeholders, "?", "?")
    args = append(args, match.Name, match.Email)
}
err := db.Select(&users,
    ` SELECT * FROM users WHERE email IN (` + strings.Join(placeholders, ",") + `)`,
    args...,
)
```

Or manually expanded:

```go
err := db.Select(&users, `SELECT * FROM users WHERE (name, email) IN (?, ?), (?, ?), (?, ?)`,
    matches[0].Name, matches[0].Email,
    matches[1].Name, matches[1].Email,
    matches[2].Name, matches[2].Email,
)
```

