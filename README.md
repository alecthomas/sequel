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
users := []struct{
    Name string
    Email string
}{
    {"Moe", "moe@stooges.com"},
    {"Larry", "larry@stooges.com"},
    {"Curly", "curly@stooges.com"},
}
err = db.Insert("users", users)
```

Selecting uses a similar approach:

```go
users := []struct{
    ID int
    Name string
    Email string
}{}
err = db.Select(&users, `
    SELECT * FROM users WHERE id IN (
        SELECT user_id FROM group_members WHERE group_id = ?
    )
`, groupID)
```

## Placeholder expansion rules

Each placeholder symbol `?` in a query string maps 1:1 to a corresponding argument in the `Select()` or `Exec()` call.

Arguments are expanded recursively. Structs map to a parentheses-enclosed, comma-separated list. Slices map to a comma-separated list.

Value                                           | Corresponding expanded placeholders
------------------------------------------------|---------------------------------------
`struct{A, B, C string}{"A", "B", "C"}`         | `(?, ?, ?)`
`[]string{"A", "B"}`                            | `?, ?`
`[]struct{A, B string}{{"A", "B"}, {"C", "D"}}` | `(?, ?), (?, ?)`

## Insert

The `Insert()` method accepts a list of rows (`Insert(table, rows)`), or a vararg 
sequence (`Insert(table, row0, row1, row2)`). Column names are reflected from the first row.

## Upsert

`Upsert()` has the same syntax as `Insert()`, with the additional requirement that rows must 
contain a field marked as a primary key.

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

