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
err = db.Exec(`INSERT INTO users (name, email) VALUES ?`, users)
```

> Currently there is no `Insert(...)` method; insertion is done via SQL.
> The downside of this is that the primary key of the inserted model will not be updated
> automatically. As Sequel evolves, this decision may be reassessed.

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

## Placeholder parameter expansion

Sequel supports placeholder parameter expansion not only from scalar types (`int`, `string`, etc.) but from slices,
structs, and slices of structs.

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