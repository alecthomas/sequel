package sequel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type TestMetadata struct {
	Email string
	Age   int
}

type TestUser struct {
	ID   int
	Name string
	TestMetadata
}

func TestDialectExpand(t *testing.T) {
	type dialectResult struct {
		dialect dialect
		query   string
		args    []interface{}
	}

	// Note that the sqlite dialect is not currently tested as its dialect is identical to MySQL.
	// If the dialects diverge this will change.
	tests := []struct {
		name     string
		query    string
		args     []interface{}
		expected []dialectResult
	}{
		{
			name:  "Scalars",
			query: `INSERT INTO user (name, age, email) VALUES (?, ?, ?)`,
			args:  []interface{}{"Moe", 39, "moe@stooges.com"},
			expected: []dialectResult{
				{
					dialect: pqDialect,
					args:    []interface{}{"Moe", 39, "moe@stooges.com"},
					query:   `INSERT INTO user (name, age, email) VALUES ($1, $2, $3)`,
				},
				{
					dialect: mysqlDialect,
					args:    []interface{}{"Moe", 39, "moe@stooges.com"},
					query:   `INSERT INTO user (name, age, email) VALUES (?, ?, ?)`,
				},
			},
		},
		{
			name:  "Struct",
			query: `INSERT INTO user (name, age, email) VALUES ?`,
			args: []interface{}{struct {
				Name  string;
				Age   int;
				Email string
			}{"Moe", 39, "moe@stooges.com"}},
			expected: []dialectResult{
				{
					dialect: pqDialect,
					args:    []interface{}{"Moe", 39, "moe@stooges.com"},
					query:   `INSERT INTO user (name, age, email) VALUES ($1, $2, $3)`,
				},
				{
					dialect: mysqlDialect,
					args:    []interface{}{"Moe", 39, "moe@stooges.com"},
					query:   `INSERT INTO user (name, age, email) VALUES (?, ?, ?)`,
				},
			},
		},
		{
			name:  "SliceOfStructs",
			query: `INSERT INTO user VALUES ?`,
			args: []interface{}{[]struct {
				Age  int
				Name string
			}{
				{43, "Moe"},
				{39, "Curly"},
			}},
			expected: []dialectResult{
				{
					dialect: pqDialect,
					query:   `INSERT INTO user VALUES ($1, $2), ($3, $4)`,
					args:    []interface{}{43, "Moe", 39, "Curly"},
				},
				{
					dialect: mysqlDialect,
					query:   `INSERT INTO user VALUES (?, ?), (?, ?)`,
					args:    []interface{}{43, "Moe", 39, "Curly"},
				},
			},
		},
		{
			name:  "EmbeddedStruct",
			query: `INSERT INTO table VALUES ?`,
			args: []interface{}{
				[]TestUser{
					TestUser{
						ID:   2,
						Name: "Moe",
						TestMetadata: TestMetadata{
							Email: "moe@stooges.com",
							Age:   39,
						},
					},
					TestUser{
						ID:   3,
						Name: "Curly",
						TestMetadata: TestMetadata{
							Email: "curly@stooges.com",
							Age:   39,
						},
					},
				},
			},
			expected: []dialectResult{
				{
					dialect: pqDialect,
					query:   `INSERT INTO table VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)`,
					args:    []interface{}{2, "Moe", "moe@stooges.com", 39, 3, "Curly", "curly@stooges.com", 39},
				},
				{
					dialect: mysqlDialect,
					query:   `INSERT INTO table VALUES (?, ?, ?, ?), (?, ?, ?, ?)`,
					args:    []interface{}{2, "Moe", "moe@stooges.com", 39, 3, "Curly", "curly@stooges.com", 39},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, result := range test.expected {
				t.Run(result.dialect.name, func(t *testing.T) {
					query, args, err := result.dialect.expand(nil, test.query, test.args)
					require.NoError(t, err, "%q", test.query)
					require.Equal(t, result.query, query)
					require.Equal(t, result.args, args)
				})
			}
		})
	}
}
