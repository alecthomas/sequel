package sequel_test

import (
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3" // imported for side-effects
	"github.com/stretchr/testify/require"

	"github.com/alecthomas/sequel"
)

type nested struct {
	Name  *string
	Email string
}

type user struct {
	ID    int
	Name  *string
	Email string
}

type invalidUser struct {
	ID   int
	Name *string
	Mail string
}

var (
	larry = user{Name: str("Larry"), Email: "larry@stooges.com", ID: 1}
	moe   = user{Email: "moe@stooges.com", ID: 2}
	curly = user{Name: str("Curly"), Email: "curly@stooges.com", ID: 3}
)

func TestDBSelect(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		args     []interface{}
		dest     interface{}
		expected interface{}
		err      string
	}{
		{name: "SelectNullPointer",
			query:    `SELECT * FROM users WHERE email = ?`,
			args:     []interface{}{`moe@stooges.com`},
			expected: &[]user{moe}},
		{name: "UnknownColumnName",
			query: `SELECT id, nmame, email FROM users`,
			err:   "no such column: nmame"},
		{name: "MismatchedFieldName",
			query: "SELECT * FROM users",
			dest:  &[]invalidUser{},
			err:   "no field"},
		{name: "Struct",
			query: "SELECT * FROM users WHERE (name, email) = ?",
			args: []interface{}{
				nested{Name: str("Larry"), Email: "larry@stooges.com"},
			},
			dest:     &[]user{},
			expected: &[]user{larry}},
		{name: "SliceOfStructs",
			query: "SELECT * FROM users WHERE (name, email) IN (?)",
			args: []interface{}{
				[]nested{{Name: str("Larry"), Email: "larry@stooges.com"}},
			},
			dest:     &[]user{},
			expected: &[]user{larry}},
		{name: "SliceOfStrings",
			query: "SELECT * FROM users WHERE email IN (?)",
			args: []interface{}{
				[]string{"curly@stooges.com", "moe@stooges.com"},
			},
			dest:     &[]user{},
			expected: &[]user{moe, curly}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := databaseFixture(t)
			defer db.Close()

			insertFixtures(t, db)

			if test.dest == nil {
				test.dest = &[]user{}
			}
			err := db.Select(test.dest, test.query, test.args...)
			if test.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, test.dest)
			}
		})
	}
}

func TestSelectOne(t *testing.T) {
	db := databaseFixture(t)
	defer db.Close()
	insertFixtures(t, db)

	type User struct {
		Name  *string
		Email string
	}

	tests := []struct {
		name     string
		sql      string
		expected User
		err      bool
	}{
		{name: "SingleRow",
			sql:      `SELECT name, email FROM users WHERE name IS NULL`,
			expected: User{Email: "moe@stooges.com"}},
		{name: "MultipleRowsErrors",
			sql: `SELECT name, email FROM users`,
			err: true},
		{name: "NoRowsErrors",
			sql: `SELECT name, email FROM users WHERE email = "WOWOWOW"`,
			err: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := User{}
			err := db.SelectOne(&actual, test.sql)
			if test.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCommitOrRollbackOnError(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		count int
	}{
		{name: "CommitsOnNoError", err: nil, count: 1},
		{name: "RollbackOnError", err: errors.New("error"), count: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := databaseFixture(t)
			defer db.Close()

			tx, err := db.Begin()
			require.NoError(t, err)
			_, err = tx.Exec(
				`INSERT INTO users (name, email) VALUES (?, ?)`,
				"Larry", "larry@stooges.com")
			require.NoError(t, err)

			tx.CommitOrRollbackOnError(&test.err)
			count, err := db.SelectInt(`SELECT COUNT(*) FROM users`)
			require.NoError(t, err)
			require.Equal(t, test.count, count)
		})
	}
}

func databaseFixture(t *testing.T) *sequel.DB {
	t.Helper()
	db, err := sequel.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	_, err = db.Exec(`
	CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name STRING,
		email STRING NOT NULL
	)
	`)
	require.NoError(t, err)
	return db
}

func str(p string) *string { return &p }

func insertFixtures(t *testing.T, db *sequel.DB) {
	t.Helper()
	// Insert fixture.
	users := []struct {
		Name  *string
		Email string
	}{
		{str("Larry"), "larry@stooges.com"},
		{nil, "moe@stooges.com"},
		{str("Curly"), "curly@stooges.com"},
	}
	_, err := db.Exec(`INSERT INTO users (name, email) VALUES ?`, users)
	require.NoError(t, err)
}
