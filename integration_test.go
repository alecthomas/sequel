// +build integration

package sequel

import (
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql" // imported for side-effects
	_ "github.com/lib/pq"              // imported for side-effects
	_ "github.com/mattn/go-sqlite3"    // imported for side-effects
	"github.com/stretchr/testify/require"
)

const sqliteTestFile = "./sqlite_integration_test.db"

func TestDialects(t *testing.T) {
	type User struct {
		ID   int `db:",pk,managed"`
		Name string
	}

	drivers := []struct {
		driver  string
		dsn     string
		create  string
		cleanup func(db *DB) error
	}{
		{driver: "sqlite3",
			dsn: sqliteTestFile,
			create: `
				CREATE TABLE users (
					id INTEGER PRIMARY KEY,
					name VARCHAR(128) NOT NULL
				)`,
			cleanup: func(*DB) error { return os.Remove(sqliteTestFile) }},
		{driver: "mysql",
			dsn: "root:@/sequel_test",
			create: `
				CREATE TABLE users (
					id INTEGER PRIMARY KEY AUTO_INCREMENT,
					name VARCHAR(128) NOT NULL
				)`,
			cleanup: func(db *DB) error {
				_, _ = db.Exec(`DROP TABLE users`)
				return nil
			},
		},
		{driver: "postgres",
			dsn: "dbname=sequel_test sslmode=disable",
			create: `
				CREATE TABLE users (
					id SERIAL PRIMARY KEY,
					name VARCHAR(128) NOT NULL
				)`,
			cleanup: func(db *DB) error {
				_, _ = db.Exec(`DROP TABLE users`)
				return nil
			},
		},
	}

	insertSlice := func(t *testing.T, db *DB) []*User {
		users := []*User{{Name: "Alice"}, {Name: "Bob"}}
		ids, err := db.Insert("users", users)
		require.NoError(t, err)
		require.Len(t, ids, 2)
		require.Equal(t, int(ids[0]), users[0].ID)
		require.Equal(t, int(ids[1]), users[1].ID)
		return users
	}

	tests := []struct {
		name string
		test func(t *testing.T, db *DB)
	}{
		{"InsertOne", func(t *testing.T, db *DB) {
			user := &User{Name: "Bob"}
			ids, err := db.Insert("users", user)
			require.NoError(t, err)
			require.Len(t, ids, 1)
			require.Equal(t, int(ids[0]), user.ID)
		}},
		{"InsertSlice", func(t *testing.T, db *DB) {
			insertSlice(t, db)
		}},
		{"SelectOne", func(t *testing.T, db *DB) {
			insertSlice(t, db)
			user := &User{}
			err := db.SelectOne(user, `SELECT ** FROM users WHERE name = ?`, "Alice")
			require.NoError(t, err)
			require.Equal(t, "Alice", user.Name)
		}},
		{"Select", func(t *testing.T, db *DB) {
			expected := insertSlice(t, db)
			users := []*User{}
			err := db.Select(&users, `SELECT ** FROM users ORDER BY name`)
			require.NoError(t, err)
			require.Equal(t, expected, users)
		}},
		{"Upsert", func(t *testing.T, db *DB) {
			users := insertSlice(t, db)
			users[0].Name = "Alex"
			_, err := db.Upsert("users", []string{"id"}, users[0])
			require.NoError(t, err)

			actual := []*User{}
			err = db.Select(&actual, `SELECT ** FROM users ORDER BY name`)
			require.NoError(t, err)
			require.Equal(t, users, actual)
		}},
	}
	for _, driver := range drivers {
		t.Run(driver.driver, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					db, err := Open(driver.driver, driver.dsn)
					require.NoError(t, err)
					defer func() {
						_ = driver.cleanup(db)
						_ = db.Close()
					}()

					if driver.cleanup != nil {
						_ = driver.cleanup(db)
					}

					_, err = db.Exec(driver.create)
					require.NoError(t, err)

					test.test(t, db)
				})
			}
		})
	}
}
