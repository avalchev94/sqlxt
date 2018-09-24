package sqlxt

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func connectDB() (*sql.DB, error) {
	connString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		os.Getenv("PG_HOST"), os.Getenv("PG_PORT"), os.Getenv("PG_USER"),
		os.Getenv("PG_PASSWORD"), "sqlorm")

	return sql.Open("postgres", connString)
}

func createDBData(db *sql.DB) error {
	_, err := db.Exec(
		`CREATE TABLE users 
		(
			id SERIAL PRIMARY KEY,
			name varchar(100) NOT NULL,
			password varchar(100) NOT NULL
		)`)
	if err != nil {
		return err
	}
	_, err = db.Exec("INSERT INTO users (name, password) VALUES ($1,$2), ($3,$4)",
		"avalchev94", "github",
		"avalchev", "linkedin")
	if err != nil {
		return err
	}
	return nil
}

func dropDBData(db *sql.DB) error {
	_, err := db.Exec("DROP TABLE users")
	return err
}

func TestScanner(t *testing.T) {
	assert := assert.New(t)

	db, err := connectDB()
	assert.NoError(err)
	defer db.Close()

	assert.NoError(createDBData(db))
	type User struct {
		ID       int32  `sql:"id"`
		Name     string `sql:"name"`
		Password string `sql:"password"`
	}

	cases := []struct {
		id       int
		query    string
		expected interface{}
	}{
		{1, "SELECT * FROM users", User{1, "avalchev94", "github"}},
		{2, "SELECT * FROM users", map[string]interface{}{"id": int32(1), "name": "avalchev94", "password": "github"}},
		{3, "SELECT name, password FROM users", map[int]string{0: "avalchev94", 1: "github"}},
		{4, "SELECT COUNT(*) FROM users", int64(2)},
		{5, "SELECT name FROM users", "avalchev94"},
		{6, "SELECT name FROM users", interface{}("avalchev94")},
		{7, "SELECT * FROM users", []interface{}{int32(1), "avalchev94", "github"}},
		{8, "SELECT name, password FROM users", []string{"avalchev94", "github"}},
		{9, "SELECT id FROM users", []int32{1}},
		{10, "SELECT * FROM users", []User{
			{1, "avalchev94", "github"},
			{2, "avalchev", "linkedin"},
		}},
		{11, "SELECT * FROM users", [][]interface{}{
			{int32(1), "avalchev94", "github"},
			{int32(2), "avalchev", "linkedin"},
		}},
		{12, "SELECT * FROM users", []map[string]interface{}{
			{"id": int32(1), "name": "avalchev94", "password": "github"},
			{"id": int32(2), "name": "avalchev", "password": "linkedin"},
		}},
		{13, "SELECT name, password FROM users", [][]string{
			{"avalchev94", "github"},
			{"avalchev", "linkedin"},
		}},
		{14, "SELECT id FROM users", [][]int32{{1}, {2}}},
	}

	for _, c := range cases {
		rows, err := db.Query(c.query)
		assert.NoError(err, "case %d", c.id)

		dest := reflect.New(reflect.TypeOf(c.expected))
		err = NewScanner(rows).Scan(dest.Interface())
		assert.NoError(err, "case %d", c.id)
		assert.Equal(c.expected, dest.Elem().Interface(), "case %d", c.id)
	}

	assert.NoError(dropDBData(db))
}
