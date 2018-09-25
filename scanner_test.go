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

func setupDB() (*sql.DB, error) {
	connString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		os.Getenv("PG_HOST"), os.Getenv("PG_PORT"), os.Getenv("PG_USER"),
		os.Getenv("PG_PASSWORD"), "sqlorm")

	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(
		`CREATE TABLE users 
		(
			id SERIAL PRIMARY KEY,
			name varchar(100) NOT NULL,
			password varchar(100) NOT NULL
		)`)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("INSERT INTO users (name, password) VALUES ($1,$2), ($3,$4)",
		"avalchev94", "github",
		"avalchev", "linkedin")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func closeDB(db *sql.DB) {
	db.Exec("DROP TABLE users")
	db.Close()
}

func TestPrimitive(t *testing.T) {
	assert := assert.New(t)

	db, err := setupDB()
	assert.NoError(err)
	assert.NotNil(db)
	defer closeDB(db)

	cases := []struct {
		id       int
		query    string
		expected interface{}
	}{
		{1, "SELECT COUNT(*) FROM users", 2},
		{2, "SELECT id FROM users", int64(1)},
		{3, "SELECT name FROM users", "avalchev94"},
		{4, "SELECT password FROM users", interface{}("github")},
		{5, "SELECT id FROM users", []int64{1}},
		{6, "SELECT name, password FROM users", []string{"avalchev94", "github"}},
		{7, "SELECT * FROM users", []interface{}{int32(1), "avalchev94", "github"}},
		{8, "SELECT name, password FROM users", [][]string{
			{"avalchev94", "github"},
			{"avalchev", "linkedin"},
		}},
		{9, "SELECT * FROM users", [][]interface{}{
			{int32(1), "avalchev94", "github"},
			{int32(2), "avalchev", "linkedin"},
		}},
		{10, "SELECT id FROM users WHERE id=3", []int64(nil)},
	}

	for _, c := range cases {
		rows, err := db.Query(c.query)
		assert.NoError(err, "case %d", c.id)

		dest := reflect.New(reflect.TypeOf(c.expected))
		err = NewScanner(rows).Scan(dest.Interface())
		assert.NoError(err, "case %d", c.id)
		assert.Equal(c.expected, dest.Elem().Interface(), "case %d", c.id)
	}
}

func TestStruct(t *testing.T) {
	assert := assert.New(t)

	db, err := setupDB()
	assert.NoError(err)
	assert.NotNil(db)
	defer closeDB(db)

	type User struct {
		ID       int64  `sql:"id"`
		Name     string `sql:"name"`
		Password string `sql:"password"`
	}

	type User2 struct {
		ID       int32  `sql:"id"`
		Name     string `sql:"name"`
		Password string `sql:"-"`
	}

	type User3 struct {
		ID       int    `sql:"ids"` // wrong on purpose
		Name     string `sql:"name"`
		password string
	}

	type User4 struct {
		ID       int64
		name     string // unexported fields should not be settable
		Password string
	}

	cases := []struct {
		id       int
		query    string
		expected interface{}
	}{
		{1, "SELECT * FROM users", User{1, "avalchev94", "github"}},
		{2, "SELECT * FROM users", User2{ID: 1, Name: "avalchev94"}},
		{3, "SELECT * FROM users", User3{Name: "avalchev94"}},
		{4, "SELECT * FROM users WHERE id=2", User4{ID: 2, Password: "linkedin"}},
		{5, "SELECT * FROM users", []User{
			{1, "avalchev94", "github"},
			{2, "avalchev", "linkedin"},
		}},
		{6, "SELECT name FROM users", []User2{
			{Name: "avalchev94"},
			{Name: "avalchev"},
		}},
		{7, "SELECT id FROM users", []User3{
			{ID: 0, Name: ""},
			{ID: 0, Name: ""},
		}},
		{8, "SELECT * FROM users WHERE id>2", []User(nil)},
	}

	for _, c := range cases {
		rows, err := db.Query(c.query)
		assert.NoError(err, "case %d", c.id)

		dest := reflect.New(reflect.TypeOf(c.expected))
		err = NewScanner(rows).Scan(dest.Interface())
		assert.NoError(err, "case %d", c.id)
		assert.Equal(c.expected, dest.Elem().Interface(), "case %d", c.id)
	}
}

func TestMap(t *testing.T) {
	assert := assert.New(t)

	db, err := setupDB()
	assert.NoError(err)
	assert.NotNil(db)
	defer closeDB(db)

	cases := []struct {
		id       int
		query    string
		expected interface{}
	}{
		{1, "SELECT * FROM users", map[string]interface{}{
			"id":       int32(1),
			"name":     "avalchev94",
			"password": "github",
		}},
		{2, "SELECT name, password FROM users WHERE id=2", map[int]string{
			0: "avalchev",
			1: "linkedin",
		}},
		{3, "SELECT * FROM users", []map[string]interface{}{
			{"id": int32(1), "name": "avalchev94", "password": "github"},
			{"id": int32(2), "name": "avalchev", "password": "linkedin"},
		}},
		{4, "SELECT * FROM users WHERE name='wrong_name'", []map[string]interface{}(nil)},
	}

	for _, c := range cases {
		rows, err := db.Query(c.query)
		assert.NoError(err, "case %d", c.id)

		dest := reflect.New(reflect.TypeOf(c.expected))
		err = NewScanner(rows).Scan(dest.Interface())
		assert.NoError(err, "case %d", c.id)
		assert.Equal(c.expected, dest.Elem().Interface(), "case %d", c.id)
	}
}
