package sqlxt

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	type User struct {
		ID       int32  `sql:"id"`
		Name     string `sql:"name"`
		Password string `sql:"password"`
	}

	assert := assert.New(t)

	connString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		os.Getenv("PG_HOST"), os.Getenv("PG_PORT"), os.Getenv("PG_USER"),
		os.Getenv("PG_PASSWORD"), "sqlorm")

	db, err := sql.Open("postgres", connString)
	assert.NoError(err)

	_, err = db.Exec(
		`CREATE TABLE users 
		(
			id SERIAL PRIMARY KEY,
			name varchar(100) NOT NULL,
			password varchar(100) NOT NULL
		)`)
	assert.NoError(err)

	_, err = db.Exec("INSERT INTO users (name, password) VALUES ($1,$2)", "avalchev94", "github")
	assert.NoError(err)

	rows, err := db.Query("SELECT * FROM users")

	var user User
	err = NewScanner(rows).Scan(&user)
	assert.NoError(err)
	fmt.Println(user)

	_, err = db.Exec("DROP TABLE users")
	assert.NoError(err)
}
