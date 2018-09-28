package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/avalchev94/sqlxt"

	_ "github.com/lib/pq"
)

func connectDB() (*sql.DB, error) {
	connString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		os.Getenv("PG_HOST"), os.Getenv("PG_PORT"), os.Getenv("PG_USER"),
		os.Getenv("PG_PASSWORD"), "sqlorm")

	return sql.Open("postgres", connString)
}

func testMap(db *sql.DB) {
	newspapers := map[string]interface{}{}

	scanner := sqlxt.NewScanner(db.Query("SELECT * FROM newspapers"))
	if err := scanner.Scan(newspapers); err != nil {
		log.Fatalln(err)
	}

	fmt.Println(newspapers)
}

func testStruct(db *sql.DB) {
	type Newspaper struct {
		ID      int32  `sql:"id"`
		Title   string `sql:"title"`
		Country string `sql:"country"`
	}

	newspapers := []Newspaper{}

	scanner := sqlxt.NewScanner(db.Query("SELECT * FROM newspapers"))
	if err := scanner.Scan(&newspapers); err != nil {
		log.Fatalln(err)
	}

	fmt.Println(newspapers)
}

func main() {
	db, err := connectDB()
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		db.Exec("DROP TABLE newspapers")
		db.Close()
	}()

	_, err = db.Exec(`
	CREATE TABLE newspapers
	(
		id SERIAL PRIMARY KEY,
		title varchar(100) NOT NULL,
		country varchar(100) NOT NULL
	)`)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = db.Exec("INSERT INTO newspapers (title, country) VALUES ($1, $2), ($3, $4)",
		"The Guardian", "United Kingdom",
		"Trud", "Bulgaria")
	if err != nil {
		log.Fatalln(err)
	}

	testMap(db)
	testStruct(db)
}
