package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/avalchev94/sqlxt"

	_ "github.com/lib/pq"
)

func setupDB() *sql.DB {
	connString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		os.Getenv("PG_HOST"), os.Getenv("PG_PORT"), os.Getenv("PG_USER"),
		os.Getenv("PG_PASSWORD"), "sqlorm")

	db, err := sql.Open("postgres", connString)
	if err != nil {
		log.Fatalln(err)
	}

	db.Exec("DROP TABLE newspapers")

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

	return db
}

func closeDB(db *sql.DB) {
	db.Exec("DROP TABLE newspapers")
	db.Close()
}

func testMap(db *sql.DB) {
	newspapers := map[string]interface{}{}

	scanner := sqlxt.NewScanner(db.Query("SELECT * FROM newspapers"))
	if err := scanner.Scan(&newspapers); err != nil {
		log.Fatalln(err)
	}

	fmt.Println(newspapers)
}

// Newspaper is a example structure with 'sql' tags
type Newspaper struct {
	ID      int32  `sql:"id"`
	Title   string `sql:"title"`
	Country string `sql:"country"`
}

func testStruct(db *sql.DB) {
	var newspapers []Newspaper

	scanner := sqlxt.NewScanner(db.Query("SELECT * FROM newspapers"))
	if err := scanner.Scan(&newspapers); err != nil {
		log.Fatalln(err)
	}

	fmt.Println(newspapers)
}

func testChan(db *sql.DB) {
	newspapers := make(chan Newspaper)
	go func() {
		for n := range newspapers {
			fmt.Println(n)
		}
	}()

	scanner := sqlxt.NewScanner(db.Query("SELECT * FROM newspapers"))
	if err := scanner.Scan(&newspapers); err != nil {
		log.Fatalln(err)
	}
	close(newspapers)
}

func main() {
	db := setupDB()
	defer closeDB(db)

	fmt.Println("Map example:")
	testMap(db)

	fmt.Println("\nStruct example:")
	testStruct(db)

	fmt.Println("\nChannel example:")
	testChan(db)
}
