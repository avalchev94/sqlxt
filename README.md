# sqlxt
sqlxt is a golang package which provides a simple extension to `database/sql` standard package.\
**It's still in development, so it must improve in the future with more features and tests.**

# Usage
For the whole example, go to [example/example.go](https://github.com/avalchev94/sqlxt/tree/master/example/example.go).\
For more examples, go to [scanner_test.go](https://github.com/avalchev94/sqlxt/blob/master/scanner_test.go)

```golang
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/avalchev94/sqlxt"
	_ "github.com/lib/pq"
)

func testMap(db *sql.DB) {
	rows, err := db.Query("SELECT * FROM newspapers")
	if err != nil {
		log.Fatalln(err)
	}

	newspapers := map[string]interface{}{}
	if err := sqlxt.NewScanner(rows).Scan(newspapers); err != nil {
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

	rows, err := db.Query("SELECT * FROM newspapers")
	if err != nil {
		log.Fatalln(err)
	}

	newspapers := []Newspaper{}
	if err := sqlxt.NewScanner(rows).Scan(&newspapers); err != nil {
		log.Fatalln(err)
	}

	fmt.Println(newspapers)
}

func main() {
	db, err := connectDB()
  
  ...
  ...

	testMap(db)
	testStruct(db)
}

```

# Install

```bash
go get -u github.com/avalchev94/sqlxt
```