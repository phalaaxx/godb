cdb
===

This is a simple interface for Golang to constant database (cdb) file format.  
Currently the interface only supports writing.

Usage
---

```go
package main

import (
	"fmt"
	"github.com/phalaaxx/cdb"
	"log"
)

func main() {
	var err error
	var db *Writer
	if db, err = cdb.Create("test.cdb", 0644); err != nil {
		log.Fatal(fmt.Errorf("Create: %v", err))
	}
	if err = db.Add("email@address.com", "alias@address.com"); err != nil {
		log.Fatal(fmt.Errorf("Add: %v", err))
	}
	if err = db.Close(); err != nil {
		log.Fatal(fmt.Errorf("Close: %v", err))
	}
}
```

The following example uses Update function. Second argument is time.Time which specifies
when was the last data change, that means if the file specified already exists and its
mtime is older than the time specified by argument it will be regenerated with new data.

```go
package main

import (
	"fmt"
	"github.com/phalaaxx/cdb"
	"log"
	"time"
)

func main() {
	err := cdb.Update(
		"/etc/postfix/cdb/virtual-alias-maps.cdb",
		time.Now(),
		func(db *cdb.Writer) error {
			if err := db.Add("email@address.com", "alias@address.com"); err != nil {
				return err
			}
			return nil
		},
	)
	if err != nil {
		log.Fatal(fmt.Errorf("cdb.Update: %v", err))
	}
}
```
