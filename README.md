cdb
===

This is a simple interface for Golang to constant database (cdb) file format.  
The interface supports both reading and writing.

Writing new cdb database
---

```go
package main

import (
	"fmt"
	"github.com/phalaaxx/godb"
	"log"
)

func main() {
	var err error
	var db *Writer
	if db, err = godb.CdbCreate("test.cdb", 0644); err != nil {
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
	"github.com/phalaaxx/godb"
	"log"
	"time"
)

func main() {
	err := godb.CdbUpdate(
		"/etc/postfix/cdb/virtual-alias-maps.cdb",
		time.Now(),
		func(db *godb.CdbWriter) error {
			if err := db.Add("email@address.com", "alias@address.com"); err != nil {
				return err
			}
			return nil
		},
	)
	if err != nil {
		log.Fatal(fmt.Errorf("godb.CdbUpdate: %v", err))
	}
}
```

Reading from existing cdb database
---

The following example uses Lookup function which performs cdb file open, read
and close all in one single step. Second argument of Lookup is a callback
function which returns error and accepts one argument of type \*godb.CdbReader.

```go
package main

import (
	"fmt"
	"github.com/phalaaxx/godb"
	"log"
)

func main() {
	var value *string
    err := godb.CdbLookup(
		"virtual-alias-maps.cdb",
		func (db *godb.CdbReader) (err error) {
			if v, err = db.Get("email@address.com"); err != nil {
				return err
			}
			return nil
		},
	)
	if err != nil {
		log.Fatal(fmt.Errorf("godb.CdbLookup: %v", err))
	}
	// should print "alias@address.com"
	fmt.Println("Value: ", *value)
}
```
