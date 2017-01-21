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
	var db *CdbWriter
	if db, err = cdb.Create("test.cdb"); err != nil {
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
