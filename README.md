cdb
===

This is a simple interface for Golang to constant database file format.  
Currently the interface only supports writing.

Usage
---

```go
var err error
var cdb *CdbWriter
if cdb, err = Create("test.cdb"); err != nil {
	log.Fatal(fmt.Errorf("Create: %v", err))
}
if err = cdb.Add("email@address.com", "alias@address.com"); err != nil {
	log.Fatal(fmt.Errorf("Add: %v", err))
}
if err = c.Close(); err != nil {
	log.Fatal(fmt.Errorf("Close: %v", err))
}
```
