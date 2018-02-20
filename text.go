package godb

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

/* TextWriter represents a constant hash database */
type TextWriter struct {
	File   *os.File
	Target string
}

/* TextCreate a new password database file */
func TextCreate(Name string, Mode os.FileMode) (pw *TextWriter, err error) {
	var FileDir, BaseName, FullName string
	pw = new(TextWriter)
	pw.Target = Name
	// get absolute path to Name
	if FullName, err = filepath.Abs(Name); err != nil {
		return nil, err
	}
	// get directory location of Name
	FileDir = filepath.Dir(FullName)
	// get filename of Name
	BaseName = filepath.Base(FullName)
	// open file for writing
	if pw.File, err = ioutil.TempFile(FileDir, BaseName); err != nil {
		return nil, err
	}
	// set file mode
	if err = pw.File.Chmod(Mode); err != nil {
		return nil, err
	}
	return pw, nil
}

/* Add a data line to text database */
func (c *TextWriter) Add(format string, data ...interface{}) (err error) {
	if _, err := fmt.Fprintf(c.File, format, data...); err != nil {
		return err
	}
	return nil
}

/* Rollback a pending transaction by removing data written */
func (c TextWriter) Rollback() (err error) {
	// remove database file
	if err = os.Remove(c.File.Name()); err != nil {
		return err
	}
	// close database without flush
	return c.File.Close()
}

/* Commit HashTable at the end of the file, PointerTable at
   the beginning of the database and finally close the file */
func (c TextWriter) Commit() (err error) {
	// close database file
	if err = c.File.Close(); err != nil {
		return err
	}
	// rename database file to target name
	return os.Rename(c.File.Name(), c.Target)
}

/* TextUpdate updates text file database if its modification time is older
   than the specified changed time by running callback to feed data */
func TextUpdate(database string, changed time.Time, callback func(*TextWriter) error) (err error) {
	// get database modification time
	var modTime time.Time
	if st, err := os.Stat(database); err == nil {
		modTime = st.ModTime()
	} else if !os.IsNotExist(err) {
		return err
	}
	// check if data has changed
	if !modTime.Before(changed) {
		return nil
	}
	// open database for writing
	var db *TextWriter
	if db, err = TextCreate(database, 0644); err != nil {
		return err
	}
	// run callback on the database
	if err = callback(db); err != nil {
		db.Rollback()
		return err
	}
	// commit and activate new database
	return db.Commit()
}
