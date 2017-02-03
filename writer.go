package cdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

/* CdbWriter represents a constant hash database */
type CdbWriter struct {
	File      *os.File
	Target    string
	HashTable [256][]HashItem
	Position  uint32
}

/* Create a new CDB database file */
func Create(Name string) (cdb *CdbWriter, err error) {
	cdb = new(CdbWriter)
	cdb.Target = Name

	var FileDir string
	var BaseName string
	var FullName string

	/* get absolute path to Name */
	if FullName, err = filepath.Abs(Name); err != nil {
		return nil, err
	}

	/* get directory location of Name */
	FileDir = filepath.Dir(FullName)

	/* get filename of Name */
	BaseName = filepath.Base(FullName)

	/* open file for writing */
	if cdb.File, err = ioutil.TempFile(FileDir, BaseName); err != nil {
		return nil, err
	}
	cdb.Position = 2048

	/* reserve space for pointers table */
	if _, err = cdb.File.Seek(int64(cdb.Position), os.SEEK_SET); err != nil {
		return nil, err
	}

	return cdb, nil
}

/* Add a key-value pair to CDB database */
func (c *CdbWriter) Add(Key, Data string) (err error) {
	buf := new(bytes.Buffer)

	if err = binary.Write(buf, binary.LittleEndian, uint32(len(Key))); err != nil {
		return fmt.Errorf("KeyLen: %v", err)
	}
	if err = binary.Write(buf, binary.LittleEndian, uint32(len(Data))); err != nil {
		return fmt.Errorf("DataLen: %v", err)
	}
	if err = binary.Write(buf, binary.LittleEndian, []byte(Key)); err != nil {
		return fmt.Errorf("Key: %v", err)
	}
	if err = binary.Write(buf, binary.LittleEndian, []byte(Data)); err != nil {
		return fmt.Errorf("Data: %v", err)
	}

	if _, err = c.File.Write(buf.Bytes()); err != nil {
		return err
	}

	/* add data in hash table */
	hash := cdbhash([]byte(Key))
	hashmod := hash % 256

	/* update hashtable */
	c.HashTable[hashmod] = append(
		c.HashTable[hashmod],
		HashItem{hash, c.Position},
	)

	/* get next global position */
	c.Position += uint32(len(Key)) + uint32(len(Data)) + 8

	return nil
}

/* Rollback a pending transaction by removing data written */
func (c CdbWriter) Rollback() (err error) {
	/* remove database file */
	if err = os.Remove(c.File.Name()); err != nil {
		return err
	}

	/* close database without flush */
	return c.File.Close()
}

/* Commit HashTable at the end of the file, PointerTable at
   the beginning of the database and finally close the file */
func (c CdbWriter) Commit() (err error) {
	var Pointers []HashPointer

	/* prepare a hash table map */
	buf := new(bytes.Buffer)

	for _, hash := range c.HashTable {
		slots := uint32(len(hash) * 2)

		/* prepare pointers table item */
		Pointers = append(
			Pointers,
			HashPointer{c.Position, slots},
		)

		if slots != 0 {
			/* prepare ordered hash table */
			HashTable := make([]HashItem, slots)
			for idx, h := range hash {
				slotpos := h.Hash / 256 % slots
				for i := slotpos; i < slots; i++ {
					if HashTable[i].Hash == 0 && HashTable[i].Position == 0 {
						HashTable[i] = hash[idx]
						break
					}
				}
			}

			/* write hash table to buffer */
			if err = binary.Write(buf, binary.LittleEndian, HashTable); err != nil {
				return err
			}

			/* calculate next position */
			c.Position += slots * 8
		}
	}

	/* flush hash tables at */
	if _, err = c.File.Write(buf.Bytes()); err != nil {
		return err
	}

	/* go to the beginning of the file */
	if _, err = c.File.Seek(0, os.SEEK_SET); err != nil {
		return err
	}

	/* write pointers table */
	if err = binary.Write(c.File, binary.LittleEndian, Pointers); err != nil {
		return err
	}

	/* close database */
	if err = c.File.Close(); err != nil {
		return err
	}

	/* swap database files */
	return os.Rename(c.File.Name(), c.Target)
}

/* Update updates cdb database if its mtime  is older than the
   specified changed time by running callback to feed data */
func Update(database string, changed time.Time, callback func(*CdbWriter) error) (err error) {
	/* get database mtime */
	var mtime time.Time
	if st, err := os.Stat(database); err == nil {
		mtime = st.ModTime()
	} else if !os.IsNotExist(err) {
		return err
	}

	/* check if data has changed */
	if !mtime.Before(changed) {
		return nil
	}

	/* open database for writing */
	var db *CdbWriter
	if db, err = Create(database); err != nil {
		return err
	}

	/* run callback on the database */
	if err = callback(db); err != nil {
		db.Rollback()
		return err
	}

	/* commit and activate new database */
	return db.Commit()
}
