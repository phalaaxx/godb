package cdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// CdbWriter represents a constant hash database
type CdbWriter struct {
	File     *os.File
	Target   string
	Elements map[uint32][]HashItem
	Position uint32
}

// Create a new CDB database file
func Create(Name string) (cdb *CdbWriter, err error) {
	cdb = new(CdbWriter)
	cdb.Target = Name

	var FileDir string
	var BaseName string
	var FullName string

	// Get absolute path to Name
	if FullName, err = filepath.Abs(Name); err != nil {
		return nil, err
	}

	// Get directory location of Name
	FileDir = filepath.Dir(FullName)

	// Get filename of Name
	BaseName = filepath.Base(FullName)

	// Open file for writing
	if cdb.File, err = ioutil.TempFile(FileDir, BaseName); err != nil {
		return nil, err
	}
	cdb.Position = 2048

	// Reserve space for pointers table
	if _, err = cdb.File.Seek(int64(cdb.Position), os.SEEK_SET); err != nil {
		return nil, err
	}

	// Allocate memory for HashTable
	cdb.Elements = make(map[uint32][]HashItem)

	return cdb, nil
}

// Add a key-value pair to CDB database
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

	// Add data in hash table
	hash := cdbhash([]byte(Key))
	hashmod := hash % 256

	// make sure hashtable exists
	if _, ok := c.Elements[hashmod]; !ok {
		c.Elements[hashmod] = []HashItem{}
	}
	c.Elements[hashmod] = append(c.Elements[hashmod], HashItem{hash, c.Position})

	// Get next global position
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

// Write HashTable at the end of the file, PointerTable at
// the beginning of the database and finally close the file.
func (c CdbWriter) Commit() (err error) {
	var Pointers [256]HashPointer
	var hash uint32

	// Prepare a hash table map
	HashTable := make(map[uint32][]HashItem)
	buf := new(bytes.Buffer)
	for hash, _ := range c.Elements {
		ElementsLen := uint32(len(c.Elements[hash]))

		// Make empty hash table
		HashTable[hash] = make([]HashItem, ElementsLen*2)

		for _, item := range c.Elements[hash] {
			HashTableSlot := item.Hash / 256 % ElementsLen

			for slot := HashTableSlot; slot < ElementsLen*2; slot++ {
				if HashTable[hash][slot].Position == 0 && HashTable[hash][slot].Hash == 0 {
					HashTable[hash][slot] = item
					break
				}
			}
		}

		// Write hash table data to the buffer
		if err = binary.Write(buf, binary.LittleEndian, HashTable[hash]); err != nil {
			return err
		}
	}

	// Fill in pointers table
	for hash = 0; hash < 256; hash++ {
		Pointers[hash].Position = c.Position
		Pointers[hash].SlotsNum = 0

		if _, ok := HashTable[hash]; ok {
			slots := uint32(len(HashTable[hash]))
			Pointers[hash].SlotsNum = slots
			c.Position += slots * 8
		}
	}

	// Flush hash tables at
	if _, err = c.File.Write(buf.Bytes()); err != nil {
		return err
	}

	// Go to the beginning of the file
	if _, err = c.File.Seek(0, os.SEEK_SET); err != nil {
		return err
	}

	// Write pointers table
	if err = binary.Write(c.File, binary.LittleEndian, Pointers); err != nil {
		return err
	}

	// Close database
	if err = c.File.Close(); err != nil {
		return err
	}

	// Swap database files
	return os.Rename(c.File.Name(), c.Target)
}
