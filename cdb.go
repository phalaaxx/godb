package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

/* HashItem defines a HashTable item with
   hash sum and pointer to key->data pair */
type HashItem struct {
	Hash     uint32
	Position uint32
}

/* HashPointer defines a pointer to a HashTable */
type HashPointer struct {
	Position uint32
	SlotsNum uint32
}

/* cdbHash calculates a cdb hash */
func cdbHash(data []byte) uint32 {
	h := uint32(5381)
	for _, r := range data {
		h = ((h << 5) + h) ^ uint32(r)
	}
	return h
}

/* CdbReader represents a constant hash database */
type CdbReader struct {
	File     *os.File
	Pointers [256]HashPointer
}

/* CdbLookup searches for a key in CDB database */
func (r *CdbReader) Get(Key string) (*string, error) {
	var err error
	// calculate hash and hash table
	hash := cdbHash([]byte(Key))
	table := r.Pointers[hash%256]
	// only read HashTable if SlotsNum is non-zero
	if table.SlotsNum == 0 {
		// not found, exit
		return nil, nil
	}
	// read hash table data from file
	buffer := bytes.NewBuffer(make([]byte, table.SlotsNum*8))
	if _, err = r.File.ReadAt(buffer.Bytes(), int64(table.Position)); err != nil {
		return nil, err
	}
	// parse hash table data to HashItem structures
	HashTable := make([]HashItem, table.SlotsNum)
	if err = binary.Read(buffer, binary.LittleEndian, HashTable); err != nil {
		return nil, err
	}
	// lookup hash in hash table
	StartSlot := hash / 256 % table.SlotsNum
	for slot := StartSlot; slot < table.SlotsNum; slot++ {
		if HashTable[slot].Hash == hash {
			// record found, seek to start of the record
			if _, err = r.File.Seek(int64(HashTable[slot].Position), 0); err != nil {
				return nil, err
			}
			// read key and value lengths from file
			buffer := bytes.NewBuffer(make([]byte, 8))
			if _, err = r.File.Read(buffer.Bytes()); err != nil {
				return nil, err
			}
			// parse key and value lengths
			var keyLen, valueLen uint32
			if err = binary.Read(buffer, binary.LittleEndian, &keyLen); err != nil {
				return nil, err
			}
			if err = binary.Read(buffer, binary.LittleEndian, &valueLen); err != nil {
				return nil, err
			}
			// read key and value data
			buffer = bytes.NewBuffer(make([]byte, keyLen+valueLen))
			if _, err = r.File.Read(buffer.Bytes()); err != nil {
				return nil, err
			}
			// compare keys
			if buffer.String()[0:keyLen] != Key {
				// key mismatch, return
				return nil, nil
			}
			// return value
			Value := buffer.String()[keyLen : keyLen+valueLen]
			return &Value, nil
		}
	}
	// not found
	return nil, nil
}

/* Open existing CDB database file for reading */
func Open(Name string) (cdb *CdbReader, err error) {
	cdb = new(CdbReader)
	if cdb.File, err = os.Open(Name); err != nil {
		return nil, err
	}
	// read pointers table data
	binaryBuffer := make([]byte, 2048)
	if _, err = cdb.File.Read(binaryBuffer); err != nil {
		return nil, err
	}
	// parse pointers table data into HashPointer structure
	buffer := bytes.NewBuffer(binaryBuffer)
	if err = binary.Read(buffer, binary.LittleEndian, &cdb.Pointers); err != nil {
		return nil, err
	}
	// return cdb structure
	return cdb, nil
}

/* CdbLookup performs cdb open, operation and close in a single step */
func CdbLookup(name string, callback func(cdb *CdbReader) error) (err error) {
	var cdb *CdbReader
	// open cdb database for reading
	if cdb, err = Open(name); err != nil {
		return err
	}
	defer cdb.File.Close()
	// run callback function
	if err = callback(cdb); err != nil {
		return err
	}
	return nil
}

/* CdbWriter represents a constant hash database */
type CdbWriter struct {
	File      *os.File
	Target    string
	HashTable [256][]HashItem
	Position  uint32
}

/* CdbCreate a new CDB database file */
func CdbCreate(Name string, Mode os.FileMode) (cdb *CdbWriter, err error) {
	var FileDir string
	var BaseName string
	var FullName string
	cdb = new(CdbWriter)
	cdb.Target = Name
	// get absolute path to Name
	if FullName, err = filepath.Abs(Name); err != nil {
		return nil, err
	}
	// get directory location of Name
	FileDir = filepath.Dir(FullName)
	// get filename of Name
	BaseName = filepath.Base(FullName)
	// open file for writing
	if cdb.File, err = ioutil.TempFile(FileDir, BaseName); err != nil {
		return nil, err
	}
	// set file mode
	if err = cdb.File.Chmod(Mode); err != nil {
		return nil, err
	}
	// set initial position
	cdb.Position = 2048
	// reserve space for pointers table
	if _, err = cdb.File.Seek(int64(cdb.Position), io.SeekStart); err != nil {
		return nil, err
	}
	return cdb, nil
}

/* Add a key-value pair to CDB database */
func (c *CdbWriter) Add(Key, Data string) (err error) {
	buf := new(bytes.Buffer)
	if err = binary.Write(buf, binary.LittleEndian, uint32(len(Key))); err != nil {
		return fmt.Errorf("key length: %v", err)
	}
	if err = binary.Write(buf, binary.LittleEndian, uint32(len(Data))); err != nil {
		return fmt.Errorf("data length: %v", err)
	}
	if err = binary.Write(buf, binary.LittleEndian, []byte(Key)); err != nil {
		return fmt.Errorf("key: %v", err)
	}
	if err = binary.Write(buf, binary.LittleEndian, []byte(Data)); err != nil {
		return fmt.Errorf("data: %v", err)
	}
	if _, err = c.File.Write(buf.Bytes()); err != nil {
		return err
	}
	// add data in hash table
	hash := cdbHash([]byte(Key))
	hashMod := hash % 256
	// update HashTable
	c.HashTable[hashMod] = append(
		c.HashTable[hashMod],
		HashItem{hash, c.Position},
	)
	// get next global position
	c.Position += uint32(len(Key)) + uint32(len(Data)) + 8
	return nil
}

/* Rollback a pending transaction by removing data written */
func (c CdbWriter) Rollback() (err error) {
	// remove database file
	if err = os.Remove(c.File.Name()); err != nil {
		return err
	}
	// close database without flush
	return c.File.Close()
}

/* Commit HashTable at the end of the file, PointerTable at
   the beginning of the database and finally close the file */
func (c CdbWriter) Commit() (err error) {
	var Pointers []HashPointer
	// prepare a hash table map
	buf := new(bytes.Buffer)
	for _, hash := range c.HashTable {
		slots := uint32(len(hash) + 1)
		// prepare pointers table item
		Pointers = append(
			Pointers,
			HashPointer{c.Position, slots},
		)
		if slots != 0 {
			// prepare ordered hash table
			HashTable := make([]HashItem, slots)
			for idx, h := range hash {
				slotPos := h.Hash / 256 % slots
				for i := slotPos; ; {
					if HashTable[i].Hash == 0 && HashTable[i].Position == 0 {
						HashTable[i] = hash[idx]
						break
					}
					i++
					if i == slots {
						i = 0
					}
				}
			}
			// write hash table to buffer
			if err = binary.Write(buf, binary.LittleEndian, HashTable); err != nil {
				return err
			}
			// calculate next position
			c.Position += slots * 8
		}
	}
	// flush hash tables at
	if _, err = c.File.Write(buf.Bytes()); err != nil {
		return err
	}
	// go to the beginning of the file
	if _, err = c.File.Seek(0, io.SeekStart); err != nil {
		return err
	}
	// write pointers table
	if err = binary.Write(c.File, binary.LittleEndian, Pointers); err != nil {
		return err
	}
	// close database
	if err = c.File.Close(); err != nil {
		return err
	}
	// swap database files
	return os.Rename(c.File.Name(), c.Target)
}

/* CdbUpdate updates cdb database if its modTime is older than the
   specified changed time by running callback to feed data */
func CdbUpdate(database string, changed time.Time, callback func(*CdbWriter) error) (err error) {
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
	var db *CdbWriter
	if db, err = CdbCreate(database, 0644); err != nil {
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
