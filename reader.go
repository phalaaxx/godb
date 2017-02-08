package cdb

import (
	"bytes"
	"encoding/binary"
	"os"
)

/* Reader represents a constant hash database */
type Reader struct {
	File     *os.File
	Pointers [256]HashPointer
}

/* Lookup searches for a key in CDB database */
func (r *Reader) Get(Key string) (*string, error) {
	var err error

	// calculate hash and hash table
	hash := cdbhash([]byte(Key))
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
			var klen, vlen uint32
			if err = binary.Read(buffer, binary.LittleEndian, &klen); err != nil {
				return nil, err
			}
			if err = binary.Read(buffer, binary.LittleEndian, &vlen); err != nil {
				return nil, err
			}

			// read key and value data
			buffer = bytes.NewBuffer(make([]byte, klen+vlen))
			if _, err = r.File.Read(buffer.Bytes()); err != nil {
				return nil, err
			}

			// compare keys
			if buffer.String()[0:klen] != Key {
				// key mismatch, return
				return nil, nil
			}

			// return value
			Value := buffer.String()[klen : klen+vlen]
			return &Value, nil
		}
	}

	// not found
	return nil, nil
}

/* Open existing CDB database file for reading */
func Open(Name string) (cdb *Reader, err error) {
	cdb = new(Reader)

	if cdb.File, err = os.Open(Name); err != nil {
		return nil, err
	}

	// read pointers table data
	bbuf := make([]byte, 2048)
	if _, err = cdb.File.Read(bbuf); err != nil {
		return nil, err
	}

	// parse pointers table data into HashPointer structure
	buffer := bytes.NewBuffer(bbuf)
	if err = binary.Read(buffer, binary.LittleEndian, &cdb.Pointers); err != nil {
		return nil, err
	}

	// return cdb structure
	return cdb, nil
}

/* Lookup performs cdb open, operation and close in a single step */
func Lookup(name string, callback func(cdb *Reader) error) (err error) {
	var cdb *Reader

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
