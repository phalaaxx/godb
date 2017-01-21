package cdb

/* cdbhash calculates a cdb hash */
func cdbhash(data []byte) uint32 {
	h := uint32(5381)
	for _, r := range data {
		h = (h + (h << 5)) & 0xFFFFFFFF
		h ^= uint32(r)
	}
	return h
}
