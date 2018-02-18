package cdb

/* cdbHash calculates a cdb hash */
func cdbHash(data []byte) uint32 {
	h := uint32(5381)
	for _, r := range data {
		h = ((h << 5) + h) ^ uint32(r)
	}
	return h
}
