package cdb

// HashItem defines a HashTable item with
// hash sum and pointer to key->data pair
type HashItem struct {
	Hash     uint32
	Position uint32
}

// HashPointer defines a pointer to a HashTable
type HashPointer struct {
	Position uint32
	SlotsNum uint32
}
