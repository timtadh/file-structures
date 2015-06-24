package btree

import "fmt"
import "os"
import . "file-structures/block/keyblock"
import . "file-structures/block/byteslice"

func (self *BTree) allocate() *KeyBlock {
	b, ok := NewKeyBlock(self.bf, self.node)
	if !ok {
		fmt.Println("Could not allocate block PANIC")
		os.Exit(1)
	}
	return b
}

func (self *BTree) getblock(pos ByteSlice) *KeyBlock {
	cblock, ok := DeserializeFromFile(self.bf, self.node, pos)
	if !ok {
		fmt.Println("Bad block pointer PANIC")
		os.Exit(7)
	}
	return cblock
}

func (self *BTree) ValidateKey(key ByteSlice) bool {
	return len(key) == int(self.node.KeySize)
}

func (self *BTree) ValidateRecord(record []ByteSlice) bool {
	if len(record) != len(self.node.RecordFields) {
		return false
	}
	r := true
	for i, field := range record {
		r = r && (int(self.node.RecordFields[i]) == len(field))
	}
	return r
}
