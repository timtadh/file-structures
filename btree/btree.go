package btree

import "fmt"
import . "block/file"
import . "block/keyblock"
import . "block/buffers"
import . "block/byteslice"

// func Init() {
//     Node, _  := BlockDimensions(RECORDS|POINTERS, 72, 8, 8, &([3]uint32{1, 1, 2}))
//     fmt.Println(Node)
// }

type BTree struct {
    bf  *BlockFile
    node *BlockDimensions
}

func NewBTree(filename string, keysize uint32, fields []uint32) (*BTree, bool) {
    self := new(BTree)
         // 4 MB buffer with a block size of 4096 bytes
    if bf, ok := NewBlockFile(filename, NewLFU(1000)); !ok {
        fmt.Println("could not create block file")
        return nil, false
    } else { self.bf = bf }
    if dim, ok := NewBlockDimensions(RECORDS|POINTERS, 4096, keysize, 8, fields); !ok {
        fmt.Println("Block Dimensions invalid")
        return nil, false
    } else { self.node = dim }
    if !self.bf.Open() {
        fmt.Println("Couldn't open file")
        return nil, false
    }
    return self, true
}

func (self *BTree) ValidateKey(key ByteSlice) bool {
    return len(key) == int(self.node.KeySize)
}

func (self *BTree) ValidateRecord(record []ByteSlice) bool {
    if len(record) != len(self.node.RecordFields) { return false }
    r := true
    for i, field := range record {
        r = r && (int(self.node.RecordFields[i]) == len(field))
    }
    return r
}

func (self *BTree) Insert(key ByteSlice, record []ByteSlice) {
}
