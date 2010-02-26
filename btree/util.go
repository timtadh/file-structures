package btree


import "fmt"
import "os"
// import "container/list"
// import . "block/file"
import . "block/keyblock"
// import . "block/buffers"
import . "block/byteslice"

func (self *BTree) parent(i int, path []ByteSlice) (*KeyBlock, bool) {
    if i-1 < 0 {
        return nil, false
    }
    block, ok := DeserializeFromFile(self.bf, self.node, path[i-1])
    if !ok {
        fmt.Println("Bad block pointer PANIC")
        os.Exit(1)
    }
    return block, true
}

func (self *BTree) allocate() *KeyBlock {
    
    b, ok := NewKeyBlock(self.bf, self.node)
    if !ok {
        fmt.Println("Could not allocate block PANIC")
        os.Exit(1)
    }
    return b
}

func (self *BTree) getblock(pos ByteSlice) *KeyBlock {
    cblock, ok := DeserializeFromFile(self.bf, self.node, pos);
    if  !ok {
        fmt.Println("Bad block pointer PANIC")
        os.Exit(1)
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

type dirty_blocks struct {
    slice []*KeyBlock
}

func new_dirty_blocks(size int) *dirty_blocks {
    self := new(dirty_blocks)
    self.slice = make([]*KeyBlock, size)[0:0]
    return self
}
func (self *dirty_blocks) insert(b *KeyBlock) {
    self.slice = self.slice[0 : len(self.slice)+1]
    self.slice[len(self.slice)-1] = b
}
func (self *dirty_blocks) sync() {
    for _, b := range self.slice {
        b.SerializeToFile()
    }
}
