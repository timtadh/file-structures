package bptree

// import "fmt"
// import "os"
import "log"
// import "container/list"
// import . "block/file"
import . "block/keyblock"
// import . "block/buffers"
import . "block/byteslice"

// TODO: update these functions for different types of B+ Tree blocks
// func (self *BpTree) allocate() *KeyBlock {
//     b, ok := NewKeyBlock(self.bf, self.node)
//     if !ok {
//         fmt.Println("Could not allocate block PANIC")
//         os.Exit(1)
//     }
//     return b
// }
//
// func (self *BpTree) getblock(pos ByteSlice) *KeyBlock {
//     cblock, ok := DeserializeFromFile(self.bf, self.node, pos)
//     if !ok {
//         fmt.Println("Bad block pointer PANIC")
//         os.Exit(7)
//     }
//     return cblock
// }

// This version of getblock needs to find out what kind of block
// it is getting. It does this by checking the mode of the block
// before deserialization thus we cannot use the convience method
// DeserializeFromFile
func (self *BpTree) getblock(pos ByteSlice) *KeyBlock {
    if bytes, read := self.bf.ReadBlock(pos.Int64(), self.blocksize); read {
        if bytes[0] == self.external.Mode {
            if block, ok := Deserialize(bf, dim, bytes, pos); !ok {
                log.Exit("Unable to deserialize block at position: ", pos)
            } else {
                return block
            }
        } else if bytes[0] == self.internal.Mode {

        } else {
            log.Exitf("Block at position %v has an invalid mode\n", pos)
        }
    }
    log.Exit("Error reading block at postion: ", pos)
}

func (self *BpTree) ValidateKey(key ByteSlice) bool {
    return len(key) == int(self.external.KeySize)
}

func (self *BpTree) ValidateRecord(record []ByteSlice) bool {
    if len(record) != len(self.external.RecordFields) {
        return false
    }
    r := true
    for i, field := range record {
        r = r && (int(self.external.RecordFields[i]) == len(field))
    }
    return r
}
