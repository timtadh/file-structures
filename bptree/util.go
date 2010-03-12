package bptree

// import "fmt"
// import "os"
import "log"
// import "container/list"
// import . "block/file"
import . "block/keyblock"
// import . "block/buffers"
import . "block/byteslice"

// Allocates a new key block. This isn't quite as convient as the method
// for BTrees as we have to tell it if we are allocating an internal or an
// external block.
func (self *BpTree) allocate(dim *BlockDimensions) *KeyBlock {
    if dim != self.external && dim != self.internal {
        log.Exit("Cannot allocate a block that has dimensions that are niether the dimensions of internal or external nodes.")
    }
    block, ok := NewKeyBlock(self.bf, dim)
    if !ok {
        log.Exit("Could not allocate block PANIC")
    }
    return block
}

// This version of getblock needs to find out what kind of block
// it is getting. It does this by checking the mode of the block
// before deserialization thus we cannot use the convience method
// DeserializeFromFile
func (self *BpTree) getblock(pos ByteSlice) *KeyBlock {
    if bytes, read := self.bf.ReadBlock(int64(pos.Int64()), self.blocksize); read {
        if bytes[0] == self.external.Mode {
            if block, ok := Deserialize(self.bf, self.external, bytes, pos); !ok {
                log.Exit("Unable to deserialize block at position: ", pos)
            } else {
                return block
            }
        } else if bytes[0] == self.internal.Mode {
            if block, ok := Deserialize(self.bf, self.internal, bytes, pos); !ok {
                log.Exit("Unable to deserialize block at position: ", pos)
            } else {
                return block
            }
        } else {
            log.Exitf("Block at position %v has an invalid mode\n", pos)
        }
    }
    log.Exit("Error reading block at postion: ", pos)
    return nil
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
