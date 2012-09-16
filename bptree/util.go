package bptree

// import "os"
import "fmt"
import "runtime"
import . "file-structures/block/keyblock"
import . "file-structures/block/byteslice"

// Allocates a new key block. This isn't quite as convient as the method
// for BTrees as we have to tell it if we are allocating an internal or an
// external block.
func (self *BpTree) allocate(dim *BlockDimensions) *KeyBlock {
    if dim != self.external && dim != self.internal {
        panic("Cannot allocate a block that has dimensions that are niether the dimensions of internal or external nodes.")
    }
    block, ok := NewKeyBlock(self.bf, dim)
    if !ok {
        panic("Could not allocate block PANIC")
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
                msg := fmt.Sprint(
                    "Unable to deserialize block at position: ", pos)
                panic(msg)
            } else {
                return block
            }
        } else if bytes[0] == self.internal.Mode {
            if block, ok := Deserialize(self.bf, self.internal, bytes, pos); !ok {
                msg := fmt.Sprint(
                    "Unable to deserialize block at position: ", pos)
                panic(msg)
            } else {
                return block
            }
        } else {
            a,b,c,d := runtime.Caller(1)
            msg := fmt.Sprintf(
                "Block at position %v has an invalid mode\n%v\n%v\n%v\n%v\n", pos, a, b, c, d)
            panic(msg)
        }
    }
    msg := fmt.Sprint(
        "Error reading block at postion: ", pos)
    panic(msg)
    return nil
}

func (self *BpTree) ValidateKey(key ByteSlice) bool {
    // fmt.Fprintf(os.Stderr, "%v == %v\n", len(key), int(self.external.KeySize))
    return len(key) == int(self.external.KeySize)
}

func (self *BpTree) ValidateRecord(record []ByteSlice) bool {
    // fmt.Fprintf(os.Stderr, "%v == %v\n", len(record), len(self.external.RecordFields))
    if len(record) != len(self.external.RecordFields) {
        return false
    }
    r := true
    for i, field := range record {
        // fmt.Fprintf(os.Stderr, "%v == %v\n", len(field), int(self.external.RecordFields[i]))
        r = r && (int(self.external.RecordFields[i]) == len(field))
    }
    return r
}

