package btree

import "fmt"
import "os"
import . "block/file"
import . "block/keyblock"
import . "block/buffers"
import . "block/byteslice"

// const BLOCKSIZE = 4096
const BLOCKSIZE = 45

type BTree struct {
    bf  *BlockFile
    node *BlockDimensions
    height int
}

// TODO: CREATE INFO BLOCK THAT SERIALIZES THE HEIGHT
func NewBTree(filename string, keysize uint32, fields []uint32) (*BTree, bool) {
    self := new(BTree)
    self.height = 1
         // 4 MB buffer with a block size of 4096 bytes
    if bf, ok := NewBlockFile(filename, NewLFU(1000)); !ok {
        fmt.Println("could not create block file")
        return nil, false
    } else { self.bf = bf }
    if dim, ok := NewBlockDimensions(RECORDS|POINTERS, BLOCKSIZE, keysize, 8, fields); !ok {
        fmt.Println("Block Dimensions invalid")
        return nil, false
    } else { self.node = dim }
    if !self.bf.Open() {
        fmt.Println("Couldn't open file")
        return nil, false
    }
    if s, open := self.bf.Size(); open && s == 0 {
        b, ok := NewKeyBlock(self.bf, self.node);
        if !ok {
            self.bf.Close()
            fmt.Println("Could not create the root block")
            return nil, false
        }
        if !b.SerializeToFile() {
            self.bf.Close()
            fmt.Println("Could not serialize root block to file")
            return nil, false
        }
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

func (self *BTree) find_block(key, pos ByteSlice, path []ByteSlice) (*KeyBlock, []ByteSlice) {
    var cblock *KeyBlock
    var ok bool

    path = path[0:len(path)+1]
    path[len(path)-1] = pos

    if cblock, ok = DeserializeFromFile(self.bf, self.node, pos); !ok {
        fmt.Println("Bad block pointer PANIC")
        os.Exit(1)
    }
    if _, rec, left, right, found := cblock.Find(key); found && rec.GetKey().Gt(key) {
        return self.find_block(key, left, path)
    } else if found {
        return self.find_block(key, right, path)
    }
    return cblock, path
}

func (self *BTree) Insert(key ByteSlice, record []ByteSlice) bool {
    if !self.ValidateKey(key) || !self.ValidateRecord(record) { return false }
    block, path := self.find_block(key, ByteSlice64(0), make([]ByteSlice, self.height)[0:0])
    fmt.Println(path)
    rec := block.NewRecord(key)
    for i,f := range record {
        rec.Set(uint32(i), f)
    }
    r := false
    if block.Full() {
        i, _, _, _, _ := block.Find(key)
        if i == self.node.KeysPerBlock() { i = i-1 }
        fmt.Println(i)
    } else {
        _,r = block.Add(rec)
        fmt.Println(block)
        block.SerializeToFile()
    }
    return r
}

func (self *BTree) String() string {
    s := "BTree:\n{\n"
    if block, ok := DeserializeFromFile(self.bf, self.node, ByteSlice64(0)); ok {
        s += fmt.Sprintln(block)
    }
    s += "}"
    return s
}

