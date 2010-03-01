package btree

import "fmt"
// import "os"
import "container/list"
import . "block/file"
import . "block/keyblock"
import . "block/buffers"
import . "block/byteslice"

const BLOCKSIZE = 4096
// const BLOCKSIZE = 45
// const BLOCKSIZE = 65
// const BLOCKSIZE = 105


type BTree struct {
    bf     *BlockFile
    node   *BlockDimensions
    height int
    root   ByteSlice
}

// TODO: CREATE INFO BLOCK THAT SERIALIZES THE HEIGHT
func NewBTree(filename string, keysize uint32, fields []uint32) (*BTree, bool) {
    self := new(BTree)
    self.height = 1
    // 4 MB buffer with a block size of 4096 bytes
    if bf, ok := NewBlockFile(filename, NewLFU(1000)); !ok {
        fmt.Println("could not create block file")
        return nil, false
    } else {
        self.bf = bf
    }
    if dim, ok := NewBlockDimensions(RECORDS|POINTERS, BLOCKSIZE, keysize, 8, fields); !ok {
        fmt.Println("Block Dimensions invalid")
        return nil, false
    } else {
        self.node = dim
    }
    if !self.bf.Open() {
        fmt.Println("Couldn't open file")
        return nil, false
    }
    if s, open := self.bf.Size(); open && s == 0 {
        // This is a new file the size is zero
        self.bf.Allocate(self.node.BlockSize)
        b, ok := NewKeyBlock(self.bf, self.node)
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
        self.root = b.Position()
    }
    return self, true
}

func (self *BTree) Find(key ByteSlice) (*Record, bool) {
    var find func(*KeyBlock, int) *Record
    find = func(block *KeyBlock, ht int) *Record {
        i, rec, _, _, found := block.Find(key);
        if i >= int(block.RecordCount()) { i-- }
        r, left, right, ok := block.Get(i)
        if found {
            return rec
        } else if ht > 0 && ok && key.Lt(r.GetKey()) && left != nil {
            // its on the left
            return find(self.getblock(left), ht-1)
        } else if ht > 0 && ok && right != nil {
            return find(self.getblock(right), ht-1)
        }
        return nil
    }
    r := find(self.getblock(self.root), self.height)
    if r == nil { return nil, false }
    return r, true
}

func (self *BTree) Filename() string { return self.bf.Filename() }

func (self *BTree) String() string {
    s := "BTree:\n{\n"
    stack := list.New()
    stack.PushBack(self.root)
    for stack.Len() > 0 {
        e := stack.Front()
        pos := e.Value.(ByteSlice)
        stack.Remove(e)
        if block, ok := DeserializeFromFile(self.bf, self.node, pos); ok {
            s += fmt.Sprintln(block)
            for i := 0; i < int(block.PointerCount()); i++ {
                if p, ok := block.GetPointer(i); ok {
                    stack.PushBack(p)
                }
            }
        }
    }
    s += "}"
    return s
}
