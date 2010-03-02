package btree

import "fmt"
// import "os"
import "runtime"
import "container/list"
import . "block/file"
import . "block/keyblock"
import . "block/buffers"
import . "block/byteslice"


// const BLOCKSIZE = 4096
// const BLOCKSIZE = 45
const BLOCKSIZE = 65
// const BLOCKSIZE = 105

type container struct {
    file   *BlockFile
    height int
    root   ByteSlice
}

func new_container(file *BlockFile, h int, r ByteSlice) *container {
    self := new(container)
    self.file = file
    self.height = h
    self.root = r
    self.Serialize()
    return self
}
func load_container(file *BlockFile) *container {
    self := new(container)
    self.file = file
    self.deserialize()
    return self
}
func (self *container) Height() int { return self.height }
func (self *container) Root() ByteSlice { return self.root }
func (self *container) SetHeight(h int) { self.height = h; self.Serialize() }
func (self *container) SetRoot(r ByteSlice) { self.root = r; self.Serialize() }
func (self *container) Serialize() {
    bytes := make([]byte, BLOCKSIZE)
    h := ByteSlice32(uint32(self.height))
    i := 0
    for _, b := range h {
        bytes[i] = b
        i++
    }
    for _, b := range self.root {
        bytes[i] = b
        i++
    }
    self.file.WriteBlock(0, bytes)
}
func (self *container) deserialize() {
    bytes, ok := self.file.ReadBlock(0, BLOCKSIZE)
    if ok {
        self.height = int(ByteSlice(bytes[0:4]).Int32())
        self.root = ByteSlice(bytes[4:12])
    }
}

type BTree struct {
    bf     *BlockFile
    node   *BlockDimensions
    info   *container
}

// TODO: CREATE INFO BLOCK THAT SERIALIZES THE HEIGHT
func NewBTree(filename string, keysize uint32, fields []uint32) (*BTree, bool) {
    self := new(BTree)
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
        self.info = new_container(self.bf, 1, b.Position())
    } else {
        self.info = load_container(self.bf)
    }
    clean := func(self *BTree) {
        self.bf.Close()
    }
    runtime.SetFinalizer(self, clean)
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
    r := find(self.getblock(self.info.Root()), self.info.Height())
    if r == nil { return nil, false }
    return r, true
}

func (self *BTree) Filename() string { return self.bf.Filename() }

func (self *BTree) String() string {
    s := "BTree:\n{\n"
    stack := list.New()
    stack.PushBack(self.info.Root())
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
