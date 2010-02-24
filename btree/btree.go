package btree

import "fmt"
import "os"
import "container/list"
import . "block/file"
import . "block/keyblock"
import . "block/buffers"
import . "block/byteslice"

// const BLOCKSIZE = 4096
const BLOCKSIZE = 45

type dirty_blocks struct {
    slice []*KeyBlock
}
func new_dirty_blocks(size int) *dirty_blocks {
    self := new(dirty_blocks)
    self.slice = make([]*KeyBlock, size)[0:0]
    return self
}
func (self *dirty_blocks) insert(b *KeyBlock) {
    self.slice = self.slice[0:len(self.slice)+1]
    self.slice[len(self.slice)-1] = b
}
func (self *dirty_blocks) sync() {
    for _,b := range self.slice {
        b.SerializeToFile()
    }
}

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
    i, _, _, _, _ := cblock.Find(key);
    if i >= int(cblock.RecordCount()) { i = int(cblock.RecordCount())-1 }
    if rec, left, right, ok := cblock.Get(i); ok && rec.GetKey().Gt(key) && left != nil {
        fmt.Println("argh 0--->", key, rec, left, right, ok, i, cblock.RecordCount())
        return self.find_block(key, left, path)
    } else if ok && right != nil {
        fmt.Println("argh 1--->", key, rec, left, right, ok, i, cblock.RecordCount())
        return self.find_block(key, right, path)
    } else {
        fmt.Println("argh 2--->", key, rec, left, right, ok, i, cblock.RecordCount())
    }
    return cblock, path
}

func (self *BTree) root_split(block *KeyBlock, m int, split_rec *Record, dirty *dirty_blocks) bool {
    var l_child, r_child *KeyBlock
    var ok1, ok2 bool
    l_child, ok1 = NewKeyBlock(self.bf, self.node);
    r_child, ok2 = NewKeyBlock(self.bf, self.node);
    if !ok1 || !ok2 {
        fmt.Println("Could not allocate block PANIC")
        os.Exit(1)
    }
    dirty.insert(l_child)
    dirty.insert(r_child)

    for j := m-1; j >= 0; j-- {
        if r, _, _, ok := block.Get(j); !ok {
            fmt.Printf("could not get index j<%v> from block: %v", j, block)
            os.Exit(2)
            return false
        } else {
            if !block.RemoveAtIndex(j) {
                fmt.Printf("could not remove index j<%v> from block: %v", j, block)
                os.Exit(2)
                return false
            }
            l_child.Add(r)
        }
        if j == m-1 {
            if p, ok := block.GetPointer(m); ok {
                l_child.InsertPointer(0, p)
            }
        }
        if p, ok := block.GetPointer(j); ok {
            l_child.InsertPointer(0, p)
        }
        block.RemovePointer(j)
    }
    for block.RecordCount() > 0 {
        if r, _, _, ok := block.Get(0); !ok {
            fmt.Printf("could not get index j<%v> from block: %v", 0, block)
            os.Exit(2)
            return false
        } else {
            if !block.RemoveAtIndex(0) {
                fmt.Printf("could not remove index j<%v> from block: %v", 0, block)
                os.Exit(2)
                return false
            }
            r_child.Add(r)
        }
        if p, ok := block.GetPointer(0); ok {
            r_child.InsertPointer(int(r_child.PointerCount()), p)
        }
        block.RemovePointer(0)
        if block.RecordCount() == 0 {
            if p, ok := block.GetPointer(0); ok {
                r_child.InsertPointer(int(r_child.PointerCount()), p)
            }
            block.RemovePointer(0)
        }
    }
    if i, ok := block.Add(split_rec); !ok {
        fmt.Printf("could not insert rec <%v> into block: %v", split_rec, block)
        os.Exit(2)
        return false
    } else {
        block.InsertPointer(i, l_child.Position())
        block.InsertPointer(i+1, r_child.Position())
    }
    fmt.Println(block)
    fmt.Println(l_child)
    fmt.Println(r_child)
    self.height += 1
    return true
}

func (self *BTree) Insert(key ByteSlice, record []ByteSlice) bool {


    parent := func(i int, path []ByteSlice) (*KeyBlock, bool) {
        if i-1 < 0 { return nil, false }
        block, ok := DeserializeFromFile(self.bf, self.node, path[i-1]);
        if !ok {
            fmt.Println("Bad block pointer PANIC")
            os.Exit(1)
        }
        return block, true
    }
    dirty := new_dirty_blocks(self.height*4)

    if !self.ValidateKey(key) || !self.ValidateRecord(record) { return false }
    block, path := self.find_block(key, ByteSlice64(0), make([]ByteSlice, self.height)[0:0])
    dirty.insert(block)
    fmt.Println(path)
    cnode := len(path)-1

    rec := block.NewRecord(key)
    for i,f := range record {
        rec.Set(uint32(i), f)
    }

    r := false
    if block.Full() {
        var split_rec *Record
        i, _, _, _, _ := block.Find(key)
        m := self.node.KeysPerBlock() >> 1
        if m != i {
            if i >= self.node.KeysPerBlock() { i-- }
            split_rec, _, _, _ = block.Get(i)
            block.RemoveAtIndex(i)
            if _, ok := block.Add(rec); !ok {
                fmt.Println("Inserting record into block failed")
                return false
            }
        } else {
            split_rec = rec
        }
        fmt.Println(split_rec)

        if _, ok := parent(cnode, path); !ok {
            // we are at the root, and the root is full
            // so we need two more blocks one for the new right and the new left
            r = self.root_split(block, m, split_rec, dirty)
        } else {
            // we are not at the root we need to recursive split blocks until we reach a non-full
            // block. This will take some thinking ...
        }
    } else {
        _,r = block.Add(rec)
    }
    dirty.sync()
    return r
}

func (self *BTree) String() string {
    s := "BTree:\n{\n"
    stack := list.New()
    stack.PushBack(ByteSlice64(0))
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

