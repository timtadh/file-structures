package btree

import "fmt"
import "os"
// import "container/list"
// import . "block/file"
import . "block/keyblock"
// import . "block/buffers"
import . "block/byteslice"

func (self *BTree) find_block(key, pos ByteSlice, path []ByteSlice) (*KeyBlock, []ByteSlice) {
    var cblock *KeyBlock
    var ok bool

    path = path[0 : len(path)+1]
    path[len(path)-1] = pos

    if cblock, ok = DeserializeFromFile(self.bf, self.node, pos); !ok {
        fmt.Println("Bad block pointer PANIC")
        os.Exit(1)
    }
    i, _, _, _, _ := cblock.Find(key)
    if i >= int(cblock.RecordCount()) {
        i = int(cblock.RecordCount()) - 1
    }
    if rec, left, right, ok := cblock.Get(i); ok && (rec.GetKey().Gt(key) || rec.GetKey().Eq(key)) && left != nil {
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


func (self *BTree) block_split(block *KeyBlock, path []ByteSlice, split_rec *Record, dirty *dirty_blocks) bool {
    
//     var pushup func(int) 
//     pushup = func(i int) {
//     
//     }
    
    return true
}

func (self BTree) balance_blocks(full *KeyBlock, empty *KeyBlock) {
    n := self.node.KeysPerBlock()
    m := n >> 1
    for j := n-1; j >= m; j-- {
        if r, _, _, ok := full.Get(j); !ok {
            fmt.Printf("could not get index j<%v> from block: %v", j, full)
            os.Exit(2)
            return
        } else {
            if !full.RemoveAtIndex(j) {
                fmt.Printf("could not remove index j<%v> from block: %v", j, full)
                os.Exit(2)
                return
            }
            empty.Add(r)
        }
        if j == n-1 {
            if p, ok := full.GetPointer(n); ok {
                empty.InsertPointer(0, p)
            }
        }
        if p, ok := full.GetPointer(j); ok {
            empty.InsertPointer(0, p)
        }
        full.RemovePointer(j)
    }
}

func (self *BTree) root_split(l_child *KeyBlock, split_rec *Record, dirty *dirty_blocks) bool {
    var root, r_child *KeyBlock
    var ok1, ok2 bool
    root, ok1 = NewKeyBlock(self.bf, self.node)
    r_child, ok2 = NewKeyBlock(self.bf, self.node)
    if !ok1 || !ok2 {
        fmt.Println("Could not allocate block PANIC")
        os.Exit(1)
    }
    dirty.insert(root)
    dirty.insert(r_child)
    self.root = root.Position()
    
    if i, ok := root.Add(split_rec); !ok {
        fmt.Printf("could not insert rec <%v> into block: %v", split_rec, root)
        os.Exit(2)
        return false
    } else {
        root.InsertPointer(i, l_child.Position())
        root.InsertPointer(i+1, r_child.Position())
    }
    
    self.balance_blocks(l_child, r_child)
    
    fmt.Println(root)
    fmt.Println(l_child)
    fmt.Println(r_child)
    self.height += 1
    return true
}

func (self *BTree) insert(block *KeyBlock, path []ByteSlice, rec *Record, dirty *dirty_blocks) (rblock *KeyBlock, i int, success bool) {
    key := rec.GetKey()
    cnode := len(path)-1

    if block.Full() {
        var split_rec *Record
        i, _, _, _, _ := block.Find(key)
        m := self.node.KeysPerBlock() >> 1
        if m != i {
            if i >= self.node.KeysPerBlock() {
                i--
            }
            split_rec, _, _, _ = block.Get(i)
            block.RemoveAtIndex(i)
            if _, ok := block.Add(rec); !ok {
                fmt.Println("Inserting record into block failed")
                return nil, -1, false
            }
        } else {
            split_rec = rec
        }
        fmt.Println(split_rec)

        if _, ok := self.parent(cnode, path); !ok {
            // we are at the root, and the root is full
            // so we need two more blocks one for the new right and the new left
            success = self.root_split(block, split_rec, dirty)
        } else {
            // we are not at the root we need to recursive split blocks until we reach a non-full
            // block. This will take some thinking ...
            success = self.block_split(block, path, split_rec, dirty)
        }
    } else {
        rblock = block
        i, success = block.Add(rec)
    }
    return
}

func (self *BTree) Insert(key ByteSlice, record []ByteSlice) bool {

    
    dirty := new_dirty_blocks(self.height * 4)

    if !self.ValidateKey(key) || !self.ValidateRecord(record) {
        return false
    }
    block, path := self.find_block(key, self.root, make([]ByteSlice, self.height)[0:0])
    dirty.insert(block)
    fmt.Println(path)

    rec := block.NewRecord(key)
    for i, f := range record {
        rec.Set(uint32(i), f)
    }
    _, _, r := self.insert(block, path, rec, dirty)
    dirty.sync()
    return r
}
