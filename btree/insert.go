package btree

import "fmt"
import "os"
// import "container/list"
// import . "block/file"
import . "block/keyblock"
// import . "block/buffers"
import . "block/byteslice"

/*
    balance blocks takes two keyblocks full, and empty and balances the records between them. full must be full
    empty must be empty
*/
func (self BTree) balance_blocks(full *KeyBlock, empty *KeyBlock) {
    fmt.Println("FULL:\n", full)
    n := self.node.KeysPerBlock()
    m := n >> 1
    for j := n-1; j >= m; j-- {
        if r, _, _, ok := full.Get(j); !ok {
            fmt.Printf("could not get index j<%v> from block: %v", j, full)
            os.Exit(5)
            return
        } else {
            if !full.RemoveAtIndex(j) {
                fmt.Printf("could not remove index j<%v> from block: %v", j, full)
                os.Exit(5)
                return
            }
            empty.Add(r)
        }
        if p, ok := full.GetPointer(j+1); ok {
            empty.InsertPointer(0, p)
        }
        full.RemovePointer(j+1)
    }
    fmt.Println("   full:\n", full)
    fmt.Println("   emtpy:\n", empty)
}

/*
    split takes a block figures out how splits it and splits it between the two blocks, it passes back
    the splitting record, and the position of the new block
*/
func (self *BTree) split(block *KeyBlock, rec *Record, nextp ByteSlice, dirty *dirty_blocks) (ByteSlice, *Record, bool) {
    var split_rec *Record
    new_block := self.allocate()
    dirty.insert(new_block)
    i, _, _, _, _ := block.Find(rec.GetKey())
    m := self.node.KeysPerBlock() >> 1
    if m != i {
        if i >= self.node.KeysPerBlock() { i-- }
        split_rec, _, _, _ = block.Get(i)
        block.RemoveAtIndex(i)
        if _, ok := block.Add(rec); !ok {
            fmt.Println("Inserting record into block failed PANIC")
            os.Exit(3)
        }
    } else {
        split_rec = rec
    }
    self.balance_blocks(block, new_block)
    if nextp != nil {
        if i, _, _, _, ok := block.Find(rec.GetKey()); ok {
            block.InsertPointer(i+1, nextp)
        } else {
            i, _, _, _, _ := new_block.Find(rec.GetKey())
            new_block.InsertPointer(i+1, nextp)
        }
    }
    j, _, _, _, _ := new_block.Find(split_rec.GetKey())
    fmt.Println("split .... ", j)
    fmt.Println(new_block.Position(), split_rec, true)
    return new_block.Position(), split_rec, true
}

/*
    Recursively inserts the record based on Sedgewick's algorithm
*/
func (self *BTree) insert(block *KeyBlock, rec *Record, height int, dirty *dirty_blocks) (ByteSlice, *Record, bool) {
    fmt.Println("inserting", block, rec, height)
    var nextp ByteSlice
    if height > 0 {
        // at an interior node
        var pos ByteSlice
        {
            // we need to find the next block to search
            k := rec.GetKey()
            i, _, _, _, _ := block.Find(k) // find where the key would go in the block
            if i >= int(block.RecordCount()) { i-- } // is it after the last key?
            r, left, right, ok := block.Get(i) // get the record
            if ok && (r.GetKey().Gt(k) || r.GetKey().Eq(k)) && left != nil {
                pos = left // hey it goes on the left
            } else if ok && right != nil {
                pos = right // the right
            } else {
                fmt.Println("Bad block pointer in interior node PANIC, for real? ", ok)
                fmt.Println(block)
                os.Exit(4)
            }
        }
        // recursive insert call, s is true we a node split occured in the level below so we change our insert
        if p, r, s := self.insert(self.getblock(pos), rec, height-1, dirty); s {
            // a node split occured in the previous call
            // so we are going to use this new record now
            nextp = p
            rec = r
        } else {
            // no node split we return to the parent saying it has nothing to do
            return nil, nil, false
        }
    }
    // this block is changed
    dirty.insert(block)
    if i, ok := block.Add(rec); ok {
        // Block isn't full record inserted, now insert pointer (if one exists)
        // return to parent saying it has nothing to do
        if nextp != nil {
            block.InsertPointer(i+1, nextp)
        }
        return nil, nil, false
    }
    // Block is full split the block
    return self.split(block, rec, nextp, dirty)
}

func (self *BTree) Insert(key ByteSlice, record []ByteSlice) bool {
    dirty := new_dirty_blocks(self.height * 4) // this is our buffer of "dirty" blocks that we will write back at the end

    if !self.ValidateKey(key) || !self.ValidateRecord(record) {
        return false
    }
//     block, path := self.find_block(key, self.root, make([]ByteSlice, self.height)[0:0])
//     dirty.insert(block)

    // makes the record
    rec := self.node.NewRecord(key)
    for i, f := range record {
        rec.Set(uint32(i), f)
    }
    
    // insert the block if split is true then we need to split the root
    if b, r, split := self.insert(self.getblock(self.root), rec, self.height-1, dirty); split {
        // root split
        // first allocate a new root then insert the key record and the associated pointers
        root := self.allocate()
        dirty.insert(root)
        if i, ok := root.Add(r); ok {
            root.InsertPointer(i, self.root)
            root.InsertPointer(i+1, b)
        } else {
            fmt.Println("Could not insert into empty block PANIC")
            os.Exit(2)
            return false
        }
        // don't forget to update the height of the tree and the root
        self.root = root.Position()
        self.height += 1
    }
    dirty.sync() // writes the dirty blocks to disk
    return true
}
