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
//     fmt.Println("FULL:\n", full)
    n := self.node.KeysPerBlock()
    m := n >> 1
    for j := n - 1; j >= m; j-- {
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
        if p, ok := full.GetPointer(j + 1); ok {
            empty.InsertPointer(0, p)
        }
        full.RemovePointer(j + 1)
    }
    //     if full.PointerCount() == full. {
    //         if p, ok := full.GetPointer(m); ok {
    //             empty.InsertPointer(0, p)
    //         }
    //         full.RemovePointer(m)
    //     }
//     fmt.Println("   full:\n", full)
//     fmt.Println("   emtpy:\n", empty)
}

/*
   split takes a block figures out how to split it and splits it between the two blocks, it passes back
   the splitting record, and a pointer new block, and whether or not it succeeded
   nextb is the block that will be pointed at by one of the blocks
        ie. it was a block that was allocated by the previous split, normally a pointer to it would have
            been inserted into the block that is being split, but as that block is full it needs to go into
            one of the blocks here
        the function should always return a valid btree if the record it returns becomes the record at the root
        level.
*/
func (self *BTree) split(block *KeyBlock, rec *Record, nextb *KeyBlock, dirty *dirty_blocks) (*KeyBlock, *Record, bool) {
    var split_rec *Record
    new_block := self.allocate()
    dirty.insert(new_block)
    i, _, _, _, _ := block.Find(rec.GetKey())
    m := self.node.KeysPerBlock() >> 1
//     fmt.Println("m=", m)
    if m > i {
        split_rec, _, _, _ = block.Get(m - 1)
        block.RemoveAtIndex(m - 1)
        if _, ok := block.Add(rec); !ok {
            fmt.Println("Inserting record into block failed PANIC")
            os.Exit(3)
        }
    } else if m < i {
        split_rec, _, _, _ = block.Get(m)
        block.RemoveAtIndex(m)
        if _, ok := block.Add(rec); !ok {
            fmt.Println("Inserting record into block failed PANIC")
            os.Exit(3)
        }
    } else {
        split_rec = rec
    }
    self.balance_blocks(block, new_block)
    dirty.sync() // figure out how to remove
    if nextb != nil {
//         fmt.Println("NEXTB: ", nextb)
        nextr, _, _, _ := nextb.Get(0)
        if i, _, _, _, ok := block.Find(rec.GetKey()); ok {
            // if this pointer is going into the old block that means there will be too many pointers in this block
            // so we must move the last one over to the new block

            if p, ok := block.GetPointer(m); ok {
                new_block.InsertPointer(0, p)
            }
            block.RemovePointer(m)

            _, left, _, _ := block.Get(i)
//             fmt.Println("left=", left)
            lblock := self.getblock(left)
            r, _, _, _ := lblock.Get(0)
//             fmt.Println("empty")
//             fmt.Printf("nextr %v > %v r, %v\n", nextr.GetKey(), r.GetKey(), nextr.GetKey().Gt(r.GetKey()))
            if nextr.GetKey().Gt(r.GetKey()) {
//                 fmt.Println("i=", i, "+1")
                block.InsertPointer(i+1, nextb.Position())
            } else {
//                 fmt.Println("i=", i)
                block.InsertPointer(i, nextb.Position())
            }
        } else {
            i, _, _, _, _ := new_block.Find(rec.GetKey())
            _, left, _, _ := new_block.Get(i)
//             fmt.Println("left=", left)
            lblock := self.getblock(left)
            r, _, _, _ := lblock.Get(0)
//             fmt.Println("empty")
//             fmt.Printf("nextr %v > %v r, %v\n", nextr.GetKey(), r.GetKey(), nextr.GetKey().Gt(r.GetKey()))
            if nextr.GetKey().Gt(r.GetKey()) {
//                 fmt.Println("i=", i, "+1")
                new_block.InsertPointer(i+1, nextb.Position())
            } else {
//                 fmt.Println("i=", i)
                new_block.InsertPointer(i, nextb.Position())
            }
        }
    }
//     j, _, _, _, _ := new_block.Find(split_rec.GetKey())
//     fmt.Println("split .... ", j)
//     fmt.Println(new_block.Position(), split_rec, true)
    return new_block, split_rec, true
}

/*
   Recursively inserts the record based on Sedgewick's algorithm
*/
func (self *BTree) insert(block *KeyBlock, rec *Record, height int, dirty *dirty_blocks) (*KeyBlock, *Record, bool) {
//     fmt.Println("inserting", rec, "\n", block, height)
    var nextb *KeyBlock
    if height > 0 {
        // at an interior node
        var pos ByteSlice
        {
            // we need to find the next block to search
            k := rec.GetKey()
            i, _, _, _, _ := block.Find(k) // find where the key would go in the block
            if i >= int(block.RecordCount()) {
                i--
            } // is it after the last key?
            r, left, right, ok := block.Get(i) // get the record
            if ok && (k.Lt(r.GetKey())) && left != nil {
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
        if b, r, s := self.insert(self.getblock(pos), rec, height-1, dirty); s {
            // a node split occured in the previous call
            // so we are going to use this new record now
            nextb = b
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
        if nextb != nil {
            block.InsertPointer(i+1, nextb.Position())
        }
        return nil, nil, false
    }
    // Block is full split the block
    return self.split(block, rec, nextb, dirty)
}

func (self *BTree) Insert(key ByteSlice, record []ByteSlice) bool {
    dirty := new_dirty_blocks(self.info.Height() * 4) // this is our buffer of "dirty" blocks that we will write back at the end

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
    if b, r, split := self.insert(self.getblock(self.info.Root()), rec, self.info.Height()-1, dirty); split {
        // root split
        // first allocate a new root then insert the key record and the associated pointers
        root := self.allocate()
        dirty.insert(root)
        if i, ok := root.Add(r); ok {
            root.InsertPointer(i, self.info.Root())
            root.InsertPointer(i+1, b.Position())
        } else {
            fmt.Println("Could not insert into empty block PANIC")
            os.Exit(2)
            return false
        }
        // don't forget to update the height of the tree and the root
        self.info.SetRoot(root.Position())
        self.info.SetHeight(self.info.Height()+1)
    }
    dirty.sync() // writes the dirty blocks to disk
    return true
}
