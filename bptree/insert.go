package bptree

import "fmt"
import "os"
import "rand"
import "math"
import "log"
import . "block/byteslice"
import . "block/keyblock"
import "block/dirty"

func init() {
    if urandom, err := os.Open("/dev/urandom", os.O_RDONLY, 0666); err != nil {
        return
    } else {
        seed := make([]byte, 8)
        if _, err := urandom.Read(seed); err == nil {
            rand.Seed(int64(ByteSlice(seed).Int64()))
        }
    }
}

/*
   balance blocks takes two keyblocks full, and empty and balances the records between them. full must be full
   empty must be empty
*/
func (self BpTree) balance_blocks(full *KeyBlock, empty *KeyBlock) {
    n := int(full.MaxRecordCount())
    m := n >> 1
    // we have to subtract 1 from m if n is even because otherwise the blocks will not correctly
    // balance in this case, do some examples out on paper to really understand this.
    if n%2 == 0 {
        m -= 1
    }
    for j := n - 1; j > m; j-- {
        // move the records
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

        //move the pointers
        if p, ok := full.GetPointer(j); ok {
            empty.InsertPointer(0, p)
        }
        full.RemovePointer(j)
    }
}

func (self *BpTree) split(a *KeyBlock, rec *tmprec, nextb *KeyBlock, dirty *dirty.DirtyBlocks) (*KeyBlock, *tmprec, bool) {

    // this closure figures out which kind of block and record we need, either external or internal
    // we make this choice based on the mode of a because b+ trees have the property that the
    // allocated block always has the same node as the block to be split.
    b, r := func() (*KeyBlock, *Record) {
        if a.Mode() == self.external.Mode {
            return self.allocate(self.external), rec.external()
        }
        return self.allocate(self.internal), rec.internal()
    }()
    dirty.Insert(b)

    // This dependent block finds which record or split the block on.
    // it also identifies which the record should point at
    var split_rec *Record
    var nextp ByteSlice
    {
        findm := func() int {
            getk := func(i int) ByteSlice {
                r, _, _, _ := a.Get(i)
                return r.GetKey()
            }
            n := int(a.MaxRecordCount()) + 1
            m := n >> 1
            key := getk(m)
            i, _, _, _, _ := a.Find(key)
            j := i
            for ; j < n-1 && getk(j).Eq(key); j++ { }
            j--
            ir := math.Fabs(float64(i)/float64(n))
            jr := math.Fabs(float64(j)/float64(n))
            if ir <= jr { return i }
            return j
        }
        i, _, _, _, _ := a.Find(r.GetKey())
        m := findm()

        if m > i {
            // the mid point is after the spot where we would insert the key so we take the record
            // just before the mid point as our new record would shift the mid point over by 1
            split_rec, nextp, _, _ = a.Get(m - 1)
            a.RemoveAtIndex(m - 1)
            a.RemovePointer(m - 1)
        } else if m < i {
            // the mid point is before the new record so we can take the record at the mid point as
            // the split record.
            split_rec, nextp, _, _ = a.Get(m)
            a.RemoveAtIndex(m)
            a.RemovePointer(m)
        }

        if i == m {
            // the mid point is where the new record would go so in this case the new record will be
            // the split record for this split, and the nextb (which is associate with our new key)
            // become the nextp pointer
            split_rec = r
            if nextb != nil { nextp = nextb.Position() }
        } else {
            // otherwise we now need to insert our new record into the block before it is balanced
            if i, ok := a.Add(r); !ok {
                log.Exit("Inserting record into block failed PANIC")
            } else {
                // after it is inserted we need to associate the next block with its key (which is
                // the key we just inserted).
                if nextb != nil {
                    a.InsertPointer(i, nextb.Position())
                    nextb = nil
                }
            }
        }
    }
    self.balance_blocks(a, b)

    var return_rec *Record = split_rec
    var block *KeyBlock

    // choose which block to insert the record into
    if a.MaxRecordCount()%2 == 0 {
        // if the btree is of even order we want to randomly choose block so it will be balanced
        // over time. if we always choose in the sameway the tree will be unbalanced over time.
        f := rand.Float()
        if f > 0.5 {
            block = a
            if rec, _, _, ok := b.Get(0); !ok {
                log.Exit("Could not get the first record from block b PANIC")
            } else {
                // we change the record returned because the first record in block b will now not
                // be the record we split on which is ok we just need to return the correct record.
                return_rec = rec
            }
        } else {
            block = b
        }
    } else {
        block = b
    }

    // add the record to the block
    if i, ok := block.Add(split_rec); !ok {
        log.Exit("Could not add the split_rec to the selected block PANIC")
    } else {
        // we now insert the pointer if we have one
        if block.Mode()&POINTERS == POINTERS && nextp != nil {
            // we have a block that supports a pointer and a pointer
            if !block.InsertPointer(i, nextp) {

            }
        } else if block.Mode()&POINTERS == 0 && nextp != nil {
            // we don't have a block that supports a pointer but we do have a pointer
            log.Exit("tried to set a pointer on a block with no pointers")
        } else if block.Mode()&POINTERS == POINTERS && nextp == nil {
            // we have a block that supports a pointer but don't have a pointer
            log.Exit("splitting an internal block split requires a next block to point at")
        } // else
        //    we have a block that doesn't support a pointer and we don't have pointer
    }

    // if we have an external node (leaf) then we need to hook up the pointers between the leaf
    // nodes to support range queries and duplicate keys
    if a.Mode() == self.external.Mode {
        tmp, _ := a.GetExtraPtr()
        b.SetExtraPtr(tmp)
        a.SetExtraPtr(b.Position())
    }

    return b, rec_to_tmp(self, return_rec), true
}

func (self *BpTree) insert(block *KeyBlock, rec *tmprec, height int, dirty *dirty.DirtyBlocks) (*KeyBlock, *tmprec, bool) {
    // function to take a tmprec and turn it into the appropriate type of *Record
    _convert := func(rec *tmprec) *Record {
        if block.Mode() == self.external.Mode {
            return rec.external()
        }
        return rec.internal()
    }
    r := _convert(rec)

    // nextb is the block to be passed up the the caller in the case of a split at this level.
    var nextb *KeyBlock

    if height > 0 {
        // internal node
        // first we will need to find the next block to traverse down to
        var pos ByteSlice
        {
            // we find where in the block this key would be inserted
            i, _, _, _, _ := block.Find(rec.key)

            if i == 0 {
                // if that spot is zero it means that it is less than the smallest key the block
                // so we adjust the block appropriately
                if r, p, _, ok := block.Get(i); ok {
                    dirty.Insert(block)
                    r.SetKey(rec.key)
                    pos = p
                } else {
                    log.Exitf("227 Error could not get record %v from block %v", i, block)
                }
            } else {
                // else this spot is one to many so we get the previous spot
                i--
                if _, p, _, ok := block.Get(i); ok {
                    pos = p
                } else {
                    log.Exitf("235 Error could not get record %v from block %v", i, block)
                }
            }
        }

        // if pos is nil we have a serious problem
        if pos == nil {
            log.Exit("242 Nil Pointer")
        }

        // after we have found the position we get the block
        // then make a recursive call to insert to insert the record into the next block
        if b, srec, s := self.insert(self.getblock(pos), rec, height-1, dirty); s {
            // if the next block split we will insert the key passed up the chain.
            nextb = b
            r = _convert(srec)
            rec = srec
        } else {
            return nil, nil, false
        }
    } else {
        c := block.Count(rec.key)
        ratio := float(c) / float(block.MaxRecordCount())
        if c > 1 && block.Full() || ratio > .5 {
            fmt.Println("Magic Heres Abouts")
        }
    }
    // this block is changed
    dirty.Insert(block)
    if i, ok := block.Add(r); ok {
        // Block isn't full record inserted, now insert pointer (if one exists)
        // return to parent saying it has nothing to do
        if block.Mode()&POINTERS == POINTERS && nextb != nil {
            if ok := block.InsertPointer(i, nextb.Position()); !ok {
                log.Exit("pointer insert failed")
            }
        } else if block.Mode()&POINTERS == 0 && nextb != nil {
            log.Exit("tried to set a pointer on a block with no pointers")
        }
        return nil, nil, false
    }
    // Block is full split the block
    return self.split(block, rec, nextb, dirty)
}

func (self *BpTree) Insert(key ByteSlice, record []ByteSlice) bool {
    dirty := dirty.New(self.info.Height() * 4)

    // package the temp rec
    rec, valid := pkg_rec(self, key, record)
    if !valid {
        fmt.Println("key or record not valid")
        return false
    }

    // insert the block if split is true then we need to split the root
    if b, r, split := self.insert(self.getblock(self.info.Root()), rec, self.info.Height()-1, dirty); split {
        // This is where the root split goes.

        // we have to sync the blocks back because the first key in the root will have been
        // modified if the key we inserted was less than any key in the b+ tree
        dirty.Sync()

        // we get the oldroot so we can get the first key from it, this key becomes the first key in
        // the new root.
        oldroot := self.getblock(self.info.Root())
        var first *tmprec
        if f, _, _, ok := oldroot.Get(0); ok {
            first = rec_to_tmp(self, f)
        }

        // first allocate a new root then insert the key record and the associated pointers
        root := self.allocate(self.internal) // the new root will always be an internal node
        dirty.Insert(root)

        // first we insert the first key from the old root into the new root and point it at the
        // old root
        if i, ok := root.Add(first.internal()); ok {
            root.InsertPointer(i, self.info.Root())
        } else {
            fmt.Println("Could not insert into empty block PANIC")
            os.Exit(2)
            return false
        }

        // then we point the split rec's key at the the split block
        if i, ok := root.Add(r.internal()); ok {
            root.InsertPointer(i, b.Position())
        } else {
            fmt.Println("Could not insert into empty block PANIC")
            os.Exit(2)
            return false
        }

        // don't forget to update the height of the tree and the root
        self.info.SetRoot(root.Position())
        self.info.SetHeight(self.info.Height() + 1)
    }
    // at the end of of the method sync back the dirty blocks
    dirty.Sync()
    return true
}
