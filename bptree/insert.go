package bptree

import "fmt"
import "os"
import . "block/byteslice"
import . "block/keyblock"
import "block/dirty"

type tmprec struct {
    key ByteSlice
    record []ByteSlice
}

func pkg_rec(key ByteSlice, rec []ByteSlice) (*tmprec, bool) {
    if !self.ValidateKey(key) || !self.ValidateRecord(record) {
        return nil, false
    }
    self := new(tmprec)
    self.key = key
    self.record = rec
    return self, true
}

/*
   balance blocks takes two keyblocks full, and empty and balances the records between them. full must be full
   empty must be empty
*/
func (self BpTree) balance_blocks(full *KeyBlock, empty *KeyBlock) {
    n := int(full.MaxRecordCount())
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
}

func (self *BpTree) insert(block *KeyBlock, rec *tmprec, height int, dirty *dirty.DirtyBlocks) (*KeyBlock, *Record, bool) {

    return nil, nil, false
}

func (self *BpTree) Insert(key ByteSlice, record []ByteSlice) bool {
    dirty := dirty.New(self.info.Height() * 4)

    // package the temp rec
    rec, valid := pkg_rec(key, record)
    if !valid { return false }


    // insert the block if split is true then we need to split the root
    if b, r, split := self.insert(self.getblock(self.info.Root()), rec, self.info.Height()-1, dirty); split {
        fmt.Println(b, r, split)
    }

    return false
}
