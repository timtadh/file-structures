package bptree

import "fmt"
import "os"
import . "block/byteslice"
import . "block/keyblock"

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

func (self *BpTree) Insert(key ByteSlice, record []ByteSlice) bool {
    return false
}
