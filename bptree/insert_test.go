package bptree

import "testing"
import . "block/keyblock"
import . "block/byteslice"

func insert(a *KeyBlock, key ByteSlice) bool {
    r := a.NewRecord(key)
    _, b := a.Add(r)
    return b
}

func fill_block(self *BpTree, a *KeyBlock, t *testing.T, skip int) {
    n := int(a.MaxRecordCount())
    if skip < n {
        n++
    }
    p_ := uint32(0)
    for i := uint32(0); int(i) < n; i++ {
        if int(i) == skip {
            p_ = 1
            continue
        }
        if !insert(a, ByteSlice32(i)) {
            t.Errorf("failed inserting ith, %v, value in block of order %v", i+1, n)
        }
//         if i-p_ == 0 {
//             a.InsertPointer(int(i-p_), ByteSlice64(uint64(i-p_+1)))
//         }
        a.InsertPointer(int(i-p_), ByteSlice64(uint64(i)))
    }
    t.Log(a)
}

func TestInsert(t *testing.T) {
    self := makebptree(BLOCKSIZE, t)
    defer cleanbptree(self)
    for i := 0; i < int(self.internal.KeysPerBlock())+1; i++ {
        b := self.allocate(self.internal)
        fill_block(self, b, t, i)
    }
}
