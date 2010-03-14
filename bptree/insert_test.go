package bptree

import "testing"
import . "block/keyblock"
import . "block/byteslice"
import "block/dirty"

var record []ByteSlice = []ByteSlice(&[3][]byte{&[1]byte{1}, &[1]byte{2}, &[2]byte{3,4}})

func insert(a *KeyBlock, key ByteSlice) bool {
    r := a.NewRecord(key)
    if a.Mode()&RECORDS == RECORDS {
        for i := uint32(0); i < r.Fields(); i++ {
            r.Set(i, record[i])
        }
    }
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
        a.InsertPointer(int(i-p_), ByteSlice64(uint64(i)))
    }
}

func TestInsert(t *testing.T) {
    self := makebptree(BLOCKSIZE, t)
    defer cleanbptree(self)
    for i := 0; i < int(self.internal.KeysPerBlock())+1; i++ {
        b := self.allocate(self.internal)
        fill_block(self, b, t, i)
    }
}

func TestSplit(t *testing.T) {
    var n int
    {
        self := makebptree(BLOCKSIZE, t)
        n = int(self.internal.KeysPerBlock())
        cleanbptree(self)
    }
    for i := 0; i <= n; i++ {
        self := makebptree(BLOCKSIZE, t)
        dirty := dirty.New(10)
        a := self.allocate(self.external)
        fill_block(self, a, t, i)
        if r, ok := pkg_rec(self, ByteSlice32(uint32(i)), record); ok {
            b, split, ok := self.split(a, r, nil, dirty)
            if b == nil {
                t.Fatal("split returned a nil block")
            }
            if split == nil {
                t.Fatal("split returned a nil record")
            }
            if ok == false {
                t.Error("split failed")
            }
            if n%2 == 0 {
                if a.RecordCount()+1 != b.RecordCount() && a.RecordCount() != b.RecordCount()+1 {
                    t.Error("a or b has the incorrect number of keys")
                }
            } else {
                if a.RecordCount() != b.RecordCount() {
                    t.Error("a has does not have the same number of keys that b has")
                }
            }
            if first, _, _, ok := b.Get(0); ok {
                if !split.key.Eq(first.GetKey()) {
                    t.Error("the first key in b does not match the returned record")
                }
                n := int(a.RecordCount())
                if n > 0 {
                    if last, _, _, ok := a.Get(n-1); ok {
                        if first.GetKey().Lt(last.GetKey()) {
                            t.Error("the first key in b is less than the last key in a")
                        }
                    } else {
                        t.Error("could not get the last record from a")
                    }
                } else {
                    t.Error("a is empty")
                }
            } else {
                t.Error("could not get the first record from b, ie b is empty!")
            }
            t.Logf("\nsplit info:\n{\nblock a:\n%v\n\nnew rec:\n%v\n\nblock b:\n%v\n\nsplit rec:\n%v\n\nsuccess: %v\n}\n", a, r, b, split, ok)
        } else {
            t.Error("could not create tmprec", i, record)
        }
        cleanbptree(self)
    }
}
