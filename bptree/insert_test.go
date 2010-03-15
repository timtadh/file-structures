package bptree

import "testing"
// import "fmt"
import . "block/keyblock"
import . "block/byteslice"
import "block/dirty"

var record []ByteSlice = []ByteSlice(&[3][]byte{&[2]byte{1, 2}, &[2]byte{3, 4}, &[4]byte{5, 6, 7, 8}})

const ORDER_2_2 = 37
const ORDER_3_3 = 50
const ORDER_4_4 = 61
const ORDER_5_5 = 73

var sizes [4]uint32 = [4]uint32{ORDER_2_2, ORDER_3_3, ORDER_4_4, ORDER_5_5}

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

func test_split(i, n int, self *BpTree, dirty *dirty.DirtyBlocks, t *testing.T) {
    log := func(a, b *KeyBlock, r, split *tmprec, ok bool) {
        t.Logf("\nsplit info:\n{\nblock a:\n%v\n\nnew rec:\n%v\n\nblock b:\n%v\n\nsplit rec:\n%v\n\nsuccess: %v\n}\n", a, r, b, split, ok)
    }
    a := self.allocate(self.external)
    fill_block(self, a, t, i)
    if r, ok := pkg_rec(self, ByteSlice32(uint32(i)), record); ok {
        b, split, ok := self.split(a, r, nil, dirty)
        if b == nil {
            log(a, b, r, split, ok)
            t.Fatal("split returned a nil block")
        }
        if split == nil {
            log(a, b, r, split, ok)
            t.Fatal("split returned a nil record")
        }
        if ok == false {
            log(a, b, r, split, ok)
            t.Fatal("split failed")
        }
        if n%2 == 0 {
            if a.RecordCount()+1 != b.RecordCount() && a.RecordCount() != b.RecordCount()+1 {
                log(a, b, r, split, ok)
                t.Fatal("a or b has the incorrect number of keys")
            }
        } else {
            if a.RecordCount() != b.RecordCount() {
                log(a, b, r, split, ok)
                t.Fatal("a has does not have the same number of keys that b has")
            }
        }
        if first, _, _, ok := b.Get(0); ok {
            if !split.key.Eq(first.GetKey()) {
                log(a, b, r, split, ok)
                t.Fatal("the first key in b does not match the returned record")
            }
            n := int(a.RecordCount())
            if n > 0 {
                if last, _, _, ok := a.Get(n - 1); ok {
                    if first.GetKey().Lt(last.GetKey()) {
                        log(a, b, r, split, ok)
                        t.Fatal("the first key in b is less than the last key in a")
                    }
                } else {
                    log(a, b, r, split, ok)
                    t.Fatal("could not get the last record from a")
                }
            } else {
                log(a, b, r, split, ok)
                t.Fatal("a is empty")
            }
        } else {
            log(a, b, r, split, ok)
            t.Fatal("could not get the first record from b, ie b is empty!")
        }
        {
            i := 0
            for ; i < int(a.RecordCount()); i++ {
                r, _, _, ok := a.Get(i)
                if !ok {
                    t.Error("Error getting rec at index ", i)
                }
                if int(r.GetKey().Int32()) != i {
                    t.Errorf("116 Error key, %v, does not equal %v", r.GetKey().Int32(), i)
                }
            }

            if int(split.key.Int32()) != i {
                t.Errorf("121 Error key, %v, does not equal %v", split.key.Int32(), i)
            }

            for j := 0; j < int(b.RecordCount()); j++ {
                r, _, _, ok := b.Get(j)
                if !ok {
                    t.Error("Error getting rec at index ", i)
                }
                if int(r.GetKey().Int32()) != i {
                    t.Errorf("130 Error key, %v, does not equal %v", r.GetKey().Int32(), i)
                }
                i++
            }
        }
    } else {
        t.Error("could not create tmprec", i, record)
    }
}

func TestSplit(t *testing.T) {
    for _, size := range sizes {
        var n int
        {
            self := makebptree(size, t)
            n = int(self.external.KeysPerBlock())
            cleanbptree(self)
        }
        for i := 0; i <= n; i++ {
            self := makebptree(size, t)
            dirty := dirty.New(10)
            test_split(i, n, self, dirty, t)
            cleanbptree(self)
        }
    }
}

func make_complete(self *BpTree, skip int, t *testing.T) {
    dirty := dirty.New(10)
    n := int(self.external.KeysPerBlock())
    m := n * n
    if skip < m {
        m++
    }

    c := self.getblock(self.info.Root())
    root := self.allocate(self.internal)
    self.info.SetRoot(root.Position())
    dirty.Insert(c)
    dirty.Insert(root)

    first := 0
    if first == skip { first = 1 }

    r, _ := pkg_rec(self, ByteSlice32(uint32(first)), record)
    if p, ok := root.Add(r.internal()); ok {
        root.InsertPointer(p, c.Position())
    } else {
        t.Fatal("could not add a record to the root")
    }

    for i := 0; i < m; i++ {
        if i == skip { continue }
        r, _ := pkg_rec(self, ByteSlice32(uint32(i)), record)
        if c.Full() {
            c = self.allocate(self.external)
            dirty.Insert(c)
            if p, ok := root.Add(r.internal()); ok {
                root.InsertPointer(p, c.Position())
            } else {
                t.Fatal("could not add a record to the root")
            }
        }
        if _, ok := c.Add(r.external()); !ok {
            t.Fatal("could not add a record to the leaf")
        }
    }
    dirty.Sync()
}

func validate(self *BpTree, t *testing.T) {
    var i int = 0
    var walk func(*KeyBlock, ByteSlice)
    walk = func(block *KeyBlock, first ByteSlice) {
        if int32(first.Int32()) != -1 {
            if r, _, _, ok := block.Get(0); ok && !r.GetKey().Eq(first) {
                t.Log(self)
                t.Fatalf("first %v != %v", r.GetKey(), first)
            }
        }
        if block.Mode() == self.internal.Mode {
            for j := 0; j < int(block.RecordCount()); j++ {
                if r, p, _, ok := block.Get(j); ok {
                    walk(self.getblock(p), r.GetKey())
                } else {
                    t.Log(self)
                    t.Fatalf("Could not get record %v from block \n%v", j, block)
                }
            }
        } else {
            for j := 0; j < int(block.RecordCount()); j++ {
                if r, _, _, ok := block.Get(j); ok {
                    if !r.GetKey().Eq(ByteSlice32(uint32(i))) {
                        t.Log(self)
                        t.Fatalf("expected %v got %v", i, r.GetKey().Int32())
                    }
                } else {
                    t.Log(self)
                    t.Fatalf("Could not get record %v from block \n%v", j, block)
                }
                i++
            }
        }
    }
    walk(self.getblock(self.info.Root()), ByteSlice32(0xffffffff))
    n := self.external.KeysPerBlock()
    if n*n + 1 != i {
        t.Fatalf("too few keys in the b+tree expected %v got %v", n*n+1, i)
    }
}

func TestInsert(t *testing.T) {
    self := makebptree(ORDER_4_4, t)
    defer cleanbptree(self)
    i := 16
    make_complete(self, i, t)
    if ok := self.Insert(ByteSlice32(uint32(i)), record); !ok {
        t.Fatal("Insert returned false")
    }
    validate(self, t)
    //     t.Fail()
}
