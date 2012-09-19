package bptree

import "testing"
import "fmt"
import "math/rand"
import "os"
import . "file-structures/block/keyblock"
import . "file-structures/block/byteslice"
import "file-structures/block/dirty"

func init() {
    if urandom, err := os.Open("/dev/urandom"); err != nil {
        return
    } else {
        seed := make([]byte, 8)
        if _, err := urandom.Read(seed); err == nil {
            rand.Seed(int64(ByteSlice(seed).Int64()))
        }
    }
}

var record []ByteSlice = []ByteSlice{[]byte{1, 2}, []byte{3, 4}, []byte{5, 6, 7, 8}}

const ORDER_2_2 = 37
const ORDER_3_3 = 50
const ORDER_4_4 = 61
const ORDER_5_5 = 73

var sizes [5]uint32 = [5]uint32{ORDER_2_2, ORDER_3_3, ORDER_4_4, ORDER_5_5, 256}

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

func test_split(j, n int, self *BpTree, dirty *dirty.DirtyBlocks, t *testing.T) {
    log := func(a, b *KeyBlock, r, split *tmprec, ok bool) {
        t.Logf("\nsplit info:\n{\nblock a:\n%v\n\nnew rec:\n%v\n\nblock b:\n%v\n\nsplit rec:\n%v\n\nsuccess: %v\n}\n", a, r, b, split, ok)
    }
    a := self.allocate(self.internal)
    nextb := self.allocate(self.external)
    fill_block(self, a, t, j)
    if r, ok := pkg_rec(self, ByteSlice32(uint32(j)), record); ok {
        b, split, ok := self.split(a, r, nextb, dirty)
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
            // t.Log(a, b)
            i := 0
            for ; i < int(a.RecordCount()); i++ {
                r, p, _, ok := a.Get(i)
                if !ok {
                    t.Error("Error getting rec at index ", i)
                }
                if int(r.GetKey().Int32()) != i {
                    t.Errorf("112 Error key, %v, does not equal %v", r.GetKey().Int32(), i)
                }
                if !p.Eq(ByteSlice64(uint64(i))) && !(i == j && p.Eq(nextb.Position())) {
                    t.Errorf("115 Pointer, %v, does not equal %v", p, ByteSlice64(uint64(i)))
                }
                // t.Log(r.GetKey(), p)
            }

            if int(split.key.Int32()) != i {
                t.Errorf("121 Error key, %v, does not equal %v", split.key.Int32(), i)
            }

            for k := 0; k < int(b.RecordCount()); k++ {
                r, p, _, ok := b.Get(k)
                if !ok {
                    t.Error("Error getting rec at index ", i)
                }
                if int(r.GetKey().Int32()) != i {
                    t.Errorf("130 Error key, %v, does not equal %v", r.GetKey().Int32(), i)
                }
                if !p.Eq(ByteSlice64(uint64(i))) && !(i == j && p.Eq(nextb.Position())) {
                    t.Log(i, j, nextb.Position())
                    t.Errorf("115 Pointer, %v, does not equal %v", p, ByteSlice64(uint64(i)))
                }
                // t.Log(r.GetKey(), p)
                i++
            }
        }
    } else {
        t.Error("could not create tmprec", j, record)
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
    dirty := dirty.New(self.internal.KeysPerBlock()*3)
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
    self.info.SetHeight(2)
}

func validate(self *BpTree, expect int, t *testing.T) {
    var i int = 0
    var walk func(*KeyBlock, ByteSlice)
    walk = func(block *KeyBlock, first ByteSlice) {
        if int32(first.Int32()) != -1 {
            if r, _, _, ok := block.Get(0); ok && !r.GetKey().Eq(first) {
                t.Logf("first %v != %v", r.GetKey(), first)
                t.Log(block)
                t.Log(self)
                t.FailNow()
            }
        }
        if block.Mode() == self.internal.Mode {
            for j := 0; j < int(block.RecordCount()); j++ {
                if r, p, _, ok := block.Get(j); ok {
                    if r == nil {
                        t.Fatal("Nil record")
                    }
                    if p == nil {
//                         t.Log(self)
//                         t.Log(block)
//                         t.Log(r)
                        t.Fatal("Nil Pointer")
                    }
                    walk(self.getblock(p), r.GetKey())
                } else {
                    t.Logf("Could not get record %v from block \n%v", j, block)
                    t.Log(self)
                    t.FailNow()
                }
            }
        } else {
            for j := 0; j < int(block.RecordCount()); j++ {
                if r, _, _, ok := block.Get(j); ok {
                    if !r.GetKey().Eq(ByteSlice32(uint32(i))) {
                        t.Logf("expected %v got %v", i, r.GetKey().Int32())
                        t.Log(self)
                        t.FailNow()
                    }
                } else {
                    t.Logf("Could not get record %v from block \n%v", j, block)
                    t.Log(self)
                    t.FailNow()
                }
                i++
            }
        }
    }
    walk(self.getblock(self.info.Root()), ByteSlice32(0xffffffff))
    if expect != i {
        t.Log(self)
        t.Fatalf("too few keys in the b+tree expected %v got %v", expect, i)
    }
}

func TestInsert(t *testing.T) {
    fmt.Println("----------- Exhaustive Insert Test -----------")
    for _, size := range sizes {
        var i int
        {
            self := makebptree(size, t)
            i = self.internal.KeysPerBlock()
            cleanbptree(self)
        }

        n := (i)*(i)
        fmt.Printf("testing block size %v, b+ tree order %v, with %v tests\n", size, i, n)
        for i := 0; i < n; i++ {
            // t.Log(i)
            self := makebptree(size, t)
            make_complete(self, i, t)
            if ok := self.Insert(ByteSlice32(uint32(i)), record); !ok {
                t.Fatal("Insert returned false")
            }
            validate(self, n+1, t)
            cleanbptree(self)
        }
    }
}

func TestRandomBuild(t *testing.T) {
    fmt.Println("----------- Random Build -----------")
    for i,size := range sizes {
        if i == 4 { break }                                     // REMOVE THIS FOR REAL TESTS
        var order int
        {
            self := makebptree(size, t)
            order = self.internal.KeysPerBlock()
            cleanbptree(self)
        }
        n := order*order*(order+2)+1
        fmt.Printf("testing block size %v, b+ tree order %v, with %v inserts\n", size, order, n)
        for k := 0; k < 15; k++ {
            inserted := make(map[int] bool)
            self := makebptree(size, t)
            for i := 0; i < n; i++ {
                m := n
                j := rand.Intn(m)
                for _, ok := inserted[j]; ok; {
                    j = rand.Intn(m)
                    _, ok = inserted[j]
                }
                inserted[j] = true
                self.Insert(ByteSlice32(uint32(j)), record)
            }
            validate(self, n, t)
            records := self.Find(ByteSlice32(uint32(0)), ByteSlice32(uint32(n)))
            i := 0
            for rec := range records {
                if !rec.GetKey().Eq(ByteSlice32(uint32(i))) {
                    t.Errorf("329 Expected %v go %v", i, rec.GetKey().Int64())
                }
                i++
            }
            if self.Size() != self.compute_size() {
                t.Log(self)
                t.Fatalf("bptree.Size() != bptree.compute_size() %v got %v", self.Size(), self.compute_size())
            }
            cleanbptree(self)
        }
    }
}
/*
func TestDupSplitO3(t *testing.T) {
    tests := [4][3]int{[3]int{1, 1, 2},
                      [3]int{2, 2, 3},
                      [3]int{1, 2, 2},
                      [3]int{1, 3, 3},
    }
    for _,test := range tests {
        t.Error(test)
    }
}
*/

func TestDupSplitO4(t *testing.T) {
    tests := [][5]uint32{
                      [5]uint32{1, 2, 3, 4, 0}, // 0
                      [5]uint32{2, 3, 4, 5, 1}, // 1
                      [5]uint32{1, 3, 4, 5, 2}, // 2
                      [5]uint32{1, 2, 4, 5, 3}, // 3
                      [5]uint32{1, 2, 3, 5, 4}, // 4
                      [5]uint32{1, 2, 3, 4, 5}, // 5
                      [5]uint32{1, 1, 2, 3, 0}, // 6
                      [5]uint32{1, 1, 2, 3, 1}, // 6
                      [5]uint32{1, 1, 2, 3, 2}, // 7
                      [5]uint32{1, 1, 2, 3, 3}, // 8
                      [5]uint32{1, 1, 2, 3, 4}, // 8
                      [5]uint32{1, 1, 1, 2, 0}, // 9
                      [5]uint32{1, 1, 1, 2, 1}, // 10
                      [5]uint32{1, 1, 1, 2, 2}, // 9
                      [5]uint32{1, 1, 1, 2, 3}, // 9
                      [5]uint32{1, 1, 1, 3, 2}, // 9
                      [5]uint32{1, 1, 1, 3, 4}, // 9
                      [5]uint32{1, 2, 2, 3, 0}, // 11
                      [5]uint32{1, 2, 2, 3, 1}, // 11
                      [5]uint32{1, 2, 2, 3, 2}, // 12
                      [5]uint32{1, 2, 2, 3, 3}, // 13
                      [5]uint32{1, 2, 2, 3, 4}, // 13
                      [5]uint32{2, 2, 2, 3, 1}, // 14
                      [5]uint32{2, 2, 2, 3, 2}, // 14
                      [5]uint32{2, 2, 2, 3, 3}, // 15
                      [5]uint32{2, 2, 2, 3, 4}, // 15
                      [5]uint32{1, 2, 2, 2, 0}, // 16
                      [5]uint32{1, 2, 2, 2, 1}, // 16
                      [5]uint32{1, 2, 2, 2, 2}, // 17
                      [5]uint32{1, 2, 2, 2, 3}, // 17
                      [5]uint32{1, 2, 3, 3, 0}, // 18
                      [5]uint32{1, 2, 3, 3, 1}, // 18
                      [5]uint32{1, 2, 3, 3, 2}, // 19
                      [5]uint32{1, 2, 3, 3, 3}, // 20
                      [5]uint32{1, 2, 3, 3, 4}, // 20
                      [5]uint32{1, 3, 3, 3, 0}, // 21
                      [5]uint32{1, 3, 3, 3, 1}, // 21
                      [5]uint32{1, 3, 3, 3, 2}, // 21
                      [5]uint32{1, 3, 3, 3, 3}, // 22
                      [5]uint32{1, 3, 3, 3, 4}, // 22
                      [5]uint32{1, 1, 3, 3, 0}, // 23
                      [5]uint32{1, 1, 3, 3, 1}, // 23
                      [5]uint32{1, 1, 3, 3, 2}, // 23
                      [5]uint32{1, 1, 3, 3, 3},
                      [5]uint32{1, 1, 3, 3, 4}, // 23
    }
    for i,test := range tests {
//         fmt.Println("-------------->", i, test)
        self := makebptree(ORDER_4_4, t)
        for _,i := range test {
            self.Insert(ByteSlice32(i), record)
        }

        prev := ByteSlice32(0)
        results := self.Find(ByteSlice32(0), ByteSlice32(6))
        for result := range results {
            if prev.Gt(result.GetKey()) {
                t.Errorf("465 prev, %v, greater than current, %v.\n", prev, result.GetKey())
            }
            prev = result.GetKey()
        }

        if i < 10 {
            Dotty(fmt.Sprintf("bptree_0%v.dot", i), self)
        } else {
            Dotty(fmt.Sprintf("bptree_%v.dot", i), self)
        }
        cleanbptree(self)
//         fmt.Println("\n\n-----------------------------------------------------------------------\n\n\n\n")
    }
}

/*
func TestDupSplitO5(t *testing.T) {
    tests := [][5]int{[5]int{1, 1, 2, 3, 5},
                      [5]int{1, 1, 1, 4, 5},
                      [5]int{1, 1, 1, 1, 5},
                      [5]int{1, 2, 2, 3, 5},
                      [5]int{2, 2, 3, 4, 5},
                      [5]int{2, 2, 2, 4, 5},
                      [5]int{1, 2, 2, 2, 5},
                      [5]int{2, 2, 2, 2, 5},
                      [5]int{1, 2, 2, 2, 2},
                      [5]int{1, 3, 3, 4, 5},
                      [5]int{1, 2, 3, 3, 5},
                      [5]int{1, 3, 3, 3, 5},
                      [5]int{3, 3, 3, 3, 5},
                      [5]int{1, 3, 3, 3, 3},
                      [5]int{1, 2, 4, 4, 5},
                      [5]int{1, 4, 4, 4, 5},
                      [5]int{1, 4, 4, 4, 5},
    }
    for _,test := range tests {
        t.Error(test)
    }
}
*/
func TestRandomDuplicate(t *testing.T) {
    fmt.Println("----------- Test Random Duplicate -----------")
    for i, size := range sizes {
        if i == 0 { continue }
        var order int
        {
            self := makebptree(size, t)
            order = self.internal.KeysPerBlock()
            cleanbptree(self)
        }
        n := order*order*(order+2)
        top := 30
        if i == 4 { top = 1 }
        fmt.Printf("testing block size %v, b+ tree order %v, with %v inserts\n", size, order, n)
        for k := 0; k < top; k++ {
            inserted := make(map[int] bool)
            self := makebptree(size, t)
            for i := 0; i < n; i++ {
                m := n>>1
                j := rand.Intn(m)
                inserted[j] = true
                self.Insert(ByteSlice32(uint32(j)), record)
            }

            prev := ByteSlice32(0)
            results := self.Find(ByteSlice32(0), ByteSlice32(uint32(n)))
            for result := range results {
                if prev.Gt(result.GetKey()) {
                    t.Errorf("465 prev, %v, greater than current, %v.\n", prev, result.GetKey())
                }
                prev = result.GetKey()
            }
            if self.Size() != self.compute_size() {
                t.Log(self)
                t.Fatalf("bptree.Size() != bptree.compute_size() %v got %v", self.Size(), self.compute_size())
            }
            if i != 4 {
                if k < 10 {
                    Dotty(fmt.Sprintf("bptreerand_%v_0%v.dot", i, k), self)
                } else {
                    Dotty(fmt.Sprintf("bptreerand_%v_%v.dot", i, k), self)
                }
            }
            cleanbptree(self)
        }
    }
//     t.Fatal(i)
}
