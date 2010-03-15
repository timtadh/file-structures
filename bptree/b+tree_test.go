package bptree

import "testing"
import "os"
import "fmt"
import "runtime"
import "treeinfo"
import . "block/file"
import . "block/keyblock"
import . "block/buffers"
import . "block/byteslice"
import "block/dirty"

var rec [][]byte = &([3][]byte{&[1]byte{1}, &[1]byte{1}, &[2]byte{1, 2}})
var BLOCKSIZE uint32 = treeinfo.BLOCKSIZE

func newBpTree(blocksize uint32, filename string, keysize uint32, fields []uint32) (*BpTree, bool) {
    self := new(BpTree)
    // 4 MB buffer with a block size of 4096 bytes
    if bf, ok := NewBlockFile(filename, NewLFU(1000)); !ok {
        fmt.Println("could not create block file")
        return nil, false
    } else {
        self.bf = bf
    }
    self.blocksize = blocksize
    if inter, ok := NewBlockDimensions(POINTERS|EQUAPTRS, self.blocksize, keysize, 8, nil); !ok {
        fmt.Println("Block Dimensions invalid")
        return nil, false
    } else {
        self.internal = inter
    }

    if leaf, ok := NewBlockDimensions(RECORDS|EXTRAPTR, self.blocksize, keysize, 8, fields); !ok {
        fmt.Println("Block Dimensions invalid")
        return nil, false
    } else {
        self.external = leaf
    }

    if !self.bf.Open() {
        fmt.Println("Couldn't open file")
        return nil, false
    }
    if s, open := self.bf.Size(); open && s == 0 {
        // This is a new file the size is zero
        self.bf.Allocate(self.blocksize)
        b, ok := NewKeyBlock(self.bf, self.external)
        if !ok {
            self.bf.Close()
            fmt.Println("Could not create the root block")
            return nil, false
        }
        if !b.SerializeToFile() {
            self.bf.Close()
            fmt.Println("Could not serialize root block to file")
            return nil, false
        }
        self.info = treeinfo.New(self.bf, 1, b.Position())
    } else {
        self.info = treeinfo.Load(self.bf)
    }
    runtime.SetFinalizer(self, func(self *BpTree) { self.bf.Close() })
    return self, true
}

func makebptree(size uint32, t *testing.T) *BpTree {
    self, ok := newBpTree(size, "test.bptree", 4, &([3]uint32{2, 2, 4}))
    if !ok {
        t.Fatal("could not create B+ Tree")
    }
    return self
}

func cleanbptree(self *BpTree) { os.Remove(self.bf.Filename()) }

func TestCreate(t *testing.T) {
    t.Log("------- TestCreate -------")
    self := makebptree(BLOCKSIZE, t)
    defer cleanbptree(self)
}

// TODO write tests for allocate, getblock, and finding the next block in an internal node
func TestAllocate(t *testing.T) {
    t.Log("------- TestAllocate -------")
    self := makebptree(BLOCKSIZE, t)
    defer cleanbptree(self)
    if self.allocate(self.internal) == nil {
        t.Error("Allocate returned nil for internal")
    }
    if self.allocate(self.external) == nil {
        t.Error("Allocate returned nil for external")
    }
}

func TestGetBlock(t *testing.T) {
    t.Log("------- TestGetBlock -------")
    self := makebptree(BLOCKSIZE, t)
    defer cleanbptree(self)
    dirty := dirty.New(self.info.Height() * 4)
    b1 := self.allocate(self.internal)
    b2 := self.allocate(self.external)
    dirty.Insert(b1)
    dirty.Insert(b2)
    dirty.Sync()

    b1_ := self.getblock(b1.Position())
    b2_ := self.getblock(b2.Position())
    if b1_ == nil || b2_ == nil {
        t.Error("getblock return nil")
    }
    b1s, _ := b1.Serialize()
    b2s, _ := b2.Serialize()
    b1_s, _ := b1_.Serialize()
    b2_s, _ := b2_.Serialize()

    if !ByteSlice(b1s).Eq(ByteSlice(b1_s)) {
        t.Error("block read from file is not the same as the one written out for b1")
    }
    if !ByteSlice(b2s).Eq(ByteSlice(b2_s)) {
        t.Error("block read from file is not the same as the one written out for b2")
    }
}

func TestFind(t *testing.T) {
    t.Log("------- TestFind -------")
    self := makebptree(BLOCKSIZE, t)
    defer cleanbptree(self)
    b := self.allocate(self.internal)

    if i, _, _, _, _ := b.Find(ByteSlice32(4)); i != 0 {
        t.Errorf("insert pos != to 0, i=%v\n%v\n", i, b)
    }

    b.Add(self.internal.NewRecord(ByteSlice32(2)))
    if i, _, _, _, _ := b.Find(ByteSlice32(0)); i != 0 {
        t.Errorf("insert pos != to 0, i=%v\n%v\n", i, b)
    }
    if i, _, _, _, _ := b.Find(ByteSlice32(4)); i != 1 {
        t.Errorf("Insert pos != to 1, i=%v\n%v\n", i, b)
    }
    if i, _, _, _, _ := b.Find(ByteSlice32(2)); i != 0 {
        t.Errorf("insert pos != to 0, i=%v\n%v\n", i, b)
    }

    b.Add(self.internal.NewRecord(ByteSlice32(6)))
    if i, _, _, _, _ := b.Find(ByteSlice32(0)); i != 0 {
        t.Errorf("insert pos != to 0, i=%v\n%v\n", i, b)
    }
    if i, _, _, _, _ := b.Find(ByteSlice32(4)); i != 1 {
        t.Errorf("Insert pos != to 1, i=%v\n%v\n", i, b)
    }
    if i, _, _, _, _ := b.Find(ByteSlice32(2)); i != 0 {
        t.Errorf("insert pos != to 0, i=%v\n%v\n", i, b)
    }
    if i, _, _, _, _ := b.Find(ByteSlice32(8)); i != 2 {
        t.Errorf("insert pos != to 1, i=%v\n%v\n", i, b)
    }
    if i, _, _, _, _ := b.Find(ByteSlice32(6)); i != 1 {
        t.Errorf("insert pos != to 1, i=%v\n%v\n", i, b)
    }

    b.Add(self.internal.NewRecord(ByteSlice32(4)))
    if i, _, _, _, _ := b.Find(ByteSlice32(0)); i != 0 {
        t.Errorf("insert pos != to 0, i=%v\n%v\n", i, b)
    }
    if i, _, _, _, _ := b.Find(ByteSlice32(4)); i != 1 {
        t.Errorf("Insert pos != to 1, i=%v\n%v\n", i, b)
    }
    if i, _, _, _, _ := b.Find(ByteSlice32(2)); i != 0 {
        t.Errorf("insert pos != to 0, i=%v\n%v\n", i, b)
    }
    if i, _, _, _, _ := b.Find(ByteSlice32(8)); i != 3 {
        t.Errorf("insert pos != to 1, i=%v\n%v\n", i, b)
    }
    if i, _, _, _, _ := b.Find(ByteSlice32(6)); i != 2 {
        t.Errorf("insert pos != to 1, i=%v\n%v\n", i, b)
    }
}
