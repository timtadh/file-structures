package bptree

import "testing"
import "os"
// import "fmt"
import "treeinfo"
import . "block/byteslice"
import "block/dirty"

var rec [][]byte = &([3][]byte{&[1]byte{1}, &[1]byte{1}, &[2]byte{1, 2}})
var BLOCKSIZE uint32 = treeinfo.BLOCKSIZE

func makebptree(size uint32, t *testing.T) *BpTree {
    self, ok := NewBpTree("test.bptree", 4, &([3]uint32{1, 1, 2}))
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
