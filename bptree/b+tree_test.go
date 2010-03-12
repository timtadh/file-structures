package bptree

import "testing"
import "os"
import "fmt"
import "treeinfo"

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
    fmt.Println(self)
}

// TODO write tests for allocate, getblock, and finding the next block in an internal node
// func TestAllocate(t *testing.T) {
//
// }
