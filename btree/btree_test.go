package btree

import "testing"
import "fmt"
import "os"

var rec [][]byte = &([3][]byte{&[1]byte{1}, &[1]byte{1}, &[2]byte{1, 2}});

func makebtree() *BTree {
    btree, _ := NewBTree("test.btree", 4, &([3]uint32{1, 1, 2}))
    return btree
}

func cleanbtree(btree *BTree) {
    os.Remove(btree.Filename())
}

func TestCreate(t *testing.T) {
    fmt.Println("TestCreate")
    btree := makebtree()
    defer cleanbtree(btree)
}

func TestAllocate(t *testing.T) {
    fmt.Println("TestAllocate")
    btree := makebtree()
    defer cleanbtree(btree)
    
    k := btree.allocate()
    if k == nil {
        t.Error("could not allocate a new block")
    }
    if !k.SerializeToFile() {
        t.Error("could not serialize a new block to file")
    }
}
