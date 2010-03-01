package btree

import "testing"
import "fmt"
import "os"
import "block/file"
import . "block/keyblock"
import . "block/buffers"

var rec [][]byte = &([3][]byte{&[1]byte{1}, &[1]byte{1}, &[2]byte{1, 2}});

func testingNewBTree(blocksize uint32) (*BTree, bool) {
    filename := "test.btree"
    keysize := uint32(4)
    fields := &([3]uint32{1, 1, 2})
    self := new(BTree)
    self.height = 1
    // 4 MB buffer with a block size of 4096 bytes
    if bf, ok := file.NewBlockFile(filename, NewLFU(1000)); !ok {
        fmt.Println("could not create block file")
        return nil, false
    } else {
        self.bf = bf
    }
    file.OPENFLAG =  os.O_RDWR | os.O_CREAT
    if dim, ok := NewBlockDimensions(RECORDS|POINTERS, blocksize, keysize, 8, fields); !ok {
        fmt.Println("Block Dimensions invalid")
        return nil, false
    } else {
        self.node = dim
    }
    //     fmt.Println("keys per block", self.node.KeysPerBlock())
    if !self.bf.Open() {
        fmt.Println("Couldn't open file")
        return nil, false
    }
    if s, open := self.bf.Size(); open && s == 0 {
        // This is a new file the size is zero
        self.bf.Allocate(self.node.BlockSize)
        b, ok := NewKeyBlock(self.bf, self.node)
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
        self.root = b.Position()
    }
    return self, true
}

func makebtree(blocksize uint32) *BTree {
    btree, _ := testingNewBTree(blocksize)
    return btree
}

func cleanbtree(btree *BTree) {
    os.Remove(btree.Filename())
}

// this is commented out because i intend to play with the blocksize, to do so i need to ensure
// the test will not fail because of a miss aligned read or write so i disable O_DIRECT on linux
// func TestCreate(t *testing.T) {
//     fmt.Println("TestCreate")
//     self, ok := NewBTree("test.btree", 4, &([3]uint32{1, 1, 2}))
//     defer cleanbtree(self)
//     if !ok || self == nil {
//         t.Error("could not make a BTree")
//     }
// }

func TestAllocate(t *testing.T) {
    fmt.Println("\n------  TestAllocate  ------")
    self := makebtree(BLOCKSIZE)
    defer cleanbtree(self)
    
    k := self.allocate()
    if k == nil {
        t.Error("could not allocate a new block")
    }
    if !k.SerializeToFile() {
        t.Error("could not serialize a new block to file")
    }
}

func TestGetBlock(t *testing.T) {
    fmt.Println("\n\n\n------  TestGetBlock  ------")
    self := makebtree(BLOCKSIZE)
    defer cleanbtree(self)
    
    if self.getblock(self.root) == nil {
        t.Error("could not read the root block")
    }
}

func TestValidateKey(t *testing.T) {
    fmt.Println("\n\n\n------  TestValidateKey  ------")
    self := makebtree(BLOCKSIZE)
    defer cleanbtree(self)
    
    goodkey := &[4]byte{1,2,3,4}
    badkey := &[3]byte{1,2,3}
    
    if !self.ValidateKey(goodkey) {
        t.Error("valid key validated as bad")
    }
    if self.ValidateKey(badkey) {
        t.Error("invalid key validated")
    }
}

func TestValidateRecord(t *testing.T) {
    fmt.Println("\n\n\n------  TestValidateRecord  ------")
    self := makebtree(BLOCKSIZE)
    defer cleanbtree(self)
    
    goodrec := rec
    bacrec1 := &([3][]byte{&[2]byte{1,2}, &[1]byte{1}, &[2]byte{1, 2}});
    bacrec2 := &([3][]byte{&[2]byte{1,2}, &[1]byte{1}, &[2]byte{1, 2}});
    bacrec3 := &([2][]byte{&[1]byte{1}, &[2]byte{1, 2}});
    bacrec4 := &([3][]byte{&[1]byte{1}, &[1]byte{1}, &[1]byte{1}});
    bacrec5 := &([3][]byte{&[1]byte{}, &[1]byte{}, &[1]byte{}});
    
    if !self.ValidateRecord(goodrec) {
        t.Error("valid key validated as bad")
    }
    if self.ValidateRecord(bacrec1) {
        t.Error("invalid key validated")
    }
    if self.ValidateRecord(bacrec2) {
        t.Error("invalid key validated")
    }
    if self.ValidateRecord(bacrec3) {
        t.Error("invalid key validated")
    }
    if self.ValidateRecord(bacrec4) {
        t.Error("invalid key validated")
    }
    if self.ValidateRecord(bacrec5) {
        t.Error("invalid key validated")
    }
}
