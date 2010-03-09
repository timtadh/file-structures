package bptree

import "fmt"
// import "os"
import "runtime"
import "container/list"
import "treeinfo"
import . "block/file"
import . "block/keyblock"
import . "block/buffers"
import . "block/byteslice"


type BpTree struct {
    blocksize uint32
    bf        *BlockFile
    internal  *BlockDimensions
    external  *BlockDimensions
    info      *treeinfo.TreeInfo
}

func NewBpTree(filename string, keysize uint32, fields []uint32) (*BpTree, bool) {
    self := new(BpTree)
    // 4 MB buffer with a block size of 4096 bytes
    if bf, ok := NewBlockFile(filename, NewLFU(1000)); !ok {
        fmt.Println("could not create block file")
        return nil, false
    } else {
        self.bf = bf
    }
    self.blocksize = treeinfo.BLOCKSIZE
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

func (self *BpTree) String() string {
    s := "B+Tree:\n{\n"
    stack := list.New()
    stack.PushBack(self.info.Root())
    for stack.Len() > 0 {
        e := stack.Front()
        pos := e.Value.(ByteSlice)
        stack.Remove(e)
        if block, ok := DeserializeFromFile(self.bf, self.external, pos); ok {
            s += fmt.Sprintln(block)
            for i := 0; i < int(block.PointerCount()); i++ {
                if p, ok := block.GetPointer(i); ok {
                    stack.PushBack(p)
                }
            }
        }
    }
    s += "}"
    return s
}
