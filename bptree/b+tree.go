package bptree

import "fmt"
// import "os"
import "log"
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

func (self *BpTree) Find(left ByteSlice, right ByteSlice) (<-chan *Record, chan<- bool) {
    records := make(chan *Record)
    ack := make(chan bool)

    go func(yield chan<- *Record, ack <-chan bool) {
        if left == nil || right == nil || (!left.Eq(right) && right.Lt(left)) {
            close(yield)
            close(ack)
            return
        }

        var find func(ByteSlice, *KeyBlock, int) (int, *KeyBlock, bool)
        find = func(key ByteSlice, block *KeyBlock, height int) (int, *KeyBlock, bool) {
            if height > 0 {
                var pos ByteSlice
                {
                    // we find where in the block this key would be inserted
                    i, r, p, _, ok := block.Find(key)

                    if i == 0 {
                        if ok && r.GetKey().Eq(key) {
                            pos = p
                        } else {
                            // this key can't be in the b+ tree
                            return  0, nil, false
                        }
                    } else {
                        // else this spot is one to many so we get the previous spot
                        i--
                        if _, p, _, ok := block.Get(i); ok {
                            pos = p
                        } else {
                            log.Exitf("235 Error could not get record %v from block %v", i, block)
                        }
                    }
                }
                return find(key, self.getblock(pos), height-1)
            }
            i, _, _, _, ok := block.Find(key)
            if !ok { return 0, nil, false }
            return i, block, true
        }
        i, block, ok := find(left, self.getblock(self.info.Root()), self.info.Height()-1)
        if !ok {
            close(yield)
            close(ack)
            return
        }
        var returns func(int, *KeyBlock) bool
        returns = func(start int, block *KeyBlock) bool {
            i := start
            for ; i < int(block.RecordCount()); i++ {
                rec, _, _, ok := block.Get(i)
                if !ok { return false }
                if  rec.GetKey().Eq(left) ||
                    rec.GetKey().Eq(right) ||
                    (rec.GetKey().Gt(left) && rec.GetKey().Lt(right)) {
                        yield<-rec
                        <-ack
                } else {
                    return false
                }
            }
            return true
        }
        start := i
        for returns(start, block) {
            p, _ := block.GetExtraPtr()
            if p.Eq(ByteSlice64(0)) { break }
            block = self.getblock(p)
            start = 0
        }
        close(yield)
        close(ack)
        return;
    }(records, ack);
    return records, ack
}

func (self *BpTree) String() string {
    s := "B+Tree:\n{\n"
    stack := list.New()
    stack.PushBack(self.info.Root())
    for stack.Len() > 0 {
        e := stack.Front()
        pos := e.Value.(ByteSlice)
        stack.Remove(e)
        block := self.getblock(pos)
        s += fmt.Sprintln(block)
        for i := 0; i < int(block.PointerCount()); i++ {
            if p, ok := block.GetPointer(i); ok {
                stack.PushBack(p)
            }
        }
    }
    s += "}"
    return s
}
