package bptree

import "fmt"
// import "os"
import "runtime"
import "container/list"
import "file-structures/treeinfo"
import . "file-structures/block/file"
import . "file-structures/block/keyblock"
import . "file-structures/block/buffers"
import . "file-structures/block/byteslice"


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
    if bf, ok := NewBlockFile(filename, NewLFU(15360)); !ok {
        fmt.Println("could not create block file")
        return nil, false
    } else {
        self.bf = bf
    }
    self.blocksize = treeinfo.BLOCKSIZE
    if inter, ok := NewBlockDimensions(POINTERS|EQUAPTRS|NODUP, self.blocksize, keysize, 8, nil); !ok {
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

/*
get all the records between the left key and the right key
Usage:
    records, ack := bptree.Find(ByteSlice64(1), ByteSlice64(15))
    for record := range records {
        do something with the record
        ack<-true;                              // ack<-true must be the last line of the loop.
    }
*/
func (self *BpTree) Find(left ByteSlice, right ByteSlice) (<-chan *Record, chan<- bool) {
    records := make(chan *Record)
    ack := make(chan bool)

    // Go Routine which finds and returns the records
    go func(yield chan<- *Record, ack <-chan bool) {
        // parameters are invalid or will yield the empty set
        if left == nil || right == nil || (!left.Eq(right) && right.Lt(left)) {
            close(yield)
            return
        }

        // recursively finds the first matching record
        var find func(ByteSlice, *KeyBlock, int) (int, *KeyBlock)
        find = func(key ByteSlice, block *KeyBlock, height int) (int, *KeyBlock) {
            if height > 0 {
                var pos ByteSlice
                {
                    // we find where in the block this key would be inserted
                    i, _, _, _, _ := block.Find(key)

                    if i == 0 {
                        // even if this key doesn't equal the key we are looking for it will be at
                        // least greater than the key we are looking for.
                        if p, ok := block.GetPointer(0); ok {
                            pos = p
                        } else {
                            msg := fmt.Sprintf(
                                "110 Error could not get pointer %v from block %v", i, block)
                            panic(msg)
                        }
                    } else {
                        // else this spot is one to many so we get the previous spot
                        i--
                        if p, ok := block.GetPointer(i); ok {
                            pos = p
                        } else {
                            msg := fmt.Sprintf(
                                "118 Error could not get record %v from block %v", i, block)
                            panic(msg)
                        }
                    }
                }
                if pos == nil {
                    msg := fmt.Sprintf(
                        "123 Error could got null pos in find key=%v\n%v\n", key, block)
                    panic(msg)
                }
                return find(key, self.getblock(pos), height-1)
            }
            i, _, _, _, _ := block.Find(key)
            return i, block
        }
        i, block := find(left, self.getblock(self.info.Root()), self.info.Height()-1)

        // for a given block and a starting index returns the matching records in that block
        // if it ends on a matching record it will return true, else it will return false.
        // returning true indicates that the next block may have matching records. returning false
        // indicates the next block will never have matching records
        returns := func(start int, block *KeyBlock) bool {
            for i := start; i < int(block.RecordCount()); i++ {
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
            // the extra pointer is in the block points to the next block
            p, _ := block.GetExtraPtr()
            if p.Eq(ByteSlice64(0)) { break }
            block = self.getblock(p)
            start = 0
        }
        close(yield)
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

