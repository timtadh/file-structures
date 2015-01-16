package bptree

import "fmt"
import "os"
import "runtime"
import "sync"
import "container/list"
import "file-structures/treeinfo"
import . "file-structures/block/file"
import . "file-structures/block/keyblock"
import . "file-structures/block/buffers"
import . "file-structures/block/byteslice"

const BUFFERSIZE = 536870912 // 512 megabytes

type BpTree struct {
    blocksize uint32
    bf        *BlockFile
    internal  *BlockDimensions
    external  *BlockDimensions
    info      *treeinfo.TreeInfo
    lock      *sync.Mutex
}

func NewBpTree(path string, keysize uint32, fields []uint32) (*BpTree, bool) {
    return NewBpTreeBufsize(path, keysize, fields, BUFFERSIZE)
}

func NewBpTreeBufsize(path string, keysize uint32, fields []uint32, bufsize int) (*BpTree, bool) {
    self := new(BpTree)
    self.lock = new(sync.Mutex)
    // 4 MB buffer with a block size of 4096 bytes
    if bf, ok := NewBlockFile(path, NewLRU(bufsize)); !ok {
        fmt.Fprintln(os.Stderr, "could not create block file")
        return nil, false
    } else {
        self.bf = bf
    }
    self.blocksize = treeinfo.BLOCKSIZE
    if inter, ok := NewBlockDimensions(POINTERS|EQUAPTRS|NODUP, self.blocksize, keysize, 8, nil); !ok {
        fmt.Fprintln(os.Stderr, "Block Dimensions invalid")
        return nil, false
    } else {
        self.internal = inter
    }

    if leaf, ok := NewBlockDimensions(RECORDS|EXTRAPTR, self.blocksize, keysize, 8, fields); !ok {
        fmt.Fprintln(os.Stderr, "Block Dimensions invalid")
        return nil, false
    } else {
        self.external = leaf
    }

    if !self.bf.Open() {
        fmt.Fprintln(os.Stderr, "Couldn't open file")
        return nil, false
    }
    if s, open := self.bf.Size(); open && s == 0 {
        // This is a new file the size is zero
        self.bf.Allocate(self.blocksize)
        b, ok := NewKeyBlock(self.bf, self.external)
        if !ok {
            self.bf.Close()
            fmt.Fprintln(os.Stderr, "Could not create the root block")
            return nil, false
        }
        if !b.SerializeToFile() {
            self.bf.Close()
            fmt.Fprintln(os.Stderr, "Could not serialize root block to file")
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

func (self *BpTree) compute_size() uint64 {
    zerokey := make([]byte, self.internal.KeySize)
    _, block := self.find(zerokey, self.getblock(self.info.Root()), self.info.Height()-1)
    count := uint64(0)
    for true {
        // the extra pointer is in the block points to the next block
        count += uint64(block.RecordCount())
        p, _ := block.GetExtraPtr()
        if p.Eq(ByteSlice64(0)) { break }
        block = self.getblock(p)
    }
    return count
}

func (self *BpTree) Size() uint64 {
    self.lock.Lock()
    defer self.lock.Unlock()
    return self.info.Entries()
}

func (self *BpTree) Get(key ByteSlice) *Record {
    self.lock.Lock()
    defer self.lock.Unlock()
    i, block := self.find(key, self.getblock(self.info.Root()), self.info.Height()-1)
    rec, _, _, ok := block.Get(i)
    last_rec, _, _, _ := block.Get(int(block.RecordCount()-1))
    for !ok && last_rec.GetKey().Lt(key) {
        next_blk, has := block.GetExtraPtr()
        if !has || next_blk.Zero() { return nil }
        block = self.getblock(next_blk)
        _, rec, _, _, ok = block.Find(key)
        last_rec, _, _, _ = block.Get(int(block.RecordCount()-1))
    }
    if !ok {
        return nil
    }
    if !key.Eq(rec.GetKey()) {
        return nil
    }
    return rec
}

func (self *BpTree) Contains(key ByteSlice) bool {
    rec := self.Get(key)
    if rec == nil {
        return false
    }
    return true
}


// recursively finds the first matching record
func (self *BpTree) find(key ByteSlice, block *KeyBlock, height int) (int, *KeyBlock) {
    // fmt.Printf("tree height %v, %v, %v, %v\n", 
    if height > 0 {
        if block.Mode() != self.internal.Mode {
            msg := fmt.Sprintf(
              "137 expected an internal block got an external %v %v\n%v", 
                  block.Position(), height, block)
            panic(msg)
        }
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
        return self.find(key, self.getblock(pos), height-1)
    }
    i, _, _, _, _ := block.Find(key)
    return i, block
}

func (self *BpTree) Find(left ByteSlice, right ByteSlice) (<-chan *Record) {
    records := make(chan *Record, 200)

    // Go Routine which finds and returns the records
    go func(yield chan<- *Record) {
        self.lock.Lock()
        defer self.lock.Unlock()
        // parameters are invalid or will yield the empty set
        if left == nil || right == nil || (!left.Eq(right) && right.Lt(left)) {
            close(yield)
            return
        }

        i, block := self.find(left, self.getblock(self.info.Root()), self.info.Height()-1)

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
    }(records);

    return records
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

