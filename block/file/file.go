package file

import "os"
import "fmt"
import . "block/buffers"
import . "block/byteslice"

const (
    INFTY64 int64 = 0x0FFFFFFFFFFFFFFF
    PNTR_SIZE uint32 = 16
)

type BlockFile struct {
    filename string
    //     dim      *blockDimensions
    opened bool
    buf    Buffer
    file   *os.File
    size uint64
    allocPointer int64
}

func NewBlockFile(filename string, buf Buffer) (*BlockFile, bool) {
    self := new(BlockFile)
    self.filename = filename
    //     self.dim = &dim
    self.buf = buf
    self.opened = false
    return self, true
}

func (self *BlockFile) initialize() bool {
    size, open := self.RealSize()
    if !open { return false }
    if size == 0 {
        // This is the first time
        // this file has been opened
        pointerSize := int64(PNTR_SIZE)
        if (!self.resize(2*pointerSize) ||
            !self.writeAllocPointer(0, 0, pointerSize) ||       // Write Head
            !self.writeAllocPointer(pointerSize, INFTY64, 0)) { // Write Tail
            return false
        }
        return true
    }
    // Initialize information from allocation list
    self.size = size - uint64(2*PNTR_SIZE)
    sz, next, ok := self.readAllocPointer(0)
    self.allocPointer = next
    for sz, next, ok = self.readAllocPointer(next); next != 0; sz, next, ok = self.readAllocPointer(next) {
        if !ok { return false }
        self.size -= sz
    }
    return true
}

func (self *BlockFile) Close() bool {
    if err := self.file.Close(); err != nil {
        fmt.Println(err)
    } else {
        self.file = nil
        self.opened = false
    }
    return self.opened
}

func (self *BlockFile) Filename() string { return self.filename }

func (self *BlockFile) Size() (uint64, bool) {
    if !self.opened {
        return 0, false
    }
    return self.size, true
}

func (self *BlockFile) RealSize() (uint64, bool) {
    if !self.opened {
        return 0, false
    }
    dir, err := os.Stat(self.filename)
    if err != nil {
        fmt.Println(err)
        return 0, false
    }
    return dir.Size, true
}

func (self *BlockFile) resize(size int64) bool {
    if err := self.file.Truncate(size); err != nil {
        fmt.Println(err)
        return false
    }
    return true
}

func (self *BlockFile) Allocate(amt uint32) (p uint64, success bool) {
    // Don't allow allocation less than the pointer size,
    // or we won't be able to deallocate that space
    if amt < PNTR_SIZE { return 0, false }
    // Go through the linked list to
    // find the deallocated gaps
    var prev int64 = 0
    prevPointer, prevSize := self.allocPointer, self.size
    gapSize, gapNext, ok := self.readAllocPointer(prevPointer);
    for ; ok && gapNext != 0;
          gapSize, gapNext, ok = self.readAllocPointer(gapNext) {

        // If the gap is a perfect fit
        if uint64(amt) == gapSize {
            // Return pointer to gap
            p = uint64(prevPointer)
            // Update previous pointer to
            // point to the next gap, and
            // update file size
            if !self.writeAllocPointer(prev, int64(prevSize), gapNext) ||
               !self.writeAllocPointer(0, int64(self.size) + int64(amt), self.allocPointer) {
                return 0, false
            }
            return p, true
        }
        // If there is enough room to fit the new
        // block without leaving a too-small gap
        if gapSize - uint64(amt) > uint64(PNTR_SIZE) {
            // Return pointer to gap
            p = uint64(prevPointer)
            // Create new pointer, update
            // previous pointer, and update
            // file size
            if !self.writeAllocPointer(prevPointer + int64(amt), int64(gapSize) - int64(amt), gapNext) ||
               !self.writeAllocPointer(prev, int64(prevSize), prevPointer + int64(amt)) ||
               !self.writeAllocPointer(0, int64(self.size) + int64(amt), self.allocPointer) {
                return 0, false
            }
            return p, true
        }
        prev, prevPointer, prevSize = prevPointer, gapNext, gapSize
    }
    if !ok { return 0, false }
    // We've reached the end of the list
    // and need to allocate more space,
    // and update as in the second case
    p = uint64(prevPointer)
    realSize, gotSize := self.RealSize()
    if !gotSize ||
       !self.resize(int64(realSize) + int64(amt)) ||
       !self.writeAllocPointer(prevPointer + int64(amt), INFTY64, 0) ||
       !self.writeAllocPointer(prev, int64(prevSize), prevPointer + int64(amt)) ||
       !self.writeAllocPointer(0, int64(self.size) + int64(amt), self.allocPointer) {
        return 0, false
    }
    return p, true
}

/*
 *  Returns a pointer to where the first
 *  allocated block in the file would be
 */
func (self *BlockFile) FirstAllocatedBlock() (int64) {
    return int64(PNTR_SIZE)
}

func (self *BlockFile) WriteBlock(p int64, block []byte) bool {
    return self.write(p, block, true)
}

func (self *BlockFile) writeAllocPointer(p int64, size int64, next int64) bool {
    data := ByteSlice64(uint64(size)).Concat(ByteSlice64(uint64(next)))
    if !self.write(p, data, false) { return false }
    // Cache the root pointer
    if p == 0 {
        self.size = uint64(size)
        self.allocPointer = next
    }
    return true
}

func (self *BlockFile) write(p int64, block []byte, useCache bool) bool {
    if !self.opened {
        return false
    }
    if useCache {
        if b, ok := self.buf.Read(p, uint32(len(block))); ok {
            if ByteSlice(b).Eq(block) {
                //fmt.Println("skip write no change in block from what is in cache")
                return true
            }
        }
    }
    for pos, err := self.file.Seek(p, 0); pos != p; pos, err = self.file.Seek(p, 0) {
        if err != nil {
            fmt.Println(err)
            return false
        }
    }
    if n, err := self.file.Write(block); err != nil {
        fmt.Print("WriteBlock line 88: ")
        fmt.Printf("%v ", n)
        fmt.Println(err)
        return false
    }
    if useCache {
        self.buf.Update(p, block)
    }
    //     fmt.Println(block)
    return true
}

func (self *BlockFile) ReadBlock(p int64, length uint32) ([]byte, bool) {
    return self.read(p, length, true)
}

func (self *BlockFile) readAllocPointer(p int64) (size uint64, next int64, ok bool) {
    data, ok := self.read(p, PNTR_SIZE, false)
    if !ok { return 0, 0, false }
    size = ByteSlice(data[0:8]).Int64()
    next = int64(ByteSlice(data[8:16]).Int64())
    return size, next, true
}

func (self *BlockFile) read(p int64, length uint32, useCache bool) ([]byte, bool) {
    if !self.opened {
        return nil, false
    }
    if useCache {
        if b, ok := self.buf.Read(p, length); ok {
            return b, ok
        }
    }
    block := make([]byte, length)
    for pos, err := self.file.Seek(p, 0); pos != p; pos, err = self.file.Seek(p, 0) {
        if err != nil {
            fmt.Println(err)
            return nil, false
        }
    }
    if n, err := self.file.Read(block); err != nil {
        fmt.Print("ReadBlock line 105: ")
        fmt.Printf("%v ", n)
        fmt.Println(err)
        return nil, false
    }
    if useCache {
        self.buf.Update(p, block)
    }
    return block, true
}

func (self *BlockFile) PrintContents() {
    size, ok := self.RealSize()
    if !ok { return }
    data, read := self.read(0, uint32(size), false)
    if !read { return }
    fmt.Printf("%v\n",data)
}
