package file

import "os"
import "fmt"
import . "block/buffers"
import . "block/byteslice"

const (
    INFTY64 int64 = 0x0FFFFFFFFFFFFFFF
    PNTR_SIZE64 int64 = 16
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
        pointerSize := PNTR_SIZE64
        if (!self.resize(2*pointerSize) ||
            !self.writeAllocPointer(0, 0, pointerSize) ||       // Write Head
            !self.writeAllocPointer(pointerSize, INFTY64, 0)) { // Write Tail
            return false
        }
        return true
    }
    // Initialize information from allocation list
    self.size = size - uint64(2*PNTR_SIZE64)
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
    return uint64(dir.Size), true
}

func (self *BlockFile) resize(size int64) bool {
    if err := self.file.Truncate(size); err != nil {
        fmt.Println(err)
        return false
    }
    return true
}

func (self *BlockFile) Allocate(amt uint32) (p uint64, success bool) {
    amt64 := int64(amt)
    // Don't allow allocation less than the pointer size,
    // or we won't be able to deallocate that space
    if amt64 < PNTR_SIZE64 { return 0, false }
    // Go through the linked list to
    // find the deallocated gaps
    var prev int64 = 0
    prevPointer, prevSize := self.allocPointer, int64(self.size)
    gapSize, gapNext, ok := self.readAllocPointer(prevPointer)
    for ; ok && gapNext != 0;
          gapSize, gapNext, ok = self.readAllocPointer(gapNext) {

        // If the gap is a perfect fit
        if uint64(amt) == gapSize {
            // Return pointer to gap
            p = uint64(prevPointer)
            // Update previous pointer to
            // point to the next gap, and
            // update file size
            if !self.writeAllocPointer(prev, prevSize, gapNext) ||
               !self.writeAllocPointer(0, int64(self.size) + amt64, self.allocPointer) {
                return 0, false
            }
            return p, true
        }
        // If there is enough room to fit the new
        // block without leaving a too-small gap
        if gapSize - uint64(amt) > uint64(PNTR_SIZE64) {
            // Return pointer to gap
            p = uint64(prevPointer)
            // Create new pointer, update
            // previous pointer, and update
            // file size
            if !self.writeAllocPointer(prevPointer + amt64, int64(gapSize) - amt64, gapNext) ||
               !self.writeAllocPointer(prev, prevSize, prevPointer + amt64) ||
               !self.writeAllocPointer(0, int64(self.size) + amt64, self.allocPointer) {
                return 0, false
            }
            return p, true
        }
        prev, prevPointer, prevSize = prevPointer, gapNext, int64(gapSize)
    }
    if !ok { return 0, false }
    // We've reached the end of the list
    // and need to allocate more space,
    // and update as in the second case
    p = uint64(prevPointer)
    realSize, gotSize := self.RealSize()
    if !gotSize ||
       !self.resize(int64(realSize) + amt64) ||
       !self.writeAllocPointer(prevPointer + amt64, INFTY64, 0) ||
       !self.writeAllocPointer(prev, prevSize, prevPointer + amt64) ||
       !self.writeAllocPointer(0, int64(self.size) + amt64, self.allocPointer) {
        return 0, false
    }
    return p, true
}

func (self *BlockFile) Deallocate(p int64, amt uint32) bool {
    amt64 := int64(amt)
    // Can't deallocate a chunk smaller than the list pointer
    if amt64 < PNTR_SIZE64 { return false }
    // Traverse list to find proper position 
    var prev int64 = 0
    prevPointer, prevSize := self.allocPointer, PNTR_SIZE64
    gapSize, gapNext, ok := self.readAllocPointer(prevPointer)
    for ; ok && prevPointer != 0;
          gapSize, gapNext, ok = self.readAllocPointer(gapNext) {
        //fmt.Printf("prev: %d, prevPntr: %d, prevSize: %d, gapSize: %d, gapNext: %d\n",prev, prevPointer, prevSize, gapSize, gapNext)
        switch {
        case p < prev: // Shouldn't happen unless input is bad
            return false
        case p < prev + prevSize:  // Deallocating within previous deallocated block
            return false
        case p >= prevPointer: // Deallocation beyond this gap
            prev, prevPointer, prevSize = prevPointer, gapNext, int64(gapSize)
            continue
        case p + amt64 > prevPointer: // Deallocating into next deallocated block
            return false

        case p == prev + prevSize && p + amt64 == prevPointer:
            // Contiguous with prev & next deallocated blocks
            // Merge the three blocks,
            // unles prev is head of list
            if prev != 0 {
                if gapNext == 0 {
                    // Need to resize file
                    return self.writeAllocPointer(prev, INFTY64, 0) &&
                           self.writeAllocPointer(0, int64(self.size) - amt64, self.allocPointer) &&
                           self.resize(prev + PNTR_SIZE64) &&
                           self.unBuffer(p)
                } else {
                    return self.writeAllocPointer(prev, prevSize + amt64 + int64(gapSize), gapNext) &&
                           self.writeAllocPointer(0, int64(self.size) - amt64, self.allocPointer) &&
                           self.unBuffer(p)
                }
            } else if prev == 0 && gapNext == 0 {
                // Deallocate the whole file
                return self.resize(0) && self.initialize()
            } // else prev is head of list
            fallthrough

        case p + amt64 == prevPointer:
            // Only contiguous with next block
            if (p - prev) - prevSize < PNTR_SIZE64 &&
               !(prev == 0 && (p - prev) - prevSize == 0) {
                // Remaining gap too small
                return false
            }
            // Merge the two blocks
            if gapNext == 0 {
                // Need to resize file
                return self.writeAllocPointer(p, INFTY64, 0) &&
                       self.writeAllocPointer(prev, prevSize, p) &&
                       self.writeAllocPointer(0, int64(self.size) - amt64, self.allocPointer) &&
                       self.resize(p + PNTR_SIZE64) &&
                       self.unBuffer(p)
            } else {
                return self.writeAllocPointer(p, amt64 + int64(gapSize), gapNext) &&
                       self.writeAllocPointer(prev, prevSize, p) &&
                       self.writeAllocPointer(0, int64(self.size) - amt64, self.allocPointer) &&
                       self.unBuffer(p)
            }

        case p == prev + prevSize:
            // Only contiguous with prev block
            if (prevPointer - p) - amt64 < PNTR_SIZE64 {
                // The remaining space in the gap
                // is too small to deallocate
                return false
            }
            // Merge the two blocks,
            // unless prev is head of list
            if prev != 0 {
                return self.writeAllocPointer(prev, prevSize + amt64, prevPointer) &&
                       self.writeAllocPointer(0, int64(self.size) - amt64, self.allocPointer) &&
                       self.unBuffer(p)
            } else {
                return self.writeAllocPointer(prev, prevSize, p) &&
                       self.writeAllocPointer(p, amt64, prevPointer) &&
                       self.writeAllocPointer(0, int64(self.size) - amt64, self.allocPointer) &&
                       self.unBuffer(p)
            }

        // Should be an isolated deallocation at this point
        case (prevPointer - p) - amt64 < PNTR_SIZE64: // Too small gap on right
            return false
        case (p - prev) - prevSize < PNTR_SIZE64 &&   // Too small gap on left
             !(p == prev + prevSize && prev == 0):    // Unless we're all the way to the right
            return false
        case true:
            return self.writeAllocPointer(prev, prevSize, p) &&
                   self.writeAllocPointer(p, amt64, prevPointer) &&
                   self.writeAllocPointer(0, int64(self.size) - amt64, self.allocPointer) &&
                   self.unBuffer(p)
        }
        // The switch statement should cover all possibilities
        fmt.Println("Reached \"impossible\" case in Deallocate")
        return false
    }
    return false
}

/*
 *  Returns a pointer to where the first
 *  allocated block in the file would be
 */
func (self *BlockFile) FirstAllocatedBlock() (int64) {
    return PNTR_SIZE64
}

func (self *BlockFile) unBuffer(p int64) bool {
    self.buf.Remove(p)
    return true
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

func (self *BlockFile) write(p int64, block []byte, useBuffer bool) bool {
    if !self.opened {
        return false
    }
    if useBuffer {
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
    if useBuffer {
        self.buf.Update(p, block)
    }
    //     fmt.Println(block)
    return true
}

func (self *BlockFile) ReadBlock(p int64, length uint32) ([]byte, bool) {
    return self.read(p, length, true)
}

func (self *BlockFile) readAllocPointer(p int64) (size uint64, next int64, ok bool) {
    data, ok := self.read(p, uint32(PNTR_SIZE64), false)
    if !ok { return 0, 0, false }
    size = ByteSlice(data[0:8]).Int64()
    next = int64(ByteSlice(data[8:16]).Int64())
    return size, next, true
}

func (self *BlockFile) read(p int64, length uint32, useBuffer bool) ([]byte, bool) {
    if !self.opened {
        return nil, false
    }
    if useBuffer {
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
    if useBuffer {
        self.buf.Update(p, block)
    }
    return block, true
}

func (self *BlockFile) PrintSummary() {
    fmt.Println(self.Summarize().String())
}

func (self *BlockFile) Summarize() (summary *FileSummary) {
    summary = NewFileSummary()
    if !self.opened { return }

    prev, prevPointer, prevSize := int64(0), self.allocPointer, PNTR_SIZE64
    summary.Add(int(prevSize), HEAD)
    for nextSize, nextPointer, ok := self.readAllocPointer(prevPointer);
        ok && prevPointer != 0;
        nextSize, nextPointer, ok = self.readAllocPointer(nextPointer) {

        summary.Add(int((prevPointer - prev) - prevSize), ALLOC)
        if nextPointer == 0 {
            summary.Add(int(PNTR_SIZE64), TAIL)
        } else {
            summary.Add(int(nextSize), DEALLOC)
        }
        prev, prevPointer, prevSize = prevPointer, nextPointer, int64(nextSize)
    }
    return
}
