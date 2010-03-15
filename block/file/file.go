package file

import "os"
import "fmt"
import . "block/buffers"
import . "block/byteslice"

type BlockFile struct {
    filename string
    //     dim      *blockDimensions
    opened bool
    buf    Buffer
    file   *os.File
}

func NewBlockFile(filename string, buf Buffer) (*BlockFile, bool) {
    self := new(BlockFile)
    self.filename = filename
    //     self.dim = &dim
    self.buf = buf
    self.opened = false
    return self, true
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

func (self *BlockFile) Allocate(amt uint32) (uint64, bool) {
    size, ok := self.Size()
    if ok {
        if self.resize(int64(size + uint64(amt))) {
            return size, true
        }
    }
    return 0, false
}

func (self *BlockFile) WriteBlock(p int64, block []byte) bool {
    if !self.opened {
        return false
    }
    if b, ok := self.buf.Read(p, uint32(len(block))); ok {
        if ByteSlice(b).Eq(block) {
            //             fmt.Println("skip write no change in block from what is in cache")
            return true
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
    self.buf.Update(p, block)
    //     fmt.Println(block)
    return true
}

func (self *BlockFile) ReadBlock(p int64, length uint32) ([]byte, bool) {
    if !self.opened {
        return nil, false
    }
    if b, ok := self.buf.Read(p, length); ok {
        return b, ok
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
    self.buf.Update(p, block)
    return block, true
}
