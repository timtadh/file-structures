package blockfile

import "os"
import "fmt"
import . "buffers"
// const BLOCKSIZE = 4096

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

func (self *BlockFile) Open() bool {
    // the O_DIRECT flag turns off os buffering of pages allow us to do it manually
    // when using the O_DIRECT block size must be a multiple of 2048
    if f, err := os.Open(self.filename, OPENFLAG, 0666); err != nil {
        fmt.Println(err)
    } else {
        self.file = f
        self.opened = true
    }
    return self.opened
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

func (self *BlockFile) Allocate(size uint32) bool {
    return self.resize(int64(size))
}

func (self *BlockFile) WriteBlock(p int64, block []byte) bool {
    if !self.opened {
        return false
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
