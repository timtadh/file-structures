package file2

import "os"
import "fmt"
import . "file-structures/block/buffers"
import . "file-structures/block/byteslice"

type BlockFile struct {
    path string
    opened bool
    buf    Buffer
    file   *os.File
}

func NewBlockFile(path string, buf Buffer) *BlockFile {
    return &BlockFile{path:path, buf:buf}
}

func (self *BlockFile) Close() error {
    if err := self.file.Close(); err != nil {
        return err
    } else {
        self.file = nil
        self.opened = false
    }
    return nil
}

func (self *BlockFile) Path() string { return self.path }

func (self *BlockFile) Size() (uint64, error) {
    if !self.opened {
        return 0, fmt.Errorf("File is not open")
    }
    dir, err := os.Stat(self.path)
    if err != nil {
        return 0, err
    }
    return uint64(dir.Size()), nil
}

func (self *BlockFile) resize(size int64) error {
    return self.file.Truncate(size)
}

func (self *BlockFile) Allocate(amt uint32) (pos uint64, err error) {
    var size uint64
    if size, err = self.Size(); err != nil {
        return 0, err
    }
    if err := self.resize(int64(size + uint64(amt))); err != nil {
        return 0, err
    }
    return size, nil
}

func (self *BlockFile) WriteBlock(p int64, block []byte) error {
    if !self.opened {
        return fmt.Errorf("File is not open")
    }
    if b, ok := self.buf.Read(p, uint32(len(block))); ok {
        if ByteSlice(b).Eq(block) {
            // skip write no change in block from what is in cache
            return nil
        }
    }
    for pos, err := self.file.Seek(p, 0); pos != p; pos, err = self.file.Seek(p, 0) {
        if err != nil {
            return err
        }
    }
    if _, err := self.file.Write(block); err != nil {
        return err
    }
    self.buf.Update(p, block)
    return nil
}

func (self *BlockFile) ReadBlock(p int64, length uint32) ([]byte, error) {
    if !self.opened {
        return nil, fmt.Errorf("File is not open")
    }
    if b, ok := self.buf.Read(p, length); ok {
        return b, nil
    }
    block := make([]byte, length)
    for pos, err := self.file.Seek(p, 0); pos != p; pos, err = self.file.Seek(p, 0) {
        if err != nil {
            return nil, err
        }
    }
    if _, err := self.file.Read(block); err != nil {
        return nil, err
    }
    self.buf.Update(p, block)
    return block, nil
}

