package file2

import (
    "os"
    "fmt"
)

import buf "file-structures/block/buffers"
import bs "file-structures/block/byteslice"

type CacheFile struct {
    file   *BlockFile
    buf    buf.Buffer
    keymap map[int64]int64 "memkey -> diskkey"
}

func NewCacheFile(path string, size uint64) (cf *CacheFile, err error) {
    cf = &CacheFile{
        file: NewBlockFile(path, &buf.NoBuffer{}),
        buf:  &buf.NoBuffer{},
    }
    if err := cf.file.Open(); err != nil {
        return nil, err
    }
    return cf, nil
}

func (self *CacheFile) Close() error {
    if err := self.file.Close(); err != nil {
        return err
    }
    return os.Remove(self.file.Path())
}

func (self *CacheFile) BlkSize() uint32 { return self.file.BlkSize() }

func (self *CacheFile) Free(key int64) error {
    return fmt.Errorf("Unimplemented")
}

func (self *CacheFile) Allocate() (key int64, err error) {
    return 0, fmt.Errorf("Unimplemented")
}

func (self *CacheFile) WriteBlock(key int64, block bs.ByteSlice) error {
    return fmt.Errorf("Unimplemented")
}

func (self *CacheFile) ReadBlock(key int64) (block bs.ByteSlice, err error) {
    return nil, fmt.Errorf("Unimplemented")
}

