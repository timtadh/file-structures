package file2

import (
    "os"
    "fmt"
    "container/list"
)

import buf "file-structures/block/buffers"
import bs "file-structures/block/byteslice"

type CacheFile struct {
    file   *BlockFile
    cache  *LRU
    keymap map[int64]int64 "memkey -> diskkey"
}

func NewCacheFile(path string, size uint64) (cf *CacheFile, err error) {
    cf = &CacheFile{
        file: NewBlockFile(path, &buf.NoBuffer{}),
    }
    cf.cache = NewLRU(1 + int(size/uint64(cf.file.BlkSize())), cf.pageout)
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

func (self *CacheFile) pageout(key int64, block bs.ByteSlice) error {
    return fmt.Errorf("Unimplemented")
}

func (self *CacheFile) ReadBlock(key int64) (block bs.ByteSlice, err error) {
    return nil, fmt.Errorf("Unimplemented")
}

type LRU struct {
    buffer map[int64]*list.Element
    stack  *list.List
    size   int
    pageout func(pos int64, bytes bs.ByteSlice) error
}

type lru_item struct {
    bytes bs.ByteSlice
    p     int64
}

func new_lruitem(p int64, bytes bs.ByteSlice) *lru_item {
    self := new(lru_item)
    self.p = p
    self.bytes = bytes
    return self
}

func NewLRU(size int, pf func(int64,bs.ByteSlice)error) *LRU {
    lru := new(LRU)
    lru.buffer = make(map[int64]*list.Element)
    lru.stack = list.New()
    lru.size = size - 1
    lru.pageout = pf
    return lru
}

func (self *LRU) Size() int { return self.size }

func (self *LRU) Remove(p int64) {
    self.Update(p, nil)
}

func (self *LRU) Update(p int64, block bs.ByteSlice) error {
    if e, has := self.buffer[p]; has {
        if block == nil {
            delete(self.buffer, p)
            self.stack.Remove(e)
        } else {
            e.Value.(*lru_item).bytes = block
            self.stack.MoveToFront(e)
        }
    } else {
        for self.size < self.stack.Len() {
            e = self.stack.Back()
            i := e.Value.(*lru_item)
            if err := self.pageout(i.p, i.bytes); err != nil {
                return err
            }
            delete(self.buffer, i.p)
            self.stack.Remove(e)
        }
        e = self.stack.PushFront(new_lruitem(p, block))
        self.buffer[p] = e
    }
    return nil
}

func (self *LRU) Read(p int64, length uint32) (bs.ByteSlice, bool) {
    if e, has := self.buffer[p]; has {
        if i, ok := e.Value.(*lru_item); ok {
            if len(i.bytes) != int(length) {
                return nil, false
            }
            self.stack.MoveToFront(e)
            // Don't write stuff like this to stdout!
            // fmt.Println("---------------------> Cache Hit")
            return i.bytes, true
        }
    }
    // Don't write stuff like this to stdout!
    // fmt.Println("---------------------> Cache Miss")
    return nil, false
}

