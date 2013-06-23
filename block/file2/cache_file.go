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
    nextkey int64
    free_keys []int64
}

func NewCacheFile(path string, size uint64) (cf *CacheFile, err error) {
    cf = &CacheFile{
        file: NewBlockFile(path, &buf.NoBuffer{}),
        keymap: make(map[int64]int64),
        free_keys: make([]int64, 0, 100),
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
    if disk_key, has := self.keymap[key]; has {
        delete(self.keymap, key)
        if err := self.file.Free(disk_key); err != nil {
            return err
        }
    } else {
        self.cache.Remove(key)
    }
    self.free_keys = append(self.free_keys, key)
    return nil
}

func (self *CacheFile) Allocate() (key int64, err error) {
    if len(self.free_keys) > 0 {
        key = self.free_keys[len(self.free_keys)-1]
        self.free_keys = self.free_keys[:len(self.free_keys)-1]
    } else {
        key = self.nextkey
        self.nextkey += 1
    }
    return key, nil
}

func (self *CacheFile) WriteBlock(key int64, block bs.ByteSlice) error {
    return self.cache.Update(key, block)
}

func (self *CacheFile) pageout(key int64, block bs.ByteSlice) error {
    if pos, err := self.file.Allocate(); err != nil {
        return nil
    } else {
        if err := self.file.WriteBlock(pos, block); err != nil {
            return err
        } else {
            self.keymap[key] = pos
        }
    }
    return nil
}

func (self *CacheFile) ReadBlock(key int64) (block bs.ByteSlice, err error) {
    if disk_key, has := self.keymap[key]; has {
        return self.file.ReadBlock(disk_key)
    } else {
        if data, has := self.cache.Read(key); has {
            return data, nil
        } else {
            return nil, fmt.Errorf("Key '%x' not found", key)
        }
    }
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

func (self *LRU) Read(p int64) (bs.ByteSlice, bool) {
    if e, has := self.buffer[p]; has {
        if i, ok := e.Value.(*lru_item); ok {
            self.stack.MoveToFront(e)
            return i.bytes, true
        }
    }
    return nil, false
}

