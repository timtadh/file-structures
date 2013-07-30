package file2

import (
    "fmt"
    "container/list"
)

import bs "file-structures/block/byteslice"

type lru struct {
    buffer map[int64]*list.Element
    stack  *list.List
    size   int
    pageout func(int64, []byte) error
}

type LRUCacheFile struct {
    file       RemovableBlockDevice
    cache      map[int64]*lru_item
    cache_size int
    lru        *lru
    keymap     map[int64]int64 "memkey -> diskkey"
    nextkey    int64
    free_keys  []int64
    userdata   []byte
}

func NewLRUCacheFile(file RemovableBlockDevice, size uint64) (cf *LRUCacheFile, err error) {
    cache_size := 0
    if size > 0 {
        cache_size = 1 + int(size/uint64(file.BlockSize()))
    }
    cf = &LRUCacheFile{
        file:       file,
        cache:      make(map[int64]*lru_item),
        cache_size: cache_size,
        keymap:     make(map[int64]int64),
        nextkey:    int64(file.BlockSize()),
        free_keys:  make([]int64, 0, 100),
        userdata:   make([]byte, file.BlockSize()-CONTROLSIZE),
    }
    cf.lru = newLRU(cache_size, cf.pageout)
    return cf, nil
}

func (self *LRUCacheFile) Close() error {
    if err := self.file.Close(); err != nil {
        return err
    }
    return self.file.Remove()
}

func (self *LRUCacheFile) ControlData() (data bs.ByteSlice, err error) {
    data = make(bs.ByteSlice, self.file.BlockSize()-CONTROLSIZE)
    copy(data, self.userdata)
    return data, nil
}

func (self *LRUCacheFile) SetControlData(data bs.ByteSlice) (err error) {
    if len(data) > int(self.file.BlockSize()-CONTROLSIZE) {
        return fmt.Errorf("control data was too large")
    }
    self.userdata = make([]byte, self.file.BlockSize()-CONTROLSIZE)
    copy(self.userdata, data)
    return nil
}

func (self *LRUCacheFile) BlockSize() uint32 { return self.file.BlockSize() }

func (self *LRUCacheFile) Free(key int64) error {
    self.lru.Remove(key)
    if disk_key, has := self.keymap[key]; has {
        delete(self.keymap, key)
        if err := self.file.Free(disk_key); err != nil {
            return err
        }
    }
    self.free_keys = append(self.free_keys, key)
    return nil
}

func (self *LRUCacheFile) Allocate() (key int64, err error) {
    if len(self.free_keys) > 0 {
        key = self.free_keys[len(self.free_keys)-1]
        self.free_keys = self.free_keys[:len(self.free_keys)-1]
    } else {
        key = self.nextkey
        self.nextkey += int64(self.file.BlockSize())
    }
    return key, self.WriteBlock(key, make(bs.ByteSlice, self.file.BlockSize()))
}

func (self *LRUCacheFile) pageout(key int64, block []byte) error {
    var disk_key int64
    disk_key, has := self.keymap[key]
    if !has {
        var err error
        disk_key, err = self.file.Allocate()
        if err != nil {
            return err
        }
        self.keymap[key] = disk_key
    }
    return self.file.WriteBlock(disk_key, block)
}

func (self *LRUCacheFile) WriteBlock(key int64, block bs.ByteSlice) (err error) {
    return self.lru.Update(key, block)
}

func (self *LRUCacheFile) ReadBlock(key int64) (block bs.ByteSlice, err error) {
    block, has := self.lru.Read(key, self.BlockSize())
    if !has {
        disk_key, has := self.keymap[key]
        if !has {
            return nil, fmt.Errorf("disk did not have key")
        }
        block, err := self.file.ReadBlock(disk_key)
        if err != nil {
            return nil, err
        }
        err = self.lru.Update(key, block)
        if err != nil {
            return nil, err
        }
        return block, nil
    } else {
        return block, nil
    }
}

// -------------------------------------------------------------------------------------


type lru_item struct {
    bytes []byte
    p     int64
}

func new_lruitem(p int64, bytes []byte) *lru_item {
    self := new(lru_item)
    self.p = p
    self.bytes = bytes
    return self
}

func newLRU(size int, pageout func(int64,[]byte)error) *lru {
    self := new(lru)
    self.buffer = make(map[int64]*list.Element)
    self.stack = list.New()
    self.size = size - 1
    self.pageout = pageout
    return self
}

func (self *lru) Size() int { return self.size }

func (self *lru) Remove(p int64) {
    self.Update(p, nil)
}

func (self *lru) Update(p int64, block []byte) error {
    if e, has := self.buffer[p]; has {
        if block == nil {
            delete(self.buffer, p)
            self.stack.Remove(e)
        } else {
            e.Value.(*lru_item).bytes = block
            self.stack.MoveToFront(e)
        }
    } else {
        if block == nil {
            // deleting the block, and it isn't in the cache
            // so do nothing.
            return nil
        }
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

func (self *lru) Read(p int64, length uint32) ([]byte, bool) {
    if e, has := self.buffer[p]; has {
        if i, ok := e.Value.(*lru_item); ok {
            if len(i.bytes) != int(length) {
                return nil, false
            }
            self.stack.MoveToFront(e)
            // hit
            return i.bytes, true
        }
    }
    // miss
    return nil, false
}
