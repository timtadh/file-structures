package file2

import (
    "container/heap"
    "fmt"
)

import bs "file-structures/block/byteslice"

const MIN_HEAP = true
const MAX_HEAP = false

type LFUCacheFile struct {
    file       RemovableBlockDevice
    cache      map[int64]bs.ByteSlice
    cache_size int
    keymap     map[int64]int64 "memkey -> diskkey"
    cache_keys *priorityQueue
    disk_keys  *priorityQueue
    nextkey    int64
    free_keys  []int64
    userdata   []byte
}

func NewLFUCacheFile(file RemovableBlockDevice, size uint64) (cf *LFUCacheFile, err error) {
    cache_size := 0
    if size > 0 {
        cache_size = 1 + int(size/uint64(file.BlockSize()))
    }
    cf = &LFUCacheFile{
        file:       file,
        cache:      make(map[int64]bs.ByteSlice),
        cache_size: cache_size,
        keymap:     make(map[int64]int64),
        cache_keys: newPriorityQueue(cache_size, MIN_HEAP),
        disk_keys:  newPriorityQueue(cache_size, MAX_HEAP),
        nextkey:    int64(file.BlockSize()),
        free_keys:  make([]int64, 0, 100),
        userdata:   make([]byte, file.BlockSize()-CONTROLSIZE),
    }
    return cf, nil
}

func (self *LFUCacheFile) Close() error {
    if err := self.file.Close(); err != nil {
        return err
    }
    return nil
}

func (self *LFUCacheFile) Remove() error {
    return self.file.Remove()
}

func (self *LFUCacheFile) ControlData() (data bs.ByteSlice, err error) {
    data = make(bs.ByteSlice, self.file.BlockSize()-CONTROLSIZE)
    copy(data, self.userdata)
    return data, nil
}

func (self *LFUCacheFile) SetControlData(data bs.ByteSlice) (err error) {
    if len(data) > int(self.file.BlockSize()-CONTROLSIZE) {
        return fmt.Errorf("control data was too large")
    }
    self.userdata = make([]byte, self.file.BlockSize()-CONTROLSIZE)
    copy(self.userdata, data)
    return nil
}

func (self *LFUCacheFile) BlockSize() uint32 { return self.file.BlockSize() }

func (self *LFUCacheFile) Free(key int64) error {
    disk_has := self.disk_keys.HasKey(key)
    cache_has := self.cache_keys.HasKey(key)
    if disk_has && cache_has {
        return fmt.Errorf("Both disk and cache have key!")
    } else if cache_has {
        if err := self.removeCache(key); err != nil {
            return err
        }
    } else if disk_has {
        if err := self.removeFile(key); err != nil {
            return err
        }
    } else {
        return fmt.Errorf("Unknown key!")
    }
    self.free_keys = append(self.free_keys, key)
    return self.balance()
}

func (self *LFUCacheFile) Allocate() (key int64, err error) {
    if len(self.free_keys) > 0 {
        key = self.free_keys[len(self.free_keys)-1]
        self.free_keys = self.free_keys[:len(self.free_keys)-1]
    } else {
        key = self.nextkey
        self.nextkey += int64(self.file.BlockSize())
    }
    return key, self.WriteBlock(key, make(bs.ByteSlice, self.file.BlockSize()))
}

func (self *LFUCacheFile) writeFile(key int64, count int, block bs.ByteSlice) (err error) {
    var disk_key int64
    disk_key, has := self.keymap[key]
    if !has {
        disk_key, err = self.file.Allocate()
        if err != nil {
            return err
        }
        self.keymap[key] = disk_key
    }
    if err := self.file.WriteBlock(disk_key, block); err != nil {
        return err
    }
    self.disk_keys.Update(key, count)
    return nil
}

func (self *LFUCacheFile) writeCache(key int64, count int, block bs.ByteSlice) {
    self.cache[key] = block
    self.cache_keys.Update(key, count)
}

func (self *LFUCacheFile) readFile(key int64) (block bs.ByteSlice, count int, err error) {
    count, err = self.disk_keys.GetCount(key)
    if err != nil {
        return nil, 0, err
    }
    disk_key, has := self.keymap[key]
    if !has {
        return nil, 0, fmt.Errorf("disk did not have key")
    }
    block, err = self.file.ReadBlock(disk_key)
    return
}

func (self *LFUCacheFile) readCache(key int64) (block bs.ByteSlice, count int, err error) {
    count, err = self.cache_keys.GetCount(key)
    if err != nil {
        return nil, 0, err
    }
    block, has := self.cache[key]
    if !has {
        return nil, 0, fmt.Errorf("expected cache to have key, it did not!")
    }
    return
}

func (self *LFUCacheFile) removeFile(key int64) (err error) {
    var disk_key int64
    disk_key, has := self.keymap[key]
    if !has {
        return fmt.Errorf("disk did not have key")
    }
    delete(self.keymap, key)
    self.disk_keys.Remove(key)
    return self.file.Free(disk_key)
}

func (self *LFUCacheFile) removeCache(key int64) (err error) {
    if _, has := self.cache[key]; !has {
        return fmt.Errorf("removeCache: cache did not have key")
    }
    delete(self.cache, key)
    self.cache_keys.Remove(key)
    return nil
}

func (self *LFUCacheFile) balance() error {
    count := func(h *priorityQueue) int {
        item := h.Peek()
        if item == nil {
            return -1
        }
        return item.count
    }
    cache_to_disk := func() error {
        key := self.cache_keys.Peek().p
        block, count, err := self.readCache(key)
        if err != nil {
            return err
        }
        if err := self.writeFile(key, count, block); err != nil {
            return err
        }
        if err := self.removeCache(key); err != nil {
            return err
        }
        return nil
    }
    disk_to_cache := func() error {
        key := self.disk_keys.Peek().p
        block, count, err := self.readFile(key)
        if err != nil {
            return err
        }
        self.writeCache(key, count, block)
        if err := self.removeFile(key); err != nil {
            return err
        }
        return nil
    }

    for self.cache_size > 0 && len(self.cache) >= self.cache_size {
        if err := cache_to_disk(); err != nil {
            return err
        }
    }
    for self.cache_size > 0 && len(self.cache) < self.cache_size-1 && len(self.keymap) > 0 {
        if err := disk_to_cache(); err != nil {
            return err
        }
    }
    if self.cache_size > 0 && len(self.cache)+1 == self.cache_size {
        cache_min := count(self.cache_keys)
        disk_max := count(self.disk_keys)
        for cache_min < disk_max {
            if err := cache_to_disk(); err != nil {
                return err
            }
            if err := disk_to_cache(); err != nil {
                return err
            }
            cache_min = count(self.cache_keys)
            disk_max = count(self.disk_keys)
        }
    }
    return nil
}

func (self *LFUCacheFile) WriteBlock(key int64, block bs.ByteSlice) (err error) {
    disk_has := self.disk_keys.HasKey(key)
    cache_has := self.cache_keys.HasKey(key)
    if disk_has && cache_has {
        return fmt.Errorf("Both disk and cache have key!")
    } else if cache_has {
        count, err := self.cache_keys.GetCount(key)
        if err != nil {
            return err
        }
        self.writeCache(key, count+1, block)
        return self.balance()
    } else if disk_has {
        count, err := self.disk_keys.GetCount(key)
        if err != nil {
            return err
        }
        if err := self.writeFile(key, count+1, block); err != nil {
            return err
        }
        return self.balance()
    } else {
        if len(self.cache)+1 < self.cache_size { // room in the cache
            self.writeCache(key, 1, block)
        } else { // write it to disk to avoid a pageout
            if err := self.writeFile(key, 1, block); err != nil {
                return err
            }
        }
        return nil
    }
}

func (self *LFUCacheFile) pageout(key int64, block bs.ByteSlice) (err error) {
    return fmt.Errorf("Unimplemented")
}

func (self *LFUCacheFile) ReadBlock(key int64) (block bs.ByteSlice, err error) {
    var count int
    disk_has := self.disk_keys.HasKey(key)
    cache_has := self.cache_keys.HasKey(key)
    if disk_has && cache_has {
        return nil, fmt.Errorf("Both disk and cache have key!")
    } else if cache_has {
        block, count, err = self.readCache(key)
        if err != nil {
            return nil, err
        }
        self.cache_keys.Update(key, count+1)
    } else if disk_has {
        block, count, err = self.readFile(key)
        if err != nil {
            return nil, err
        }
        self.disk_keys.Update(key, count+1)
    } else {
        return nil, fmt.Errorf("Unknown key! %d", key)
    }
    return block, self.balance()
}

func (self *LFUCacheFile) ReadBlocks(key int64, n int) (blocks bs.ByteSlice, err error) {
    blk_size := int64(self.BlockSize())
    blocks = make(bs.ByteSlice, n*int(blk_size))
    for i := int64(0); i < int64(n); i++ {
        blk, err := self.ReadBlock(key + i*blk_size)
        if err != nil {
            return nil, err
        }
        copy(blocks[i*blk_size:(i+1)*blk_size], blk)
    }
    return blocks, nil
}



// -------------------------------------------------------------------------------

type priorityQueue struct {
    slice   []*priorityQueueItem
    indices map[int64]int
    min     bool
}

type priorityQueueItem struct {
    p     int64
    count int
}

func newPriorityQueue(size int, min bool) *priorityQueue {
    self := &priorityQueue{
        slice:   make([]*priorityQueueItem, 0, size),
        indices: make(map[int64]int),
        min:     min,
    }
    heap.Init(self)
    return self
}

func (self *priorityQueue) Size() int { return cap(self.slice) }

func (self *priorityQueue) Len() int { return len(self.slice) }

func (self *priorityQueue) Less(i, j int) bool {
    if self.min == MIN_HEAP {
        return self.slice[i].count < self.slice[j].count
    } else { // max heap
        return self.slice[i].count > self.slice[j].count
    }
}

func (self *priorityQueue) Swap(i, j int) {
    self.slice[i], self.slice[j] = self.slice[j], self.slice[i]
    self.indices[self.slice[i].p] = i
    self.indices[self.slice[j].p] = j
}

func (self *priorityQueue) Push(x interface{}) {
    n := len(self.slice)
    item := x.(*priorityQueueItem)
    self.indices[item.p] = n
    self.slice = append(self.slice, item)
}

func (self *priorityQueue) Pop() interface{} {
    item := self.slice[len(self.slice)-1]
    delete(self.indices, item.p)
    self.slice = self.slice[0 : len(self.slice)-1]
    return item
}

func (self *priorityQueue) Peek() *priorityQueueItem {
    if len(self.slice) == 0 {
        return nil
    }
    return self.slice[0]
}

func (self *priorityQueue) HasKey(key int64) bool {
    _, has := self.indices[key]
    return has
}

func (self *priorityQueue) GetCount(key int64) (int, error) {
    if i, has := self.indices[key]; has {
        item := self.slice[i]
        return item.count, nil
    } else {
        return 0, fmt.Errorf("GetCount: Key not found!")
    }
}

func (self *priorityQueue) Remove(p int64) {
    if i, has := self.indices[p]; has {
        heap.Remove(self, i)
        delete(self.indices, p)
    }
}

func (self *priorityQueue) Update(p int64, count int) {
    if i, has := self.indices[p]; has {
        item := self.slice[i]
        heap.Remove(self, i)
        item.count = count
        heap.Push(self, item)
    } else {
        heap.Push(self, &priorityQueueItem{p: p, count: count})
    }
}
