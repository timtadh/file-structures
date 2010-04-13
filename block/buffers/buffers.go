package buffers

import list "container/list"
// import "fmt"
import "block/heap"

type Buffer interface {
    Update(p int64, block []byte)
    Read(p int64, length uint32) ([]byte, bool)
    Remove(p int64)
}

// -------------------------------------------------------------------------------
//notes for LRU:
// use "func (l *List) MoveToFront(e *Element)" to move the current request element to front
// use "func (l *List) PushFront(value interface{}) *Element" to insert a new element
// use "func (l *List) Back() *Element" to get the last element
// and "func (l *List) Remove(e *Element)" to remove it
type LRU struct {
    buffer map[int64]*list.Element
    stack  *list.List
    Size   int
}

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

func NewLRU(size int) *LRU {
    lru := new(LRU)
    lru.buffer = make(map[int64]*list.Element)
    lru.stack = list.New()
    lru.Size = size - 1
    return lru
}

func (self *LRU) Remove(p int64) {
    self.Update(p, nil)
}

func (self *LRU) Update(p int64, block []byte) {
    if e, has := self.buffer[p]; has {
        if block == nil {
            self.buffer[p] = nil, false
            self.stack.Remove(e)
        } else {
            e.Value.(*lru_item).bytes = block
            self.stack.MoveToFront(e)
        }
    } else {
        for self.Size < self.stack.Len() {
            e = self.stack.Back()
            i := e.Value.(*lru_item)
            self.buffer[i.p] = nil, false
            self.stack.Remove(e)
        }
        e = self.stack.PushFront(new_lruitem(p, block))
        self.buffer[p] = e
    }
}

func (self *LRU) Read(p int64, length uint32) ([]byte, bool) {
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
// -------------------------------------------------------------------------------

type LFU struct {
    buffer map[int64][]byte
    queue  *lfu_heap
}

type lfu_heap struct {
    slice []lfu_item
}

type lfu_item struct {
    p     int64
    count int
}

func new_heap(size int) *lfu_heap {
    self := new(lfu_heap)
    self.slice = make([]lfu_item, size)[0:0]
    //     fmt.Println(cap(self.slice))
    //     fmt.Println(len(self.slice))
    heap.Init(self)
    return self
}

func (self *lfu_heap) Size() int { return cap(self.slice) }

func (self *lfu_heap) Len() int { return len(self.slice) }

func (self *lfu_heap) Less(i, j int) bool { return self.slice[i].count < self.slice[j].count }

func (self *lfu_heap) Swap(i, j int) {
    self.slice[i], self.slice[j] = self.slice[j], self.slice[i]
}

func (self *lfu_heap) Push(x interface{}) {
    item := x.(*lfu_item)
    self.slice = self.slice[0 : len(self.slice)+1]
    self.slice[len(self.slice)-1] = *item
}

func (self *lfu_heap) Pop() interface{} {
    item := self.slice[len(self.slice)-1]
    self.slice = self.slice[0 : len(self.slice)-1]
    return item
}

func (self *lfu_heap) Remove(p int64) {
    for i := self.Len() - 1;
        i >= 0; i-- {
        if self.slice[i].p == p {
            heap.Remove(self, i)
            break
        }
    }
}

func (self *lfu_heap) Update(p int64) {
    i := self.Len() - 1
    for ; i >= 0; i-- {
        if self.slice[i].p == p {
            self.slice[i].count += 1
            heap.Down(self, i, self.Len())
            break
        }
    }
}

func NewLFU(size int) *LFU {
    self := new(LFU)
    self.buffer = make(map[int64][]byte)
    self.queue = new_heap(size)
    return self
}

func (self *LFU) Remove(p int64) {
    self.Update(p, nil)
}

func (self *LFU) Update(p int64, block []byte) {
    if _, has := self.buffer[p]; has {
        if block == nil {
            self.buffer[p] = nil, false
            self.queue.Remove(p)
        } else {
            self.queue.Update(p)
            self.buffer[p] = block
        }
    } else {
        for len(self.queue.slice) >= cap(self.queue.slice) {
            i := heap.Pop(self.queue).(lfu_item)
            self.buffer[i.p] = nil, false
        }
        heap.Push(self.queue, &lfu_item{p, 1})
        self.buffer[p] = block
    }
}

func (self *LFU) Read(p int64, length uint32) ([]byte, bool) {
    if bytes, has := self.buffer[p]; has {
        if len(bytes) != int(length) {
            return nil, false
        }
        //         fmt.Println("---------------------> Cache Hit")
        self.queue.Update(p)
        return bytes, true
    }
    //     fmt.Println("---------------------> Cache Miss")
    return nil, false
}

type NoBuffer struct {}

func (self *NoBuffer) Update(p int64, block []byte) {}
func (self *NoBuffer) Read(p int64, length uint32) ([]byte, bool) { return nil, false }
func (self *NoBuffer) Remove(p int64) {}
