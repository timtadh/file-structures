package keyblock

import "fmt"
import . "block/file"
import . "block/byteslice"
import "log"

const BLOCKHEADER = 5

type KeyBlock struct {
    bf        *BlockFile
    dim       *BlockDimensions
    rec_count uint16
    ptr_count uint16
    fields    []uint32
    position  ByteSlice
    records   RecordsSlice
    pointers  []ByteSlice
    extraptr  ByteSlice
}

func NewKeyBlock(bf *BlockFile, dim *BlockDimensions) (*KeyBlock, bool) {
    if size, ok := bf.Allocate(dim.BlockSize); ok {
        b := newKeyBlock(bf, ByteSlice64(size), dim)
        return b, true
    }
    return nil, false
}

func newKeyBlock(bf *BlockFile, pos ByteSlice, dim *BlockDimensions) *KeyBlock {
    n := dim.KeysPerBlock()
    //     fmt.Println(n)
    self := new(KeyBlock)
    self.bf = bf
    self.dim = dim
    self.position = pos
    self.rec_count = 0
    self.ptr_count = 0
    self.records = make(RecordsSlice, n)
    if self.dim.Mode&POINTERS == POINTERS && self.dim.Mode&EQUAPTRS == 0 {
        self.pointers = make([]ByteSlice, n+1)
    } else if self.dim.Mode&(POINTERS|EQUAPTRS) == (POINTERS | EQUAPTRS) {
        self.pointers = make([]ByteSlice, n)
    }
    return self
}

func (self *KeyBlock) NewRecord(key ByteSlice) *Record {
    return newRecord(key, self.dim)
}

func (self *KeyBlock) Size() uint32           { return self.dim.BlockSize }
func (self *KeyBlock) RecordSize() uint32     { return self.dim.RecordSize() }
func (self *KeyBlock) KeySize() uint32        { return self.dim.KeySize }
func (self *KeyBlock) PointerSize() uint32    { return self.dim.PointerSize }
func (self *KeyBlock) MaxRecordCount() uint16 { return uint16(len(self.records)) }
func (self *KeyBlock) Full() bool             { return len(self.records) == int(self.rec_count) }
func (self *KeyBlock) RecordCount() uint16    { return self.rec_count }
func (self *KeyBlock) PointerCount() uint16   { return self.ptr_count }
func (self *KeyBlock) Position() ByteSlice    { return self.position }
func (self *KeyBlock) Mode() uint8            { return self.dim.Mode }
func (self *KeyBlock) Dim() BlockDimensions   { return *self.dim }

func (self *KeyBlock) SetExtraPtr(ptr ByteSlice) bool {
    if self.dim.Mode&EXTRAPTR != 0 && len(ptr) == int(self.dim.PointerSize) {
        self.extraptr = ptr
        return true
    }
    return false
}

func (self *KeyBlock) GetExtraPtr() (ByteSlice, bool) {
    if self.dim.Mode&EXTRAPTR != 0 {
        return self.extraptr, true
    }
    return nil, false
}

func (b *KeyBlock) Add(r *Record) (int, bool) {
    if b.RecordCount() >= b.MaxRecordCount() {
        return -1, false
    }
    //     fmt.Println()
    //     fmt.Println(r)
    i, ok := b.find(r.key)
    if b.dim.Mode&NODUP == NODUP && ok {
        log.Exit("tried to insert a duplicate key into a block which does not allow that.\n", b)
        return -1, false
    }
    //     fmt.Printf("i=%v, k=%v\n", i, r.GetKey())
    //     if !ok {
    j := len(b.records)
    j -= 1
    for ; j > int(i); j-- {
        b.records[j] = b.records[j-1]
    }
    b.records[i] = r
    b.rec_count += 1
    return i, true
    //     }
    //     return -1, false
}

func (self *KeyBlock) InsertPointer(i int, ptr ByteSlice) bool {
    if self.dim.Mode&POINTERS == 0 {
        return false
    }
    //     fmt.Println(self.PointerSize())
    if ptr == nil || uint32(len(ptr)) != self.PointerSize() ||
        i > int(self.PointerCount()) || self.PointerCount() >= self.MaxRecordCount()+1 {
        return false
    }
    j := len(self.records)
    if self.dim.Mode&EQUAPTRS != EQUAPTRS {
        self.pointers[j] = self.pointers[j-1]
    }
    j -= 1
    for ; j > int(i); j-- {
        self.pointers[j] = self.pointers[j-1]
    }
    self.pointers[i] = ptr
    self.ptr_count += 1
    return true
}

func (self *KeyBlock) SetPointer(i int, ptr ByteSlice) bool {
    if self.dim.Mode&POINTERS == 0 {
        return false
    }
    if i > int(self.PointerCount()) {
        return false
    }
    self.pointers[i] = ptr
    return true
}

func (self *KeyBlock) Find(k ByteSlice) (int, *Record, ByteSlice, ByteSlice, bool) {
    i, ok := self.find(k)
    if !ok { return i, nil, nil, nil, false }
    rec, l, r, ok := self.Get(i)
    return i, rec, l, r, ok
}

func (self *KeyBlock) Count(k ByteSlice) int {
    i, ok := self.find(k)
    if !ok { return 0 }
    count := 0
    for j := i;
        j < len(self.records) && self.records[j] != nil && self.records[j].GetKey().Eq(k);
        j++ {
            count++
    }
    return count
}

func (self *KeyBlock) FindAll(k ByteSlice) (<-chan *Record, chan<- bool) {
    records := make(chan *Record)
    ack := make(chan bool)

    go func(yield chan<- *Record, ack <-chan bool) {
        i, ok := self.find(k)
        if !ok {
            close(ack)
            close(yield)
            return
        }
        for j := i;
            j < len(self.records) && self.records[j] != nil && self.records[j].GetKey().Eq(k);
            j++ {
                yield<-self.records[j]
                <-ack

        }
        close(yield)
        return
    }(records, ack)

    return records, ack
}

func (self *KeyBlock) Get(i int) (*Record, ByteSlice, ByteSlice, bool) {
    if self.dim.Mode&POINTERS == 0 && i < int(self.RecordCount()) && i >= 0 {
        return self.records[i], nil, nil, true
    } else if self.dim.Mode&EQUAPTRS == 0 && i < int(self.RecordCount()) && i >= 0 {
        return self.records[i], self.pointers[i], self.pointers[i+1], true
    } else if self.dim.Mode&EQUAPTRS == EQUAPTRS && i < int(self.RecordCount()) && i >= 0 {
        if i+1 == int(self.RecordCount()) {
            return self.records[i], self.pointers[i], nil, true
        }
        return self.records[i], self.pointers[i], self.pointers[i+1], true
    }
    return nil, nil, nil, false
}

func (self *KeyBlock) GetPointer(i int) (ByteSlice, bool) {
    if i < int(self.PointerCount()) {
        return self.pointers[i], true
    }
    return nil, false
}

func (self *KeyBlock) PointerIndex(ptr ByteSlice) (int, bool) {
    if self.dim.Mode&POINTERS == 0 {
        return -1, false
    }
    if ptr == nil || uint32(len(ptr)) != self.PointerSize() {
        return -1, false
    }
    for i, p := range self.pointers {
        if p.Eq(ptr) {
            return i, true
        }
    }
    return -1, false
}

func (self *KeyBlock) Remove(k ByteSlice) (int, bool) {
    i, ok := self.find(k)
    if ok {
        for j := i; j < len(self.records); j++ {
            if j+1 < len(self.records) {
                self.records[j] = self.records[j+1]
            } else {
                self.records[j] = nil
            }
        }
    } else {
        return -1, false
    }
    self.rec_count -= 1
    return i, true
}

func (self *KeyBlock) RemoveAtIndex(i int) bool {
    if i >= int(self.RecordCount()) {
        fmt.Printf("RemoveAtIndex failed %v >= %v\n", i, self.RecordCount())
        return false
    }
    for j := i; j < len(self.records); j++ {
        if j+1 < len(self.records) {
            self.records[j] = self.records[j+1]
        } else {
            self.records[j] = nil
        }
    }
    self.rec_count -= 1
    return true
}

func (self *KeyBlock) RemovePointer(i int) bool {
    if self.dim.Mode&POINTERS == 0 {
        return false
    }
    if i > int(self.PointerCount()) || self.PointerCount() == 0 {
        return false
    }
    j := i
    for ; j < len(self.pointers); j++ {
        if j+1 < len(self.pointers) {
            self.pointers[j] = self.pointers[j+1]
        } else {
            self.pointers[j] = nil
        }
    }
    self.ptr_count -= 1
    return true
}

func (self *KeyBlock) SerializeToFile() bool {
    if bytes, ok := self.Serialize(); ok {
        return self.bf.WriteBlock(int64(self.Position().Int64()), bytes)
    }
    return false
}

func (self *KeyBlock) Serialize() ([]byte, bool) {
    bytes := make([]byte, self.Size())
    c := 0
    bytes[c] = self.dim.Mode
    c++
    for _, v := range ByteSlice16(self.RecordCount()) {
        bytes[c] = v
        c++
    }
    for _, v := range ByteSlice16(self.PointerCount()) {
        bytes[c] = v
        c++
    }
    for i := 0; i < len(self.records); i++ {
        rec := self.records[i]
        if rec != nil {
            for _, v := range rec.key {
                bytes[c] = v
                c++
            }
            for _, field := range rec.data {
                for _, v := range field {
                    bytes[c] = v
                    c++
                }
            }
        } else {
            for j := 0; j < int(self.RecordSize()+self.KeySize()); j++ {
                bytes[c] = 0
                c++
            }
        }
    }
    for i := 0; i < len(self.pointers) && self.PointerSize() > 0; i++ {
        ptr := self.pointers[i]
        if ptr != nil {
            for _, v := range ptr {
                bytes[c] = v
                c++
            }
        } else {
            for j := 0; j < int(self.PointerSize()); j++ {
                bytes[c] = 0
                c++
            }
        }
    }
    if self.dim.Mode&EXTRAPTR != 0 {
        if self.extraptr != nil {
            for _, v := range self.extraptr {
                bytes[c] = v
                c++
            }
        } else {
            for j := 0; j < int(self.PointerSize()); j++ {
                bytes[c] = 0
                c++
            }
        }
    }
    return bytes, true
}

func DeserializeFromFile(bf *BlockFile, dim *BlockDimensions, pos ByteSlice) (*KeyBlock, bool) {
    var bytes []byte
    {
        var ok bool
        bytes, ok = bf.ReadBlock(int64(pos.Int64()), dim.BlockSize)
        if !ok {
            return nil, false
        }
        if !dim.Valid() {
            return nil, false
        }
    }
    return Deserialize(bf, dim, bytes, pos)
}

func Deserialize(bf *BlockFile, dim *BlockDimensions, bytes []byte, pos ByteSlice) (*KeyBlock, bool) {
    b := newKeyBlock(bf, pos, dim)
    c := 5
    if dim.Mode != bytes[0] {
        fmt.Println("Block mode != too dim.Mode")
        return nil, false
    }
    b.rec_count = ByteSlice(bytes[1:3]).Int16()
    b.ptr_count = ByteSlice(bytes[3:5]).Int16()
    for i := 0; i < len(b.records); i++ {
        if i >= int(b.rec_count) {
            c += int(b.KeySize() + b.RecordSize())
            continue
        }
        rec := b.NewRecord(bytes[c : c+int(b.KeySize())])
        c += int(b.KeySize())
        if b.dim.RecordSize() > 0 {
            for _, field := range rec.data {
                for j, _ := range field {
                    field[j] = bytes[c]
                    c++
                }
            }
        }
        b.records[i] = rec
    }
    for i := 0; i < int(b.ptr_count) && b.PointerSize() > 0; i++ {
        if i >= int(b.ptr_count) {
            c += int(b.PointerSize())
            continue
        }
        ptr := make(ByteSlice, b.PointerSize())
        for j, _ := range ptr {
            ptr[j] = bytes[c]
            c++
        }
        b.pointers[i] = ptr
    }
    if b.dim.Mode&EXTRAPTR != 0 {
        ptr := make(ByteSlice, b.PointerSize())
        for j, _ := range ptr {
            ptr[j] = bytes[c]
            c++
        }
        b.extraptr = ptr
    }
    return b, true
}

func (b *KeyBlock) find(k ByteSlice) (int, bool) {
    var l int = 0
    var r int = int(b.rec_count) - 1
    var m int
    for l <= r {
        m = ((r - l) >> 1) + l
        if b.records[m] == nil || k.Lt(b.records[m].GetKey()) {
            r = m - 1
        } else if k.Eq(b.records[m].GetKey()) {
            for j := m; j >= 0; j-- {
                if j == 0 || !k.Eq(b.records[j-1].GetKey()) {
                    return j, true
                }
            }
        } else {
            l = m + 1
        }
    }
    return l, false
}


func (b *KeyBlock) String() string {
    if b == nil {
        return "<nil KeyBlock>"
    }
    s := "Dimensions: " + fmt.Sprintln(b.dim)
    s += "Position: " + fmt.Sprintln(b.Position())
    s += "rec_count: " + fmt.Sprintln(b.rec_count)
    s += "ptr_count: " + fmt.Sprintln(b.ptr_count)
    s += "records: " + fmt.Sprintln(b.records)
    s += "pointers: " + fmt.Sprintln(b.pointers)
    s += "extra pointer: " + fmt.Sprintln(b.extraptr)
    return s
}
