package keyblock

import "fmt"
import . "file"
import . "byteslice"

const BLOCKHEADER = 5

type key_block struct {
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

func NewKeyBlock(bf *BlockFile, dim *BlockDimensions) (*key_block, bool) {
    if size, ok := bf.Size(); ok {
        b := newKeyBlock(bf, ByteSlice64(size), dim)
        bf.Allocate(uint32(size) + dim.BlockSize)
        return b, true
    }
    return nil, false
}

func newKeyBlock(bf *BlockFile, pos ByteSlice, dim *BlockDimensions) *key_block {
    n := dim.NumberOfKeysInBlock()
    //     fmt.Println(n)
    self := new(key_block)
    self.bf = bf
    self.dim = dim
    self.position = pos
    self.rec_count = 0
    self.ptr_count = 0
    if self.dim.Mode&RECORDS != 0 {
        self.records = make(RecordsSlice, n)
    }
    if self.dim.Mode&POINTERS != 0 {
        self.pointers = make([]ByteSlice, n+1)
    }
    return self
}

func (self *key_block) NewRecord(key ByteSlice) *record {
    return newRecord(key, self.dim)
}

func (self *key_block) Size() uint32        { return self.dim.BlockSize }
func (self *key_block) RecordSize() uint32  { return self.dim.RecordSize() }
func (self *key_block) KeySize() uint32     { return self.dim.KeySize }
func (self *key_block) PointerSize() uint32 { return self.dim.PointerSize }
func (self *key_block) MaxRecordCount() uint16 {
    return uint16(len(self.records))
}
func (self *key_block) RecordCount() uint16  { return self.rec_count }
func (self *key_block) PointerCount() uint16 { return self.ptr_count }
func (self *key_block) Position() ByteSlice  { return self.position }

func (self *key_block) SetExtraPtr(ptr ByteSlice) bool {
    if self.dim.Mode&EXTRAPTR != 0 && len(ptr) == int(self.dim.PointerSize) {
        self.extraptr = ptr
        return true
    }
    return false
}

func (self *key_block) GetExtraPtr() (ByteSlice, bool) {
    if self.dim.Mode&EXTRAPTR != 0 {
        return self.extraptr, true
    }
    return nil, false
}

// TODO: Support Multiple Keys
func (b *key_block) Add(r *record) (int, bool) {
    if b.RecordCount() >= b.MaxRecordCount() {
        return -1, false
    }
    //     fmt.Println()
    //     fmt.Println(r)
    i, _ := b.find(r.key)
    fmt.Printf("i=%v, k=%v\n", i, r.GetKey())
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

func (self *key_block) InsertPointer(i int, ptr ByteSlice) bool {
    if self.dim.Mode&POINTERS == 0 {
        return false
    }
    //     fmt.Println(self.PointerSize())
    if ptr == nil || uint32(len(ptr)) != self.PointerSize() ||
        i > int(self.PointerCount()) || self.PointerCount() >= self.MaxRecordCount()+1 {
        return false
    }
    j := len(self.records)
    self.pointers[j] = self.pointers[j-1]
    j -= 1
    for ; j > int(i); j-- {
        self.pointers[j] = self.pointers[j-1]
    }
    self.pointers[i] = ptr
    self.ptr_count += 1
    return true
}

func (self *key_block) SetPointer(i int, ptr ByteSlice) bool {
    if self.dim.Mode&POINTERS == 0 {
        return false
    }
    if i > int(self.PointerCount()) {
        return false
    }
    self.pointers[i] = ptr
    return true
}

func (self *key_block) Find(k ByteSlice) (int, *record, ByteSlice, ByteSlice, bool) {
    i, ok := self.find(k);
    if  ok {
        return i, self.records[i], self.pointers[i], self.pointers[i+1], true
    }
    return i, nil, nil, nil, false
}

func (self *key_block) Get(i int) (*record, ByteSlice, ByteSlice, bool) {
    if i < int(self.RecordCount()) {
        return self.records[i], self.pointers[i], self.pointers[i+1], true
    }
    return nil, nil, nil, false
}

func (self *key_block) GetPointer(i int) (ByteSlice, bool) {
    if i < int(self.PointerCount()) {
        return self.pointers[i], true
    }
    return nil, false
}

func (self *key_block) PointerIndex(ptr ByteSlice) (int, bool) {
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

func (self *key_block) Remove(k ByteSlice) (int, bool) {
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

func (self *key_block) RemoveAtIndex(i int) (bool) {
    if i >= int(self.PointerCount()) { return false }
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

func (self *key_block) RemovePointer(i int) bool {
    if self.dim.Mode&POINTERS == 0 {
        return false
    }
    if i > int(self.PointerCount()) {
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

func (self *key_block) SerializeToFile() bool {
    if bytes, ok := self.Serialize(); ok {
        return self.bf.WriteBlock(int64(self.Position().Int64()), bytes)
    }
    return false
}

func (self *key_block) Serialize() ([]byte, bool) {
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

func DeserializeFromFile(bf *BlockFile, dim *BlockDimensions, pos ByteSlice) (*key_block, bool) {
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

func Deserialize(bf *BlockFile, dim *BlockDimensions, bytes []byte, pos ByteSlice) (*key_block, bool) {
    b := newKeyBlock(bf, pos, dim)
    c := 5
    if dim.Mode != bytes[0] { return nil, false }
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

func (b *key_block) find(k ByteSlice) (int, bool) {
    var l int = 0
    var r int = int(b.rec_count) - 1
    var m int
    for l <= r {
        m = ((r-l)>>1) + l
        if b.records[m] == nil || k.Lt(b.records[m].GetKey()) {
            r = m - 1
        } else if k.Eq(b.records[m].GetKey()) {
            for j := m; j >= 0; j-- {
                if j == 0 || !k.Eq(b.records[j-1].GetKey()) { return j, true }
            }
        } else {
            l = m + 1
        }
    }
    return l, false
}

func (b *key_block) String() string {
    if b == nil {
        return "<nil> key_block"
    }
    s := "Dimensions: " + fmt.Sprintln(b.dim)
    s += "rec_count: " + fmt.Sprintln(b.rec_count)
    s += "ptr_count: " + fmt.Sprintln(b.ptr_count)
    s += "records: " + fmt.Sprintln(b.records)
    s += "pointers: " + fmt.Sprintln(b.pointers)
    s += "extra pointer: " + fmt.Sprintln(b.extraptr)
    return s
}
