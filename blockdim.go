package keyblock

import "fmt"

const (
    RECORDS = 1 << iota
    POINTERS
    EXTRAPTR
)

type blockDimensions struct {
    Mode         uint8
    BlockSize    uint32
    KeySize      uint32
    PointerSize  uint32
    RecordFields []uint32
}

func BlockDimensions(Mode uint8, BlockSize, KeySize, PointerSize uint32, RecordFields []uint32) (*blockDimensions, bool) {
    dim := blockDimensions{Mode, BlockSize, KeySize, PointerSize, RecordFields}
    if !dim.Valid() {
        return nil, false
    }
    return &dim, true
}

func (self *blockDimensions) NumberOfKeysInBlock() int {
    var n int
    if self.Mode&EXTRAPTR != 0 {
        n = int((self.BlockSize - self.PointerSize - BLOCKHEADER) /
            (self.RecordSize() + self.KeySize))
    } else {
        n = int((self.BlockSize - self.PointerSize - BLOCKHEADER) /
            (self.RecordSize() + self.KeySize + self.PointerSize))
    }
    return n
}

func (self *blockDimensions) RecordSize() uint32 {
    sum := uint32(0)
    for _, v := range self.RecordFields {
        sum += v
    }
    return sum
}

func (self *blockDimensions) Valid() bool {
    if self.KeySize <= 0 {
        return false
    }
    switch self.Mode {
    case RECORDS:
        if self.RecordSize() > 0 && self.PointerSize == 0 &&
            self.BlockSize >= self.RecordSize()+self.KeySize {
            return true
        } else {
            return false
        }
    case POINTERS:
        if self.PointerSize > 0 && self.RecordSize() == 0 &&
            self.BlockSize >= self.PointerSize+self.KeySize {
            return true
        } else {
            return false
        }
    case RECORDS | POINTERS, RECORDS | EXTRAPTR:
        if self.RecordSize() > 0 && self.PointerSize > 0 &&
            self.BlockSize >= self.PointerSize+self.RecordSize()+self.KeySize {
            return true
        } else {
            return false
        }
    case EXTRAPTR | RECORDS | POINTERS, EXTRAPTR, POINTERS | EXTRAPTR:
        return false
    }
    return false
}

func (self *blockDimensions) String() string {
    return fmt.Sprintf(
        "{Mode = %v, BlockSize = %v, KeySize = %v, PointerSize = %v, RecordFields = %v}",
        self.Mode, self.BlockSize, self.KeySize, self.PointerSize, self.RecordFields)
}
