package hashblock

import (
    "fmt"
)

import (
    bs "file-structures/block/byteslice"
)

const (
    OVERFLOW = 1 << iota
)

const HEADER_SIZE = 18
type header struct {
    flag uint8
    hashsize uint8
    valsize uint8
    records uint32
    next int64
    blocks uint16
}

func (self *header) Bytes() (bytes []byte) {
    bytes = make([]byte, HEADER_SIZE)
    bytes[0] = self.flag
    bytes[1] = self.hashsize
    bytes[2] = self.valsize
    copy(bytes[4:8], bs.ByteSlice32(self.records))
    copy(bytes[8:16], bs.ByteSlice64(uint64(self.next)))
    copy(bytes[16:18], bs.ByteSlice16(self.blocks))
    return bytes
}

func new_header(hashsize, valsize uint8, overflow bool) *header {
    self := &header{hashsize:hashsize, valsize:valsize}
    self.set_flags(overflow)
    return self
}

func load_header(bytes bs.ByteSlice) (h *header, err error) {
    if len(bytes) < HEADER_SIZE {
        return nil, fmt.Errorf("len(bytes) < %d", HEADER_SIZE)
    }
    h = &header{
        flag: bytes[0],
        hashsize: bytes[1],
        valsize: bytes[2],
        records: bytes[4:8].Int32(),
        next: int64(bytes[8:16].Int64()),
        blocks: bytes[16:18].Int16(),
    }
    return h, nil
}

func (self *header) set_next(next int64) *header {
    self.next = next
    return self
}

func (self *header) set_flags(overflow bool) *header {
    flag := uint8(0)
    if overflow {
        flag |= OVERFLOW
    }
    self.flag = flag
    return self
}

func (self *header) flags() (overflow bool) {
    if self.flag&(OVERFLOW) == OVERFLOW {
        overflow = true
    }
    return
}

