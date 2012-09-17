package keyblock

import "fmt"
import . "file-structures/block/byteslice"

type Record struct {
    dim  *BlockDimensions
    record ByteSlice
}
type RecordsSlice []*Record

func newRecord(key ByteSlice, dim *BlockDimensions) *Record {
    self := new(Record)
    self.dim = dim
    self.record = make([]byte, self.Size())
    self.SetKey(key)
    return self
}

func (r *Record) Size() uint32 {
    return r.dim.KeySize + r.dim.RecordSize()
}

func (r *Record) KeySize() uint32 {
    return r.dim.KeySize
}

func (r *Record) Fields() uint32 {
    return uint32(len(r.dim.RecordFields))
}

func (r *Record) getFieldOffset(i uint32) uint32 {
    offset := r.KeySize()
    for j := uint32(0); j < i; j++ {
        offset += r.dim.RecordFields[j]
    }
    return offset
}

func (r *Record) Get(i uint32) ByteSlice {
    offset := r.getFieldOffset(i)
    ret := make([]byte, r.dim.RecordFields[i])
    copy(ret, r.record[offset:offset+r.dim.RecordFields[i]])
    return ret
}

func (r *Record) Set(i uint32, val ByteSlice) {
    offset := r.getFieldOffset(i)
    for j, v := range val {
        r.record[offset+uint32(j)] = v
    }
}

func (r *Record) SetKey(key ByteSlice) {
    copy(r.record[0:r.KeySize()], key)
}

func (r *Record) GetKey() ByteSlice {
    key := make([]byte, r.KeySize())
    copy(key, r.record[0:r.KeySize()])
    return key
}

func (r *Record) AllFields() [][]byte {
    dataCopy := make([][]byte, r.Fields())
    for i := range dataCopy {
        dataCopy[i] = r.Get(uint32(i))
    }
    return dataCopy
}

func (r *Record) Bytes() []byte {
    return r.record
}

func (r *Record) SetBytes(bytes []byte) bool {
    if uint32(len(bytes)) != r.Size() {
        return false
    }
    r.record = bytes
    return true
}

func (self *Record) String() string {
    if self == nil {
        return "<nil>"
    }
    return fmt.Sprintf("{%v, data=%v}", self.GetKey(), self.AllFields())
}

func (recs RecordsSlice) String() string {
    s := "{"
    for i, rec := range recs {
        s += fmt.Sprint(rec)
        if i+1 < len(recs) {
            s += ", "
        }
    }
    s += "}"
    return s
}

