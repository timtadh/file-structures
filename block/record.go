package keyblock

import "fmt"
import . "byteslice"

type Record struct {
    dim  *BlockDimensions
    key  ByteSlice
    data [][]byte
}
type RecordsSlice []*Record

// TODO: sanity check fields verses size
func newRecord(key ByteSlice, dim *BlockDimensions) *Record {
    self := new(Record)
    self.key = key
    self.dim = dim
    self.data = make([][]byte, len(self.dim.RecordFields))
    for i := 0; i < len(self.data); i++ {
        self.data[i] = make([]byte, self.dim.RecordFields[i])
    }
    return self
}
func (r *Record) Size() uint32             { return r.dim.RecordSize() }
func (r *Record) KeySize() uint32          { return r.dim.KeySize }
func (r *Record) Fields() uint32           { return uint32(len(r.data)) }
func (r *Record) Get(i uint32) []byte      { return r.data[i] }
func (r *Record) Set(i uint32, val []byte) { r.data[i] = val }
func (r *Record) SetKey(k ByteSlice)       { r.key = k }
func (r *Record) GetKey() ByteSlice        { return r.key }

func (self *Record) String() string {
    if self == nil {
        return "<nil>"
    }
    return fmt.Sprintf("{%v, data=%v}", self.key.Int64(), self.data)
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
