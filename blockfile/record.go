package keyblock

import "fmt"
import . "byteslice"

type record struct {
    dim  *blockDimensions
    key  ByteSlice
    data [][]byte
}
type RecordsSlice []*record

// TODO: sanity check fields verses size
func newRecord(key ByteSlice, dim *blockDimensions) *record {
    self := new(record)
    self.key = key
    self.dim = dim
    self.data = make([][]byte, len(self.dim.RecordFields))
    for i := 0; i < len(self.data); i++ {
        self.data[i] = make([]byte, self.dim.RecordFields[i])
    }
    return self
}
func (r *record) Size() uint32             { return r.dim.RecordSize() }
func (r *record) KeySize() uint32          { return r.dim.KeySize }
func (r *record) Fields() uint32           { return uint32(len(r.data)) }
func (r *record) Get(i uint32) []byte      { return r.data[i] }
func (r *record) Set(i uint32, val []byte) { r.data[i] = val }
func (r *record) SetKey(k ByteSlice)       { r.key = k }
func (r *record) GetKey() ByteSlice        { return r.key }

func (self *record) String() string {
    if self == nil {
        return "<nil>"
    }
    return fmt.Sprintf("{%v, data=%v}", self.key, self.data)
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
