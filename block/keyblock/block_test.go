package keyblock

import "testing"
import . "block/byteslice"

func TestAdd(t *testing.T) {
    dim,_ := NewBlockDimensions(POINTERS|EQUAPTRS, 128, 8, 8, nil)
    self := newKeyBlock(nil, nil, dim)
    if _, ok := self.Add(dim.NewRecord(ByteSlice64(1))); !ok {
        t.Errorf("could not insert %v\n%v", 1, self)
    }
}

func TestCount(t *testing.T) {
    dim,_ := NewBlockDimensions(POINTERS|EQUAPTRS, 128, 8, 8, nil)
    self := newKeyBlock(nil, nil, dim)
    for i := 0; i < 6; i++ {
        if _, ok := self.Add(dim.NewRecord(ByteSlice64(1))); !ok {
            t.Errorf("could not insert %v\n%v", 1, self)
        }
    }
    if self.Count(ByteSlice64(1)) != 6 {
        t.Errorf("self.Count returned %v expected %v", self.Count(ByteSlice64(1)), 6)
    }
}

func TestFindAll(t *testing.T) {
    dim,_ := NewBlockDimensions(RECORDS, 128, 8, 0, ([]uint32{4}))
    self := newKeyBlock(nil, nil, dim)
    for i := 0; i < 6; i++ {
        rec := dim.NewRecord(ByteSlice64(1))
        rec.Set(0, ByteSlice32(uint32(i)))
        if _, ok := self.Add(rec); !ok {
            t.Errorf("could not insert %v\n%v", 1, self)
        }
    }
    records, ack := self.FindAll(ByteSlice64(1))
    i := 5
    for rec := range records {
        if !rec.Get(0).Eq(ByteSlice32(uint32(i))) {
            t.Errorf("\n\nexpected %v as the value of the record got %v\n\n%v", i, rec.Get(0).Int32(), self)
        }
        i--
        ack <- true
    }
}
