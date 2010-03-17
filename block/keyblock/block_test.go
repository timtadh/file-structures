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
