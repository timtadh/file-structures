package bptree

import "fmt"
import . "file-structures/block/keyblock"
import . "file-structures/block/byteslice"

type tmprec struct {
    exdim  *BlockDimensions
    indim  *BlockDimensions
    key    ByteSlice
    record []ByteSlice
}

func pkg_rec(bptree *BpTree, key ByteSlice, rec []ByteSlice) (*tmprec, bool) {
    if !bptree.ValidateKey(key) || !bptree.ValidateRecord(rec) {
        return nil, false
    }
    self := new(tmprec)
    self.exdim = bptree.external
    self.indim = bptree.internal
    self.key = key
    self.record = rec
    return self, true
}

func rec_to_tmp(bptree *BpTree, rec *Record) *tmprec {
    self := new(tmprec)
    self.exdim = bptree.external
    self.indim = bptree.internal
    self.key = rec.GetKey()
    self.record = make([]ByteSlice, rec.Fields())
    for i := 0; i < int(rec.Fields()); i++ {
        self.record[i] = rec.Get(uint32(i))
    }
    return self
}

func (self *tmprec) makerec(rec *Record) *Record {
    for i, f := range self.record {
        rec.Set(uint32(i), f)
    }
    return rec
}

func (self *tmprec) external() *Record { return self.makerec(self.exdim.NewRecord(self.key)) }

func (self *tmprec) internal() *Record { return self.indim.NewRecord(self.key) }

func (self *tmprec) String() string {
    if self == nil {
        return "<nil tmprec>"
    }
    s := "tmprec:\n{\n"
    s += fmt.Sprintln("  exdim:", self.exdim)
    s += fmt.Sprintln("  indim:", self.indim)
    s += fmt.Sprintln("  key:", self.key)
    s += fmt.Sprintln("  record:", self.record)
    s += "}\n"
    return s
}
