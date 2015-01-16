package dirty

import "file-structures/block/keyblock"

type DirtyBlocks struct {
	slice []*keyblock.KeyBlock
}

func New(size int) *DirtyBlocks {
	self := new(DirtyBlocks)
	self.slice = make([]*keyblock.KeyBlock, size)[0:0]
	return self
}
func (self *DirtyBlocks) Insert(b *keyblock.KeyBlock) {
	self.slice = self.slice[0 : len(self.slice)+1]
	self.slice[len(self.slice)-1] = b
}
func (self *DirtyBlocks) Sync() {
	for _, b := range self.slice {
		b.SerializeToFile()
	}
}
