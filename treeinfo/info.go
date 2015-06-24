package treeinfo

import . "file-structures/block/file"
import . "file-structures/block/byteslice"

const BLOCKSIZE = 4096

// const BLOCKSIZE = 1048576

type TreeInfo struct {
	file    *BlockFile
	height  int
	entries uint64
	root    ByteSlice
}

func New(file *BlockFile, h int, r ByteSlice) *TreeInfo {
	self := new(TreeInfo)
	self.file = file
	self.height = h
	self.root = r
	self.entries = 0
	self.Serialize()
	return self
}

func Load(file *BlockFile) *TreeInfo {
	self := new(TreeInfo)
	self.file = file
	self.deserialize()
	return self
}

func (self *TreeInfo) Height() int {
	return self.height
}

func (self *TreeInfo) Root() ByteSlice {
	return self.root
}

func (self *TreeInfo) Entries() uint64 {
	return self.entries
}

func (self *TreeInfo) IncEntries() {
	self.entries += 1
}

func (self *TreeInfo) DecEntries() {
	if self.entries > 0 {
		self.entries -= 1
	}
}

func (self *TreeInfo) SetHeight(h int) {
	self.height = h
	self.Serialize()
}

func (self *TreeInfo) SetRoot(r ByteSlice) {
	self.root = r
	self.Serialize()
}

func (self *TreeInfo) Serialize() {
	bytes := make([]byte, BLOCKSIZE)
	i := 0
	copy(bytes[i:i+4], ByteSlice32(uint32(self.height)))
	i += 4
	copy(bytes[i:i+len(self.root)], self.root)
	i += len(self.root)
	copy(bytes[i:i+8], ByteSlice64(self.entries))
	i += 8
	self.file.WriteBlock(0, bytes)
}

func (self *TreeInfo) deserialize() {
	bytes, ok := self.file.ReadBlock(0, BLOCKSIZE)
	if ok {
		self.height = int(ByteSlice(bytes[0:4]).Int32())
		self.root = ByteSlice(bytes[4:12])
		self.entries = ByteSlice(bytes[12:20]).Int64()
	}
}
