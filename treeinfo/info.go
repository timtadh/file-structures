package treeinfo

import . "file-structures/block/file"
import . "file-structures/block/byteslice"

const BLOCKSIZE = 4096

type TreeInfo struct {
    file   *BlockFile
    height int
    root   ByteSlice
}

func New(file *BlockFile, h int, r ByteSlice) *TreeInfo {
    self := new(TreeInfo)
    self.file = file
    self.height = h
    self.root = r
    self.Serialize()
    return self
}

func Load(file *BlockFile) *TreeInfo {
    self := new(TreeInfo)
    self.file = file
    self.deserialize()
    return self
}

func (self *TreeInfo) Height() int     { return self.height }
func (self *TreeInfo) Root() ByteSlice { return self.root }
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
    h := ByteSlice32(uint32(self.height))
    i := 0
    for _, b := range h {
        bytes[i] = b
        i++
    }
    for _, b := range self.root {
        bytes[i] = b
        i++
    }
    position := self.file.FirstAllocatedBlock()
    self.file.WriteBlock(position, bytes)
}
func (self *TreeInfo) deserialize() {
    position := self.file.FirstAllocatedBlock()
    bytes, ok := self.file.ReadBlock(position, BLOCKSIZE)
    if ok {
        self.height = int(ByteSlice(bytes[0:4]).Int32())
        self.root = ByteSlice(bytes[4:12])
    }
}
