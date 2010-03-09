package treeinfo

import . "block/file"
import . "block/byteslice"

// const BLOCKSIZE = 4096
const BLOCKSIZE = 21

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

func (self *TreeInfo) Height() int { return self.height }
func (self *TreeInfo) Root() ByteSlice { return self.root }
func (self *TreeInfo) SetHeight(h int) { self.height = h; self.Serialize() }
func (self *TreeInfo) SetRoot(r ByteSlice) { self.root = r; self.Serialize() }

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
    self.file.WriteBlock(0, bytes)
}
func (self *TreeInfo) deserialize() {
    bytes, ok := self.file.ReadBlock(0, BLOCKSIZE)
    if ok {
        self.height = int(ByteSlice(bytes[0:4]).Int32())
        self.root = ByteSlice(bytes[4:12])
    }
}
