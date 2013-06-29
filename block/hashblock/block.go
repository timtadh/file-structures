package hashblock

import (
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
)

type block struct {
    key int64
    block bs.ByteSlice
    header bs.ByteSlice
    data bs.ByteSlice
}

func datasize(file file.BlockDevice) int64 {
    return int64(file.BlockSize()) - HEADER_SIZE
}

func (self *block) datasize() int64 {
    return int64(len(self.data))
}

func (self *block) blocksize() int64 {
    return int64(len(self.block))
}

func load_block(key int64, bytes bs.ByteSlice) (blk *block) {
    return &block{
        key: key,
        block: bytes,
        header: bytes[:HEADER_SIZE],
        data: bytes[HEADER_SIZE:],
    }
}

func (self *block) Header() *header {
    h, err := load_header(self.header)
    if err != nil {
        panic(err)
    }
    return h
}

func (self *block) SetHeader(h *header) {
    copy(self.header, h.Bytes())
}

func (self *block) WriteBlock(file file.BlockDevice) error {
    return file.WriteBlock(self.key, self.block)
}

func readBlock(file file.BlockDevice, key int64) (blk *block, err error) {
    bytes, err := file.ReadBlock(key)
    if err != nil {
        return nil, err
    }
    return load_block(key, bytes), err
}

func allocBlock(file file.BlockDevice) (blk *block, err error) {
    key, err := file.Allocate()
    if err != nil {
        return nil, err
    }
    size := file.BlockSize()
    bytes := make(bs.ByteSlice, size)
    return load_block(key, bytes), nil
}

