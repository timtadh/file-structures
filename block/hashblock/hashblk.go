package hashblock

import (
    "fmt"
)

import (
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
)

type KVStore interface {
    Get(bytes bs.ByteSlice) (key, value bs.ByteSlice, err error)
    Put(key, value bs.ByteSlice) (bytes bs.ByteSlice, err error)
    Update(bytes, key, value bs.ByteSlice) (rbytes bs.ByteSlice, err error)
    Remove(bytes bs.ByteSlice) (err error)
}

type HashBlock struct {
    file file.BlockDevice
    key int64
    header *header
    blocks []*block
    kv KVStore
}

func NewHashBlock(file file.BlockDevice, hashsize, valsize uint8, kv KVStore) (self *HashBlock, err error) {
    if kv == nil {
        return nil, fmt.Errorf("Must have a KVStore")
    }
    blk, err := allocBlock(file)
    if err != nil {
        return nil, err
    }
    blk.SetHeader(new_header(hashsize, valsize, false))
    if err := blk.WriteBlock(file); err != nil {
        return nil, err
    }
    self = &HashBlock{
        key: blk.key,
        header: blk.Header(),
        blocks: []*block{blk},
        kv: kv,
    }
    return self, nil
}


func blocks(file file.BlockDevice, key int64) (blocks []*block, err error) {
    start_blk, err := readBlock(file, key)
    if err != nil {
        return nil, err
    }
    header := start_blk.Header()
    blocks = append(blocks, start_blk)
    pblk := start_blk
    for i := 1; i < header.blocks(); i++ {
        ph := pblk.Header()
        blk, err := readBlock(file, ph.next)
        if err != nil {
            return nil, err
        }
        blocks = append(blocks, blk)
        pblk = blk
    }
    return blocks, nil
}


func ReadHashBlock(file file.BlockDevice, key int64, kv KVStore) (self *HashBlock, err error) {
    if kv == nil {
        return nil, fmt.Errorf("Must have a KVStore")
    }
    blocks, err := blocks(file, key)
    if err != nil {
        return nil, err
    }
    self = &HashBlock{
        key: key,
        header: blocks[0].Header(),
        blocks: blocks,
        kv: kv,
    }
    return self, nil
}

func (self *HashBlock) Key() int64 {
    return self.key
}

type record struct {
    bytes bs.ByteSlice,
    key bs.ByteSlice,
    value bs.ByteSlice,
}

func (self *HashBlock) records_per_block() int {
    rec_size := int(header.keysize) + int(header.valsize)
}

func (self *HashBlock) records() (records []*record) {
}

func (self *HashBlock) Get(hash, key bs.ByteSlice) (value bs.ByteSlice, err error) {
    return nil, fmt.Errorf("HashBlock.Get Unimplemented")
}

func (self *HashBlock) Put(hash, key, value bs.ByteSlice) (err error) {
    return nil, fmt.Errorf("HashBlock.Get Unimplemented")
}

func (self *HashBlock) Remove(hash, key bs.ByteSlice) (err error) {
    return nil, fmt.Errorf("HashBlock.Get Unimplemented")
}

