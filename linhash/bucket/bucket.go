package bucket

import (
    "fmt"
)

import (
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
)

type KVStore interface {
    Size() uint8
    Get(bytes bs.ByteSlice) (key, value bs.ByteSlice, err error)
    Put(key, value bs.ByteSlice) (bytes bs.ByteSlice, err error)
    Update(bytes, key, value bs.ByteSlice) (rbytes bs.ByteSlice, err error)
    Remove(bytes bs.ByteSlice) (err error)
}

type BlockTable struct {
    file file.BlockDevice
    key int64
    header *header
    blocks []*block
    records []*record
}

type HashBucket struct {
    bt *BlockTable
    kv KVStore
}

func NewBlockTable(file file.BlockDevice, keysize, valsize uint8) (*BlockTable, error) {
    blk, err := allocBlock(file)
    if err != nil {
        return nil, err
    }
    blk.SetHeader(new_header(keysize, valsize, false))
    if err := blk.WriteBlock(file); err != nil {
        return nil, err
    }
    self := &BlockTable{
        file: file,
        key: blk.key,
        header: blk.Header(),
        blocks: []*block{blk},
    }
    self.records = self._records()
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
    for i := 1; i < int(header.blocks); i++ {
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


func ReadBlockTable(file file.BlockDevice, key int64) (self *BlockTable, err error) {
    blocks, err := blocks(file, key)
    if err != nil {
        return nil, err
    }
    self = &BlockTable{
        file: file,
        key: blocks[0].key,
        header: blocks[0].Header(),
        blocks: blocks,
    }
    self.records = self._records()
    return self, nil
}

func (self *BlockTable) save() (err error) {
    self.blocks[0].SetHeader(self.header)
    for _, blk := range self.blocks {
        err := blk.WriteBlock(self.file)
        if err != nil {
            return err
        }
    }
    return nil
}

func (self *BlockTable) Key() int64 {
    return self.key
}

func (self *BlockTable) records_per_blk() int {
    blk := self.blocks[0]
    h := self.header
    keysize := int(h.keysize)
    valsize := int(h.valsize)
    rec_size := keysize + valsize
    return len(blk.data)/rec_size
}

func (self *BlockTable) _records() (records []*record) {
    keysize := int(self.header.keysize)
    valsize := int(self.header.valsize)
    rec_size := keysize + valsize
    length := self.records_per_blk()
    records = make([]*record, length*len(self.blocks))
    offset := 0
    for j, blk := range self.blocks {
        blk_offset := 0
        for i := 0; i < length; i++ {
            end := blk_offset + rec_size
            recbytes := blk.data[blk_offset:end]
            records[j*length+i] = &record{
                key: recbytes[:keysize],
                value: recbytes[keysize:],
            }
            blk_offset = end
        }
        offset += length
    }
    return records
}

func (self *BlockTable) add_block() (err error) {
    blk, err := allocBlock(self.file)
    if err != nil {
        return err
    }
    myh := blk.Header()
    myh.set_flags(true)
    blk.SetHeader(myh)

    last_blk := self.blocks[len(self.blocks)-1]
    h := last_blk.Header()
    h.next = blk.key
    last_blk.SetHeader(h)
    self.blocks = append(self.blocks, blk)
    self.header.blocks += 1
    self.records = self._records()
    return nil
}

type record_slice []*record
func (self record_slice) find(key bs.ByteSlice) (int, bool) {
    var l int = 0
    var r int = len(self) - 1
    var m int
    for l <= r {
        m = ((r - l) >> 1) + l
        if key.Lt(self[m].key) {
            r = m - 1
        } else if key.Eq(self[m].key) {
            for j := m; j >= 0; j-- {
                if j == 0 || !key.Eq(self[j-1].key) {
                    return j, true
                }
            }
        } else {
            l = m + 1
        }
    }
    return l, false
}

func (self *BlockTable) Has(key bs.ByteSlice) bool {
    all_records := self.records
    records := record_slice(all_records[:self.header.records])
    _, ok := records.find(key)
    return ok
}

func (self *BlockTable) Get(key bs.ByteSlice) (value bs.ByteSlice, err error) {
    all_records := self.records
    records := record_slice(all_records[:self.header.records])
    i, ok := records.find(key)
    if !ok {
        return nil ,fmt.Errorf("Key not found!")
    }
    record := records[i]
    return record.value, nil
}

func (self *BlockTable) Put(key, value bs.ByteSlice) (err error) {
    return self.put(key, value, true)
}

func (self *BlockTable) put(key, value bs.ByteSlice, replace bool) (err error) {
    if len(key) != int(self.header.keysize) {
        return fmt.Errorf(
          "Key size is wrong, %d != %d", self.header.keysize, len(key))
    }
    if len(value) > int(self.header.valsize) {
        return fmt.Errorf(
          "Value size is wrong, %d >= %d", self.header.valsize, len(value))
    }
    all_records := self.records
    if len(all_records) <= int(self.header.records) + 1 {
        // alloc another block
        err := self.add_block()
        if err != nil {
            return err
        }
        all_records = self.records
    }
    records := record_slice(all_records[:self.header.records])
    i, found := records.find(key)
    if !found || (found && !replace) {
        j := len(all_records)
        j -= 1
        for ; j > int(i); j-- {
            cur := all_records[j-1]
            next := all_records[j]
            copy(next.key, cur.key)
            copy(next.value, cur.value)
        }
        self.header.records += 1
    }
    spot := all_records[i]
    copy(spot.key, key)
    copy(spot.value, value)
    return self.save()
}

func (self *BlockTable) Remove(key bs.ByteSlice) (err error) {
    all_records := self.records
    records := record_slice(all_records[:self.header.records])
    i, ok := records.find(key)
    if !ok {
        return fmt.Errorf("Key not found!")
    }
    for ; i < len(all_records)-1; i++ {
        cur := all_records[i]
        next := all_records[i+1]
        copy(cur.key, next.key)
        copy(cur.value, next.value)
    }
    self.header.records -= 1
    return self.save()
}

// --------------------------------------------------------------------------------------------------

func NewHashBucket(file file.BlockDevice, hashsize uint8, kv KVStore) (self *HashBucket, err error) {
    if kv == nil {
        return nil, fmt.Errorf("Must have a KVStore")
    }
    bt, err := NewBlockTable(file, hashsize, kv.Size())
    if err != nil {
        return nil, err
    }
    self = &HashBucket{
        bt: bt,
        kv: kv,
    }
    return self, nil
}

func ReadHashBucket(file file.BlockDevice, key int64, kv KVStore) (self *HashBucket, err error) {
    if kv == nil {
        return nil, fmt.Errorf("Must have a KVStore")
    }
    bt, err := ReadBlockTable(file, key)
    if err != nil {
        return nil, err
    }
    self = &HashBucket{
        bt: bt,
        kv: kv,
    }
    return self, nil
}

func (self *HashBucket) Key() int64 {
    return self.bt.Key()
}

func (self *HashBucket) Get(hash, key bs.ByteSlice) (value bs.ByteSlice, err error) {
    return nil, fmt.Errorf("HashBucket.Get Unimplemented")
}

func (self *HashBucket) Put(hash, key, value bs.ByteSlice) (err error) {
    return fmt.Errorf("HashBucket.Put Unimplemented")
}

func (self *HashBucket) Remove(hash, key bs.ByteSlice) (err error) {
    return fmt.Errorf("HashBucket.Remove Unimplemented")
}

