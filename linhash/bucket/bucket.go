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
    h := new_header(keysize, valsize, false)
    h.blocks = 1
    blk.SetHeader(h)
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
    if len(self.blocks) == 1 {
        self.header.next = blk.key
    } else {
        h := last_blk.Header()
        h.next = blk.key
        last_blk.SetHeader(h)
    }
    self.blocks = append(self.blocks, blk)
    self.header.blocks += 1
    self.records = self._records()
    return self.save()
}

func (self *BlockTable) remove_block() (err error) {
    if len(self.blocks) <= 1 {
        return fmt.Errorf("Cannot remove any more blocks")
    }
    freed := self.blocks[len(self.blocks)-1]
    self.header.blocks -= 1
    self.blocks = self.blocks[:len(self.blocks)-1]
    last_blk := self.blocks[len(self.blocks)-1]
    if len(self.blocks) == 1 {
        self.header.next = 0
    } else {
        h := last_blk.Header()
        h.next = 0
        last_blk.SetHeader(h)
    }
    if err := self.file.Free(freed.key); err != nil {
        return err
    }
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

func (self record_slice) find_all(key bs.ByteSlice) (found record_slice) {
    found = make(record_slice, 0, 5)
    i, ok := self.find(key)
    if !ok {
        return found
    }
    for ; i < len(self); i++ {
        if key.Eq(self[i].key) {
            found = append(found, self[i])
        } else {
            break
        }
    }
    return found
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
    return self.put(key, value, func(x *record) bool { return true })
}

func (self *BlockTable) put(key, value bs.ByteSlice, doreplace func(*record)bool) (err error) {
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
    replace := false
    if found {
        for j := i; j < len(records); j++ {
            if key.Eq(records[j].key) {
                replace = doreplace(records[j])
                if replace {
                    i = j
                    break
                }
            } else {
                break
            }
        }
    }
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

func (self *BlockTable) remove_index(i int) (err error) {
    all_records := self.records
    for ; i < len(all_records)-1; i++ {
        cur := all_records[i]
        next := all_records[i+1]
        copy(cur.key, next.key)
        copy(cur.value, next.value)
    }
    self.header.records -= 1
    if (int(self.header.records) / self.records_per_blk()) + 1 < len(self.blocks) {
        if err := self.remove_block(); err != nil {
            return err
        }
    }
    return self.save()
}

func (self *BlockTable) Remove(key bs.ByteSlice) (err error) {
    all_records := self.records
    records := record_slice(all_records[:self.header.records])
    i, ok := records.find(key)
    if !ok {
        return fmt.Errorf("Key not found!")
    }
    return self.remove_index(i)
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

func (self *HashBucket) Has(hash, key bs.ByteSlice) bool {
    all_records := self.bt.records
    records := record_slice(all_records[:self.bt.header.records])
    found := records.find_all(hash)
    for _, rec := range found {
        k2, _, err := self.kv.Get(rec.value)
        if err != nil {
            panic(err)
        }
        if key.Eq(k2) {
            return true
        }
    }
    return false
}

func (self *HashBucket) Get(hash, key bs.ByteSlice) (value bs.ByteSlice, err error) {
    all_records := self.bt.records
    records := record_slice(all_records[:self.bt.header.records])
    found := records.find_all(hash)
    for _, rec := range found {
        k2, value, err := self.kv.Get(rec.value)
        if err != nil {
            return nil, err
        }
        if key.Eq(k2) {
            return value, nil
        }
    }
    return nil, fmt.Errorf("Key not found")
}

func (self *HashBucket) Put(hash, key, value bs.ByteSlice) (err error) {
    defer func() {
        if e := recover(); e != nil {
            err = e.(error)
        }
    }()
    bytes, err := self.kv.Put(key, value)
    if err != nil {
        return err
    }
    err = self.bt.put(hash, bytes, func(rec *record) bool {
        k2, _, err := self.kv.Get(rec.value)
        if err != nil { panic(err) }
        return key.Eq(k2)
    })
    if err != nil {
        return err
    }
    return self.bt.save()
}

func (self *HashBucket) Remove(hash, key bs.ByteSlice) (err error) {
    all_records := self.bt.records
    records := record_slice(all_records[:self.bt.header.records])
    i, found := records.find(hash)
    if found {
        for j := i; j < len(records); j++ {
            if hash.Eq(records[j].key) {
                k2, _, err := self.kv.Get(records[j].value)
                if err != nil {
                    return err
                }
                if key.Eq(k2) {
                    return self.bt.remove_index(j)
                }
            } else {
                break
            }
        }
    }
    return fmt.Errorf("Key not found")
}

