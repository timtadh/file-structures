package linhash

import (
    "fmt"
    "hash/fnv"
)

import (
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
    bucket "file-structures/linhash/bucket"
)

func hash(data []byte) uint64 {
    fnv := fnv.New64a()
    fnv.Write(data)
    return fnv.Sum64()
}

type ctrlblk struct {
    buckets uint32 "number of buckets"
    records uint64 "number of records"
    table int64 "key of bucket translation table"
    i uint8 "number of bits of H(.)"
}

const CONTROLSIZE = 21
func (self *ctrlblk) Bytes() []byte {
    bytes := make([]byte, CONTROLSIZE)
    copy(bytes[0:4], bs.ByteSlice32(self.buckets))
    copy(bytes[4:12], bs.ByteSlice64(self.records))
    copy(bytes[12:20], bs.ByteSlice64(uint64(self.table)))
    bytes[20] = self.i
    return bytes
}

func load_ctrlblk(bytes bs.ByteSlice) (cb *ctrlblk, err error) {
    if len(bytes) < CONTROLSIZE {
        return nil, fmt.Errorf("len(bytes) < %d", CONTROLSIZE)
    }
    cb = &ctrlblk{
        buckets: bytes[0:4].Int32(),
        records: bytes[4:12].Int64(),
        table: int64(bytes[12:20].Int64()),
        i: bytes[20],
    }
    return cb, nil
}

type LinearHash struct {
    file file.BlockDevice
    kv   bucket.KVStore
    ctrl ctrlblk
}

func NewLinearHash(file file.BlockDevice, kv bucket.KVStore) (self *LinearHash, err error) {
    self = &LinearHash{
        file: file,
        kv: kv,
        ctrl: ctrlblk{
        },
    }
    return self, self.write_ctrlblk()
}

func OpenLinearHash(file file.BlockDevice, kv bucket.KVStore) (self *LinearHash, err error) {
    self = &LinearHash{
        file: file,
        kv: kv,
    }
    if err := self.read_ctrlblk(); err != nil {
        return nil, err
    }
    return self, nil
}

func (self *LinearHash) Close() error {
    return self.file.Close()
}

func (self *LinearHash) write_ctrlblk() error {
    return self.file.SetControlData(self.ctrl.Bytes())
}

func (self *LinearHash) read_ctrlblk() error {
    if bytes, err := self.file.ControlData(); err != nil {
        return err
    } else {
        if cb, err := load_ctrlblk(bytes); err != nil {
            return err
        } else {
            self.ctrl = *cb
        }
    }
    return nil
}

func (self *LinearHash) bucket(hash uint64) uint64 {
    i := uint64(self.ctrl.i)
    n := uint64(self.ctrl.buckets)
    m := hash & ((1<<i)-1) // last i bits of hash as bucket number m
    if m < n {
        return m
    } else {
        m = m ^ (1<<(i-1)) // unset the top bit
        if m < n {
            panic(fmt.Errorf("Expected m < self.ctrl.buckets, got %d < %d", m, self.ctrl.buckets))
        }
        return m
    }
}

func (self *LinearHash) Has(key bs.ByteSlice) (has bool, error error) {
    return false, fmt.Errorf("Has Unimplemented")
}

func (self *LinearHash) Put(key bs.ByteSlice, value bs.ByteSlice) (err error) {
    hash := hash(key)
    return fmt.Errorf("Put Unimplemented %v %v", key, hash)
}

func (self *LinearHash) Get(key bs.ByteSlice) (value bs.ByteSlice, err error) {
    return nil, fmt.Errorf("Get Unimplemented")
}

func (self *LinearHash) DefaultGet(key bs.ByteSlice, default_value bs.ByteSlice) (value bs.ByteSlice, err error) {
    return nil, fmt.Errorf("DefaultGet Unimplemented")
}

func (self *LinearHash) Remove(key bs.ByteSlice) (err error) {
    return fmt.Errorf("Remove Unimplemented")
}

