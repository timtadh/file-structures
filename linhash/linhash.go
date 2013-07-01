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

const HASHSIZE = 8
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
    table *bucket.BlockTable
    ctrl ctrlblk
}

func NewLinearHash(file file.BlockDevice, kv bucket.KVStore) (self *LinearHash, err error) {
    const NUMBUCKETS = 16
    const I = 5
    table, err := bucket.NewBlockTable(file, 4, 8)
    if err != nil {
        return nil, err
    }
    for n := uint32(0); n < NUMBUCKETS; n++ {
        bkt, err := bucket.NewHashBucket(file, HASHSIZE, kv)
        if err != nil {
            return nil, err
        }
        err = table.Put(bs.ByteSlice32(n), bs.ByteSlice64(uint64(bkt.Key())))
        if err != nil {
            return nil, err
        }
    }
    self = &LinearHash{
        file: file,
        kv: kv,
        table: table,
        ctrl: ctrlblk{
            buckets: NUMBUCKETS,
            records: 0,
            table: table.Key(),
            i: I,
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
    table, err := bucket.ReadBlockTable(self.file, self.ctrl.table)
    if err != nil {
        return err
    }
    self.table = table
    return nil
}

func (self *LinearHash) bucket(hash uint64) uint32 {
    i := uint64(self.ctrl.i)
    n := uint64(self.ctrl.buckets)
    m := hash & ((1<<i)-1) // last i bits of hash as bucket number m
    if m < n {
        return uint32(m)
    } else {
        m = m ^ (1<<(i-1)) // unset the top bit
        if m >= n {
            panic(fmt.Errorf("Expected m < self.ctrl.buckets, got %d >= %d", m, self.ctrl.buckets))
        }
        return uint32(m)
    }
}

func (self *LinearHash) split_needed() bool {
    records := float64(self.ctrl.records)
    buckets := float64(self.ctrl.buckets)
    records_per_block := float64(self.table.RecordsPerBlock())
    if records/buckets/records_per_block > .8 {
        return true
    }
    return false
}

func (self *LinearHash) get_bucket(bkt_idx uint32) (*bucket.HashBucket, error) {
    bkt_key, err := self.table.Get(bs.ByteSlice32(bkt_idx))
    if err != nil {
        fmt.Println("Couldn't get bkt_idx out of table", bkt_idx)
        return nil, err
    }
    bkt, err := bucket.ReadHashBucket(self.file, int64(bkt_key.Int64()), self.kv)
    if err != nil {
        fmt.Println("Couldn't read bucket", bkt_key.Int64())
        return nil, err
    }
    return bkt, nil
}

func (self *LinearHash) split() (err error) {
    bkt_idx := self.ctrl.buckets % (1 << (self.ctrl.i - 1))
    bkt, err := self.get_bucket(bkt_idx)
    if err != nil {
        return err
    }
    keys := bkt.Keys()
    self.ctrl.buckets += 1
    if self.ctrl.buckets > (1 << self.ctrl.i) {
        self.ctrl.i += 1
    }
    newbkt, err := bkt.Split(func(key bs.ByteSlice)bool {
        this_idx := self.bucket(key.Int64())
        return this_idx == bkt_idx
    })
    err = self.table.Put(bs.ByteSlice32(self.ctrl.buckets-1), bs.ByteSlice64(uint64(newbkt.Key())))
    if err != nil {
        return err
    }
    err = self.write_ctrlblk()
    if err != nil {
        return err
    }
    for _, key := range keys {
        if has, err := self.Has(key); err != nil {
            return err
        } else if !has {
            hash := bs.ByteSlice64(hash(key))
            in_first := bkt.Has(hash, key)
            in_second := newbkt.Has(hash, key)
            fmt.Println()
            fmt.Println("i", self.ctrl.i, "records", self.ctrl.records, "buckets", self.ctrl.buckets)
            fmt.Println("key", key, "hash", hash)
            fmt.Println("in_first", in_first, bkt_idx)
            fmt.Println("in_second", in_second, self.ctrl.buckets-1)
            return fmt.Errorf("Key went missing during split")
        }
    }
    return nil
}

func (self *LinearHash) Length() int {
    return int(self.ctrl.records)
}

func (self *LinearHash) Has(key bs.ByteSlice) (has bool, error error) {
    hash := hash(key)
    bkt_idx := self.bucket(hash)
    bkt, err := self.get_bucket(bkt_idx)
    if err != nil {
        return false, err
    }
    has = bkt.Has(bs.ByteSlice64(hash), key)
    // fmt.Println("LinearHash.Has", bs.ByteSlice64(hash), key, bkt_idx, has)
    return has, nil
}

func (self *LinearHash) Put(key bs.ByteSlice, value bs.ByteSlice) (err error) {
    hash := hash(key)
    bkt_idx := self.bucket(hash)
    bkt, err := self.get_bucket(bkt_idx)
    if err != nil {
        fmt.Println("Couldn't get bucket idx", bkt_idx)
        return err
    }
    updated, err := bkt.Put(bs.ByteSlice64(hash), key, value)
    if err != nil {
        return err
    }
    if !updated {
        self.ctrl.records += 1
        if self.split_needed() {
            // fmt.Println("did split")
            return self.split()
        }
        // fmt.Println("no split")
        return self.write_ctrlblk()
    }
    return nil
}

func (self *LinearHash) Get(key bs.ByteSlice) (value bs.ByteSlice, err error) {
    hash := hash(key)
    bkt_idx := self.bucket(hash)
    bkt, err := self.get_bucket(bkt_idx)
    if err != nil {
        return nil, err
    }
    return bkt.Get(bs.ByteSlice64(hash), key)
}

func (self *LinearHash) DefaultGet(key bs.ByteSlice, default_value bs.ByteSlice) (value bs.ByteSlice, err error) {
    hash := hash(key)
    hash_bytes := bs.ByteSlice64(hash)
    bkt_idx := self.bucket(hash)
    bkt, err := self.get_bucket(bkt_idx)
    if err != nil {
        return nil, err
    }
    if bkt.Has(hash_bytes, key) {
        return bkt.Get(hash_bytes, key)
    }
    return default_value, nil
}

func (self *LinearHash) Remove(key bs.ByteSlice) (err error) {
    hash := hash(key)
    bkt_idx := self.bucket(hash)
    bkt, err := self.get_bucket(bkt_idx)
    if err != nil {
        return err
    }
    err = bkt.Remove(bs.ByteSlice64(hash), key)
    if err != nil {
        return err
    }
    self.ctrl.records -= 1
    return self.write_ctrlblk()
}

