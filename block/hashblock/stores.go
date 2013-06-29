package hashblock

import (
    "fmt"
)

import (
    bs "file-structures/block/byteslice"
)

type BytesStore struct{}

func NewBytesStore() *BytesStore {
    return &BytesStore{}
}

const MAX_BYTES_STORE_SIZE = 255 - 2
type _bytes_kv struct {
    keysize uint8
    valsize uint8
    key bs.ByteSlice
    val bs.ByteSlice
    block bs.ByteSlice
}

func (self *_bytes_kv) Bytes() []byte {
    size := int(self.keysize + self.valsize + 2)
    if len(self.key) != int(self.keysize) {
        panic(fmt.Errorf("len(self.key) != self.keysize, %d != %d", len(self.key), self.keysize))
    }
    if len(self.val) != int(self.valsize) {
        panic(fmt.Errorf("len(self.val) != self.valsize, %d != %d", len(self.val), self.valsize))
    }
    if len(self.block) != size {
        panic(fmt.Errorf("len(self.block) != size, %d != %d", len(self.block), size))
    }
    return self.block
}

func new_bytes_kv(key, val bs.ByteSlice) *_bytes_kv {
    if len(key) + len(val) + 2 > MAX_BYTES_STORE_SIZE {
        panic(fmt.Errorf("key val too big"))
    }
    block := make(bs.ByteSlice, len(key) + len(val) + 2)
    block[0] = uint8(len(key))
    block[1] = uint8(len(val))
    copy(block[2:2+len(key)], key)
    copy(block[2+len(key):], val)
    return &_bytes_kv{
        keysize: uint8(len(key)),
        valsize: uint8(len(val)),
        key: block[2:2+len(key)],
        val: block[2+len(key):],
        block: block,
    }
}

func load_bytes_kv(bytes bs.ByteSlice) *_bytes_kv {
    keysize := bytes[0]
    valsize := bytes[1]
    key := bytes[2:2+keysize]
    val := bytes[2+keysize:]
    if len(key) != int(keysize) {
        panic(fmt.Errorf("len(key) != keysize, %d != %d", len(key), keysize))
    }
    if len(val) != int(valsize) {
        panic(fmt.Errorf("len(val) != valsize, %d != %d", len(val), valsize))
    }
    return &_bytes_kv{
        keysize: keysize,
        valsize: valsize,
        key: key,
        val: val,
        block: bytes,
    }
}

func (self *BytesStore) Get(bytes bs.ByteSlice) (key, value bs.ByteSlice, err error) {
    if len(bytes) > MAX_BYTES_STORE_SIZE {
        return nil, nil, fmt.Errorf("in ByteStore.Get len(bytes) > %d", MAX_BYTES_STORE_SIZE)
    }
    kv := load_bytes_kv(bytes)
    return kv.key, kv.val, nil
}

func (self *BytesStore) Put(key, value bs.ByteSlice) (bytes bs.ByteSlice, err error) {
    if len(key) + len(value) + 2 > MAX_BYTES_STORE_SIZE {
        return nil, fmt.Errorf("in ByteStore.Get len(key) + len(val) + 2 > %d", MAX_BYTES_STORE_SIZE)
    }
    kv := new_bytes_kv(key, value)
    return kv.Bytes(), nil
}

func (self *BytesStore) Update(bytes, key, value bs.ByteSlice) (rbytes bs.ByteSlice, err error) {
    return self.Put(key, value)
}

func (self *BytesStore) Remove(bytes bs.ByteSlice) (err error) {
    return nil
}

