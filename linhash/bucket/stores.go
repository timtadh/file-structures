package bucket

import (
    "fmt"
)

import (
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
    varchar "file-structures/varchar"
)

const MAX_BYTES_STORE_SIZE = 255 - 2
type BytesStore struct{
    keysize uint8
    valsize uint8
}

func NewBytesStore(keysize, valsize uint8) (*BytesStore, error) {
    if int(keysize) + int(valsize) + 2 > MAX_BYTES_STORE_SIZE {
        return nil, fmt.Errorf("bytes store too big")
    }
    return &BytesStore{keysize, valsize}, nil
}

func (self *BytesStore) Size() uint8 {
    return self.keysize + self.valsize + 2
}

type _bytes_kv struct {
    keysize uint8
    valsize uint8
    key bs.ByteSlice
    val bs.ByteSlice
    block bs.ByteSlice
}

func (self *_bytes_kv) Bytes() []byte {
    size := int(self.keysize) + int(self.valsize) + 2
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
    if len(bytes) > int(self.Size()) {
        return nil, nil, fmt.Errorf("in ByteStore.Get len(bytes) > %d", self.Size())
    }
    kv := load_bytes_kv(bytes)
    return kv.key, kv.val, nil
}

func (self *BytesStore) Put(key, value bs.ByteSlice) (bytes bs.ByteSlice, err error) {
    if len(key) + len(value) + 2 > int(self.Size()) {
        return nil, fmt.Errorf("in ByteStore.Get len(key) + len(val) + 2 > %d", self.Size())
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

// ------------------------------------------------------------------------------------------------


type VarcharStore struct {
    varchar *varchar.Varchar
}

type _varchar_kv struct {
    keysize uint32
    valsize uint32
    key bs.ByteSlice
    val bs.ByteSlice
}

func new_varchar_kv(key, value bs.ByteSlice) *_varchar_kv {
    return &_varchar_kv {
        keysize: uint32(len(key)),
        valsize: uint32(len(value)),
        key: key,
        val: value,
    }
}

func (self *_varchar_kv) Bytes() []byte {
    bytes := make([]byte, 8 + self.keysize + self.valsize)
    copy(bytes[0:4], bs.ByteSlice32(self.keysize))
    copy(bytes[4:8], bs.ByteSlice32(self.valsize))
    copy(bytes[8:8+self.keysize], self.key)
    copy(bytes[8+self.keysize:8+self.keysize+self.valsize], self.val)
    return bytes
}

func load_varchar_kv(bytes bs.ByteSlice) *_varchar_kv {
    keysize := bytes[0:4].Int32()
    valsize := bytes[4:8].Int32()
    return &_varchar_kv{
        keysize: keysize,
        valsize: valsize,
        key: bytes[8:8+keysize],
        val: bytes[8+keysize:8+keysize+valsize],
    }
}


func NewVarcharStore(file file.BlockDevice) (*VarcharStore, error) {
    vc, err := varchar.NewVarchar(file)
    if err != nil {
        return nil, err
    }
    return &VarcharStore{vc}, nil
}

func (self *VarcharStore) Size() uint8 {
    return 8
}

func (self *VarcharStore) Get(bytes bs.ByteSlice) (key, value bs.ByteSlice, err error) {
    vc := self.varchar
    vkey := int64(bytes.Int64())
    bytes, err = vc.Read(vkey)
    if err != nil {
        return nil, nil, err
    }
    kv := load_varchar_kv(bytes)
    return kv.key, kv.val, nil
}

func (self *VarcharStore) Put(key, value bs.ByteSlice) (bytes bs.ByteSlice, err error) {
    vc := self.varchar
    kv := new_varchar_kv(key, value).Bytes()
    vkey, err := vc.Write(kv)
    if err != nil {
        return nil, err
    }
    return bs.ByteSlice64(uint64(vkey)), nil
}

func (self *VarcharStore) Update(bytes, key, value bs.ByteSlice) (rbytes bs.ByteSlice, err error) {
    vc := self.varchar
    vkey := int64(bytes.Int64())
    kv := new_varchar_kv(key, value).Bytes()
    oldkv, err := vc.Read(vkey)
    if err != nil {
        return nil, err
    }
    if len(oldkv) == len(kv) {
        err = vc.Update(vkey, kv)
        if err != nil {
            return nil, err
        }
        return bs.ByteSlice64(uint64(vkey)), nil
    } else {
        err := vc.Remove(vkey)
        if err != nil {
            return nil, err
        }
        return self.Put(key, value)
    }
}

func (self *VarcharStore) Remove(bytes bs.ByteSlice) (err error) {
    vc := self.varchar
    vkey := int64(bytes.Int64())
    return vc.Remove(vkey)
}

