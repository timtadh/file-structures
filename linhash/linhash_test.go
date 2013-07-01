package linhash

import "testing"

import (
    "os"
    "math/rand"
)

import (
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
    buf "file-structures/block/buffers"
    bucket "file-structures/linhash/bucket"
)

const PATH = "/tmp/__lin_hash"
const VPATH = "/tmp/__varchar_store_lin_hash"

func init() {
    if urandom, err := os.Open("/dev/urandom"); err != nil {
        return
    } else {
        seed := make([]byte, 8)
        if _, err := urandom.Read(seed); err == nil {
            rand.Seed(int64(bs.ByteSlice(seed).Int64()))
        }
    }
}

func randslice(length int) bs.ByteSlice {
    if urandom, err := os.Open("/dev/urandom"); err != nil {
        panic(err)
    } else {
        slice := make([]byte, length)
        if _, err := urandom.Read(slice); err != nil {
            panic(err)
        }
        urandom.Close()
        return slice
    }
    panic("unreachable")
}

func testfile(t *testing.T, path string) file.BlockDevice {
    const CACHESIZE = 1000
    ibf := file.NewBlockFile(path, &buf.NoBuffer{})
    if err := ibf.Open(); err != nil {
        t.Fatal(err)
    }
    f, err := file.NewCacheFile(ibf, 4096*CACHESIZE)
    if err != nil {
        t.Fatal(err)
    }
    return f
}

func TestNewLinearHash(t *testing.T) {
    g := testfile(t, VPATH)
    defer g.Close()
    store, err := bucket.NewVarcharStore(g)
    if err != nil { panic(err) }
    hash, err := NewLinearHash(testfile(t, PATH), store)
    defer hash.Close()
    if err != nil {
        t.Fatal(err)
    }
}

func TestPutHasGetRemoveLinearHash(t *testing.T) {
    const RECORDS = 300
    g := testfile(t, VPATH)
    defer g.Close()
    store, err := bucket.NewVarcharStore(g)
    if err != nil { panic(err) }
    hash, err := NewLinearHash(testfile(t, PATH), store)
    defer hash.Close()
    if err != nil {
        t.Fatal(err)
    }

    type record struct {
        key bs.ByteSlice
        value bs.ByteSlice
    }

    keyset := make(map[uint64]bool)
    var records []*record
    var values2 []bs.ByteSlice
    for i := 0; i < RECORDS; i++ {
        key := randslice(8)
        for {
            if _, has := keyset[key.Int64()]; !has {
                break
            }
            key = randslice(8)
        }
        keyset[key.Int64()] = true
        records = append(records, &record{key, randslice(255)})
        values2 = append(values2, randslice(255))
    }

    for _, record := range records {
        err := hash.Put(record.key, record.value)
        if err != nil { t.Fatal(err) }
    }

    if hash.ctrl.records != RECORDS {
        t.Fatalf("Expected record count == %d got %d", RECORDS,
          hash.ctrl.records)
    }

    for _, record := range records {
        has, err := hash.Has(record.key)
        if err != nil { t.Fatal(err) }
        if !has { t.Fatal("Expected key") }
        value, err := hash.Get(record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(record.value) {
            t.Fatal("Error getting record, value was not as expected")
        }
    }
}

