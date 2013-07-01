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
    const CACHESIZE = 10000
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

    keyset := make(map[string]bool)
    var records []*record
    var values2 []bs.ByteSlice
    for i := 0; i < RECORDS; i++ {
        key := randslice(rand.Intn(25)+1)
        for {
            if _, has := keyset[string(key)]; !has {
                break
            }
            key = randslice(rand.Intn(25)+1)
        }
        keyset[string(key)] = true
        records = append(records, &record{key, randslice(1232)})
        values2 = append(values2, randslice(2327))
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
        ran := randslice(rand.Intn(25)+1)
        if _, has := keyset[string(ran)]; !has {
            value, err := hash.DefaultGet(ran, bs.ByteSlice64(0))
            if err != nil { t.Fatal(err) }
            if !value.Eq(bs.ByteSlice64(0)) {
                t.Fatal("Error getting default")
            }
        } else {
            _, err := hash.DefaultGet(ran, bs.ByteSlice64(0))
            if err != nil { t.Fatal(err) }
        }
    }

    for i, record := range records {
        has, err := hash.Has(record.key)
        if err != nil { t.Fatal(err) }
        if !has { t.Fatal("Expected key") }
        value, err := hash.Get(record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(record.value) {
            t.Fatal("Error getting record, value was not as expected")
        }
        err = hash.Put(record.key, values2[i])
        if err != nil {
            t.Fatal(err)
        }
        value, err = hash.Get(record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(values2[i]) {
            t.Fatal("Error getting record, value was not as expected")
        }
        if hash.Length() != RECORDS {
            t.Fatalf("Expected record count == %d got %d", RECORDS,
              hash.ctrl.records)
        }
    }

    length := hash.Length()
    for _, record := range records[length/2:] {
        err := hash.Remove(record.key)
        if err != nil { t.Fatal(err) }
    }

    for _, record := range records[length/2:] {
        has, err := hash.Has(record.key)
        if err != nil { t.Fatal(err) }
        if has {
            t.Fatal("expected key to be gone")
        }
    }

    for i, record := range records[:length/2] {
        has, err := hash.Has(record.key)
        if err != nil { t.Fatal(err) }
        if !has { t.Fatal("Expected key") }
        value, err := hash.Get(record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(values2[i]) {
            t.Fatal("Error getting record, value was not as expected")
        }
    }

    for _, record := range records[:length/2] {
        err := hash.Remove(record.key)
        if err != nil { t.Fatal(err) }
    }

    for _, record := range records {
        has, err := hash.Has(record.key)
        if err != nil { t.Fatal(err) }
        if has {
            t.Fatal("expected key to be gone")
        }
    }

    if hash.Length() != 0 {
        t.Fatalf("Expected record count == %d got %d", 0,
          hash.ctrl.records)
    }
}

