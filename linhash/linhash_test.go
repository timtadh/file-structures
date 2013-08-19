package linhash

import "testing"

import (
    "fmt"
    "os"
    "math/rand"
)

import (
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
    buf "file-structures/block/buffers"
    bucket "file-structures/linhash/bucket"
)

const PATH = "/tmp/__lin_linhash"
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

func testfile(t *testing.T, path string) file.RemovableBlockDevice {
    const CACHESIZE = 10000
    ibf := file.NewBlockFile(path, &buf.NoBuffer{})
    if err := ibf.Open(); err != nil {
        t.Fatal(err)
    }
    f, err := file.NewLRUCacheFile(ibf, 4096*CACHESIZE)
    if err != nil {
        t.Fatal(err)
    }
    return f
}

func TestNewLinearHash(t *testing.T) {
    g := testfile(t, VPATH)
    defer func() {
        if e := g.Close(); e != nil { panic(e) }
        if e := g.Remove(); e != nil { panic(e) }
    }()
    store, err := bucket.NewVarcharStore(g)
    if err != nil { panic(err) }
    f := testfile(t, PATH)
    defer func() {
        if e := f.Close(); e != nil { panic(e) }
        if e := f.Remove(); e != nil { panic(e) }
    }()
    _, err = NewLinearHash(f, store)
    if err != nil {
        t.Fatal(err)
    }
}

func TestPutHasGetRemoveLinearHash(t *testing.T) {
    fmt.Println("start test")
    const RECORDS = 300
    g := testfile(t, VPATH)
    defer func() {
        if e := g.Close(); e != nil { panic(e) }
        if e := g.Remove(); e != nil { panic(e) }
    }()
    store, err := bucket.NewVarcharStore(g)
    if err != nil { panic(err) }
    f := testfile(t, PATH)
    defer func() {
        if e := f.Close(); e != nil { panic(e) }
        if e := f.Remove(); e != nil { panic(e) }
    }()
    linhash, err := NewLinearHash(f, store)
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
        key := randslice(rand.Intn(16)+1)
        for {
            if _, has := keyset[string(key)]; !has {
                break
            }
            key = randslice(rand.Intn(16)+1)
        }
        keyset[string(key)] = true
        records = append(records, &record{key, randslice(rand.Intn(150)+25)})
        values2 = append(values2, randslice(rand.Intn(150)+25))
    }
    fmt.Println("real start test")

    for i, record := range records {
        err := linhash.Put(record.key, record.value)
        if err != nil { t.Fatal(err) }
        has, err := linhash.Has(record.key)
        if err != nil { t.Fatal(err) }
        if !has {
            hash := hash(record.key)
            bkt_idx := linhash.bucket(hash)
            bkt, _ := linhash.get_bucket(bkt_idx)
            bkt.PrintBucket()
            bkt_idx2 := bkt_idx - (1<<(linhash.ctrl.i-1))
            if bkt_idx2 < linhash.ctrl.buckets {
                bkt2, _ := linhash.get_bucket(bkt_idx2)
                bkt2.PrintBucket()
            }
            fmt.Println(i, bs.ByteSlice64(hash), record.key, bkt_idx, bkt_idx2, linhash.ctrl.buckets, linhash.ctrl.i)
            t.Fatal("Expected key")
        }
        value, err := linhash.Get(record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(record.value) {
            t.Fatal("Error getting record, value was not as expected")
        }
    }

    if linhash.ctrl.records != RECORDS {
        t.Fatalf("Expected record count == %d got %d", RECORDS,
          linhash.ctrl.records)
    }

    rkeys, err := linhash.Keys()
    if err != nil { t.Fatal(err) }
    rkeyset := make(map[string]bool)
    for _, bkey := range rkeys {
        key := string(bkey)
        if _, has := keyset[key]; !has {
            t.Fatal("got non-existent key", bs.ByteSlice(key))
        }
        rkeyset[key] = true
    }

    for key, _ := range keyset {
        if _, has := rkeyset[key]; !has {
            t.Fatal("missed key", bs.ByteSlice(key))
        }
    }

    for i, record := range records {
        has, err := linhash.Has(record.key)
        if err != nil { t.Fatal(err) }
        if !has {
            hash := hash(record.key)
            bkt_idx := linhash.bucket(hash)
            bkt, _ := linhash.get_bucket(bkt_idx)
            bkt.PrintBucket()
            bkt_idx2 := bkt_idx - (1<<(linhash.ctrl.i-1))
            if bkt_idx2 < linhash.ctrl.buckets {
                bkt2, _ := linhash.get_bucket(bkt_idx2)
                bkt2.PrintBucket()
            }
            fmt.Println(i, bs.ByteSlice64(hash), record.key, bkt_idx, bkt_idx2, linhash.ctrl.buckets, linhash.ctrl.i)
            t.Fatal("Expected key")
        }
        value, err := linhash.Get(record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(record.value) {
            t.Fatal("Error getting record, value was not as expected")
        }
        ran := randslice(rand.Intn(25)+1)
        if _, has := keyset[string(ran)]; !has {
            value, err := linhash.DefaultGet(ran, bs.ByteSlice64(0))
            if err != nil { t.Fatal(err) }
            if !value.Eq(bs.ByteSlice64(0)) {
                t.Fatal("Error getting default")
            }
        } else {
            _, err := linhash.DefaultGet(ran, bs.ByteSlice64(0))
            if err != nil { t.Fatal(err) }
        }
    }

    for i, record := range records {
        has, err := linhash.Has(record.key)
        if err != nil { t.Fatal(err) }
        if !has { t.Fatal("Expected key") }
        value, err := linhash.Get(record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(record.value) {
            t.Fatal("Error getting record, value was not as expected")
        }
        err = linhash.Put(record.key, values2[i])
        if err != nil {
            t.Fatal(err)
        }
        value, err = linhash.Get(record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(values2[i]) {
            t.Fatal("Error getting record, value was not as expected")
        }
        if linhash.Length() != RECORDS {
            t.Fatalf("Expected record count == %d got %d", RECORDS,
              linhash.ctrl.records)
        }
    }

    length := linhash.Length()
    for _, record := range records[length/2:] {
        err := linhash.Remove(record.key)
        if err != nil { t.Fatal(err) }
    }

    for _, record := range records[length/2:] {
        has, err := linhash.Has(record.key)
        if err != nil { t.Fatal(err) }
        if has {
            t.Fatal("expected key to be gone")
        }
    }

    for i, record := range records[:length/2] {
        has, err := linhash.Has(record.key)
        if err != nil { t.Fatal(err) }
        if !has { t.Fatal("Expected key") }
        value, err := linhash.Get(record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(values2[i]) {
            t.Fatal("Error getting record, value was not as expected")
        }
    }

    for _, record := range records[:length/2] {
        err := linhash.Remove(record.key)
        if err != nil { t.Fatal(err) }
    }

    for _, record := range records {
        has, err := linhash.Has(record.key)
        if err != nil { t.Fatal(err) }
        if has {
            t.Fatal("expected key to be gone")
        }
    }

    if linhash.Length() != 0 {
        t.Fatalf("Expected record count == %d got %d", 0,
          linhash.ctrl.records)
    }
}

