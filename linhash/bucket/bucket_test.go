package bucket

import "testing"

import (
    "os"
    "math/rand"
    "fmt"
)

import (
    bs "file-structures/block/byteslice"
    buf "file-structures/block/buffers"
    file "file-structures/block/file2"
)

const PATH = "/tmp/__bucket_test"

func init() {
    if urandom, err := os.Open("/dev/urandom"); err != nil {
        return
    } else {
        seed := make([]byte, 8)
        if _, err := urandom.Read(seed); err == nil {
            rand.Seed(int64(bs.ByteSlice(seed).Int64()))
        }
        urandom.Close()
    }
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



func Test_records(t *testing.T) {
    f := testfile(t, PATH)
    defer f.Close()
    bt, err := NewBlockTable(f, 8, 8)
    if err != nil { t.Fatal(err) }
    if bt == nil { t.Fatal(fmt.Errorf("bt == nil")) }
    err = bt.add_block()
    if err != nil { t.Fatal(err) }
    err = bt.add_block()
    if err != nil { t.Fatal(err) }
    err = bt.save()
    if err != nil { t.Fatal(err) }


    records := bt.records
    expected :=  (len(bt.blocks[0].data)/16)*3
    if len(records) != expected {
        t.Fatalf("expected length of records == %d got %d", expected, len(records))
    }
}

func TestGetPutRemoveBlockTable(t *testing.T) {
    const RECORDS = 300
    f := testfile(t, PATH)
    defer f.Close()
    bt, err := NewBlockTable(f, 8, 255)
    if err != nil { t.Fatal(err) }

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
        err := bt.Put(record.key, record.value)
        if err != nil { t.Fatal(err) }
    }

    if bt.header.records != RECORDS {
        t.Fatalf("Expected record count == %d got %d", RECORDS,
          bt.header.records)
    }

    for _, record := range records {
        value, err := bt.Get(record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(record.value) {
            t.Fatal("Error getting record, value was not as expected")
        }
    }

    chosen := make(map[uint64]bool)
    test := func(bt *BlockTable) {
        for c := 0; c < len(records)/4; c++ {
            i := rand.Intn(len(records))
            record := records[i]
            for {
                record = records[i]
                key := record.key
                if _, has := chosen[key.Int64()]; !has {
                    chosen[key.Int64()] = true
                    break
                }
                i = rand.Intn(len(records))
            }
            if has := bt.Has(record.key); !has {
                t.Fatal("Should have had key")
            }
            random_key := randslice(8)
            _, real_key := keyset[random_key.Int64()]
            if has := bt.Has(random_key); has != real_key {
                t.Fatal("Has not working")
            }
            value, err := bt.Get(record.key)
            if err != nil { t.Fatal(err) }
            if !value.Eq(record.value) {
                t.Log(c, i)
                t.Log("key", record.key)
                t.Log("value", record.value)
                t.Log("value2", values2[i])
                t.Log("actual", value)
                t.Log()
                for _, record := range bt.records[:bt.header.records] {
                    t.Log(record.key, record.value)
                }
                t.Fatal("Error getting record, value was not as expected")
            }
            err = bt.Put(record.key, values2[i])
            if err != nil { t.Fatal(err) }
            if bt.header.records != RECORDS {
                t.Fatalf("Expected record count == %d got %d", RECORDS,
                  bt.header.records)
            }
            value2, err := bt.Get(record.key)
            if err != nil { t.Fatal(err, record.key) }
            if !value2.Eq(values2[i]) {
                t.Fatal("Error getting record, value was not as expected")
            }
        }
    }

    test(bt)
    bt2, err := ReadBlockTable(f, bt.Key())
    if err != nil { t.Fatal(err) }
    test(bt2)

    length := len(records)
    for _, record := range records[length/2:] {
        err := bt2.Remove(record.key)
        if err != nil {
            t.Log(record.key)
            t.Log()
            for _, record := range bt2.records[:bt2.header.records] {
                t.Log(record.key)
            }
            t.Fatal(err)
        }
    }

    for _, record := range records[length/2:] {
        if bt2.Has(record.key) {
            t.Fatal("Had key which had been removed")
        }
    }

    for i, record := range records[:length/2] {
        value, err := bt2.Get(record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(record.value) && !value.Eq(values2[i]) {
            t.Fatal("Error getting record, value was not as expected")
        }
    }

    for _, record := range records[:length/2] {
        err := bt2.Remove(record.key)
        if err != nil {
            t.Log(record.key)
            t.Log()
            for _, record := range bt2.records[:bt2.header.records] {
                t.Log(record.key)
            }
            t.Fatal(err)
        }
    }

    if bt2.header.records != 0 {
        t.Fatalf("Expected record count == 0 got %d", bt2.header.records)
    }

    if len(bt2.blocks) != 1 {
        t.Fatal("bt2.blocks != 1", len(bt2.blocks), bt2.header.blocks)
    }
}


func TestGetPutRemoveHashBucket(t *testing.T) {
    const RECORDS = 300
    f := testfile(t, PATH)
    defer f.Close()
    g := testfile(t, "/tmp/__varchar_store")
    defer g.Close()
    store, err := NewVarcharStore(g)
    if err != nil { panic(err) }
    hb, err := NewHashBucket(f, 8, store)
    if err != nil { t.Fatal(err) }

    type hash_record struct {
        hash, key, value bs.ByteSlice
    }

    hashset := make(map[uint64]bool)
    var records []*hash_record
    var values2 []bs.ByteSlice
    for i := 0; i < RECORDS; i++ {
        hash := randslice(8)
        for {
            if _, has := hashset[hash.Int64()]; !has {
                break
            }
            hash = randslice(8)
        }
        hashset[hash.Int64()] = true
        records = append(records,
          &hash_record{hash, randslice(8), randslice(128)})
        records = append(records,
          &hash_record{hash, randslice(8), randslice(128)})
        values2 = append(values2, randslice(128))
        values2 = append(values2, randslice(128))
    }

    for _, record := range records {
        _, err := hb.Put(record.hash, record.key, record.value)
        if err != nil { t.Fatal(err) }
    }

    if int(hb.bt.header.records) != len(records) {
        t.Fatalf("Expected record count == %d got %d", len(records),
          hb.bt.header.records)
    }

    for _, record := range records {
        value, err := hb.Get(record.hash, record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(record.value) {
            t.Fatal("Error getting record, value was not as expected")
        }
    }

    chosen := make(map[uint64]bool)
    test := func(hb *HashBucket) {
        for c := 0; c < len(records)/4; c++ {
            i := rand.Intn(len(records))
            record := records[i]
            for {
                record = records[i]
                key := record.key
                if _, has := chosen[key.Int64()]; !has {
                    chosen[key.Int64()] = true
                    break
                }
                i = rand.Intn(len(records))
            }
            if has := hb.Has(record.hash, record.key); !has {
                t.Fatal("Should have had key")
            }
            value, err := hb.Get(record.hash, record.key)
            if err != nil { t.Fatal(err) }
            if !value.Eq(record.value) {
                t.Fatal("Error getting record, value was not as expected")
            }
            _, err = hb.Put(record.hash, record.key, values2[i])
            if err != nil { t.Fatal(err) }
            if int(hb.bt.header.records) != len(records) {
                fmt.Println("x", record.hash, record.key, values2[i])
                t.Fatalf("Expected record count == %d got %d", len(records),
                  hb.bt.header.records)
            }
            value2, err := hb.Get(record.hash, record.key)
            if err != nil { t.Fatal(err, record.key) }
            if !value2.Eq(values2[i]) {
                t.Fatal("Error getting record, value was not as expected")
            }
        }
    }

    test(hb)
    hb2, err := ReadHashBucket(f, hb.Key(), store)
    if err != nil { t.Fatal(err) }
    test(hb2)

    length := len(records)
    for _, record := range records[length/2:] {
        err := hb2.Remove(record.hash, record.key)
        if err != nil {
            t.Fatal(err)
        }
    }

    for _, record := range records[length/2:] {
        if hb2.Has(record.hash, record.key) {
            t.Fatal("Had key which had been removed")
        }
    }

    for i, record := range records[:length/2] {
        value, err := hb2.Get(record.hash, record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(record.value) && !value.Eq(values2[i]) {
            t.Fatal("Error getting record, value was not as expected")
        }
    }

    for _, record := range records[:length/2] {
        err := hb2.Remove(record.hash, record.key)
        if err != nil {
            t.Fatal(err)
        }
    }

    if hb2.bt.header.records != 0 {
        t.Fatalf("Expected record count == 0 got %d", hb2.bt.header.records)
    }

    if len(hb2.bt.blocks) != 1 {
        t.Fatal("bt2.blocks != 1", len(hb2.bt.blocks), hb2.bt.header.blocks)
    }
}

func TestSplitHashBucket(t *testing.T) {
    const RECORDS = 300
    f := testfile(t, PATH)
    defer f.Close()
    store, err := NewBytesStore(8, 128)
    if err != nil { t.Fatal(err) }
    hb, err := NewHashBucket(f, 8, store)
    if err != nil { t.Fatal(err) }

    type hash_record struct {
        hash, key, value bs.ByteSlice
    }

    hashset := make(map[uint64]bool)
    var records []*hash_record
    var values2 []bs.ByteSlice
    for i := 0; i < RECORDS; i++ {
        hash := randslice(8)
        for {
            if _, has := hashset[hash.Int64()]; !has {
                break
            }
            hash = randslice(8)
        }
        hashset[hash.Int64()] = true
        records = append(records,
          &hash_record{hash, randslice(8), randslice(128)})
        records = append(records,
          &hash_record{hash, randslice(8), randslice(128)})
        values2 = append(values2, randslice(128))
        values2 = append(values2, randslice(128))
    }

    for _, record := range records {
        _, err := hb.Put(record.hash, record.key, record.value)
        if err != nil { t.Fatal(err) }
    }

    if int(hb.bt.header.records) != len(records) {
        t.Fatalf("Expected record count == %d got %d", len(records),
          hb.bt.header.records)
    }

    for _, record := range records {
        value, err := hb.Get(record.hash, record.key)
        if err != nil { t.Fatal(err) }
        if !value.Eq(record.value) {
            t.Fatal("Error getting record, value was not as expected")
        }
    }

    other, err := hb.Split(7)
    if err != nil {
        t.Fatal(err)
    }

    count := 0
    for _, record := range records {
        if !hb.Has(record.hash, record.key) && !other.Has(record.hash, record.key) {
            t.Error("Couldn't find record after split")
            count += 1
        }
    }

    mask := uint64(1) << 7
    for _, rec := range hb.bt.records[:hb.bt.header.records] {
        key := rec.key.Int64()
        if key & mask == mask {
            t.Errorf("Record in wrong block should be in other")
        }
    }
    for _, rec := range other.bt.records[:other.bt.header.records] {
        key := rec.key.Int64()
        if key & mask == 0 {
            t.Errorf("Record in wrong block should be in hb")
        }
    }

    if hb.bt.header.records == 0 {
        t.Errorf("hb shouldn't be empty")
    }
    if other.bt.header.records == 0 {
        t.Errorf("other shouldn't be empty")
    }
}

