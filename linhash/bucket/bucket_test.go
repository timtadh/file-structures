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
    }
}

func testfile(t *testing.T) file.BlockDevice {
    const CACHESIZE = 1000
    ibf := file.NewBlockFile(PATH, &buf.NoBuffer{})
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
    f := testfile(t)
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
    f := testfile(t)
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
    for c := 0; c < len(records); c++ {
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

    for _, record := range records {
        err := bt.Remove(record.key)
        if err != nil {
            t.Log(record.key)
            t.Log()
            for _, record := range bt.records[:bt.header.records] {
                t.Log(record.key)
            }
            t.Fatal(err)
        }
    }

    if bt.header.records != 0 {
        t.Fatalf("Expected record count == 0 got %d", bt.header.records)
    }
}

