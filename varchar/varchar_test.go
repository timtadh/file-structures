package varchar

import "testing"

import (
    "os"
    "math/rand"
)

import (
    buf "../block/buffers"
    bs "../block/byteslice"
    file "../block/file2"
)

const PATH = "/tmp/__y"

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

func TestNewVarchar(t *testing.T) {
    if v, err := NewVarchar(testfile(t)); err != nil {
        t.Fatal(err)
    } else if v == nil {
        t.Fatal("Expected a initialized Varchar got nil")
    } else {
        v.Close()
    }
}

func TestAllocateLengthBlocksFree(t *testing.T) {
    varchar, _ := NewVarchar(testfile(t))
    defer varchar.Close()

    var key int64
    var err error
    if _, _, err = varchar.alloc(1234); err != nil { t.Fatal(err) }
    if _, _, err = varchar.alloc(231); err != nil { t.Fatal(err) }
    if _, _, err = varchar.alloc(30131); err != nil { t.Fatal(err) }
    if _, _, err = varchar.alloc(42); err != nil { t.Fatal(err) }
    if key, _, err = varchar.alloc(9232); err != nil { t.Fatal(err) }
    if _, _, err = varchar.alloc(612); err != nil { t.Fatal(err) }
    if _, _, err = varchar.alloc(612); err != nil { t.Fatal(err) }

    t.Log("Key", key)
    if blocks, err := varchar.blocks(key); err != nil {
        t.Fatal(err)
    } else if len(blocks) != 3 {
        t.Fatalf("Expected len(blocks) == 3 got %d", len(blocks))
    } else {
        length := varchar.length(key, blocks[0])
        if length != 9232 {
            t.Fatalf("Expected length == 9232 got %d", length)
        }
    }

    if err = varchar.free(key); err != nil {
        t.Fatal(err)
    }

    if key2, _, err := varchar.alloc(9000); err != nil {
        t.Fatal(err)
    } else if key != key2 {
        t.Fatalf("Expected key == key2 got %d != %d", key, key2)
    }
}

