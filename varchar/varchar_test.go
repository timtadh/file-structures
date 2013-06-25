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
    if v := NewVarchar(testfile(t)); v == nil {
        t.Fatal("Expected a initialized Varchar got nil")
    } else {
        v.Close()
    }
}

