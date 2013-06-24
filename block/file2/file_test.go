package file2

import "testing"

import (
    "os"
    "math/rand"
    "fmt"
)

import (
    buf "../buffers"
    bs "file-structures/block/byteslice"
)

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

const PATH = "/tmp/__x"
const CACHESIZE = 4096*16

func cleanup(path string) {
    os.Remove(path)
}

func TestOpen(t *testing.T) {
    f := NewBlockFile(PATH, &buf.NoBuffer{})
    defer cleanup(f.Path())
    if err := f.Open(); err != nil {
        t.Fatal(err)
    }
}

func TestAllocate(t *testing.T) {
    f := NewBlockFile(PATH, &buf.NoBuffer{})
    defer cleanup(f.Path())
    if err := f.Open(); err != nil {
        t.Fatal(err)
    }
    if p, err := f.Allocate(); err != nil {
        t.Fatal(err)
    } else if p != 4096 {
        t.Fatalf("Expected p == 4096 got %d", p)
    }
}

func TestSize(t *testing.T) {
    f := NewBlockFile(PATH, &buf.NoBuffer{})
    defer cleanup(f.Path())
    if err := f.Open(); err != nil {
        t.Fatal(err)
    }
    if p, err := f.Allocate(); err != nil {
        t.Fatal(err)
    } else if p != 4096 {
        t.Fatalf("Expected p == 4096 got %d", p)
    }
    if size, err := f.Size(); err != nil {
        t.Fatal(err)
    } else if size != 2*uint64(f.BlkSize()) {
        t.Fatalf("Expected size == %d got %d", 2*f.BlkSize(), size)
    }
}

func TestWriteRead(t *testing.T) {
    f := NewBlockFile(PATH, &buf.NoBuffer{})
    defer cleanup(f.Path())
    if err := f.Open(); err != nil {
        t.Fatal(err)
    }
    if p, err := f.Allocate(); err != nil {
        t.Fatal(err)
    } else if p != 4096 {
        t.Fatalf("Expected p == 4096 got %d", p)
    }
    if size, err := f.Size(); err != nil {
        t.Fatal(err)
    } else if size != 2*uint64(f.BlkSize()) {
        t.Fatalf("Expected size == %d got %d", 2*f.BlkSize(), size)
    }
    blk := make([]byte, f.BlkSize())
    for i := range blk {
        blk[i] = 0xf
    }
    if err := f.WriteBlock(4096, blk); err != nil {
        t.Fatal(err)
    }
    if err := f.Close(); err != nil {
        t.Fatal(err)
    }
    if err := f.Open(); err != nil {
        t.Fatal(err)
    }
    if rblk, err := f.ReadBlock(4096); err != nil {
        t.Fatal(err)
    } else if len(rblk) != int(f.BlkSize()) {
        t.Fatalf("Expected len(rblk) == %d got %d", f.BlkSize(), len(rblk))
    } else {
        for i, b := range rblk {
            if b != 0xf {
                t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
            }
        }
    }

    if p, err := f.Allocate(); err != nil {
        t.Fatal(err)
    } else if p != 8192 {
        t.Fatalf("Expected p == 8192 got %d", p)
    }

    if err := f.Free(4096); err != nil {
        t.Fatal(err)
    }
    if p, err := f.Allocate(); err != nil {
        t.Fatal(err)
    } else if p != 4096 {
        t.Fatalf("Expected p == 4096 got %d", p)
    }
    if size, err := f.Size(); err != nil {
        t.Fatal(err)
    } else if size != 3*uint64(f.BlkSize()) {
        t.Fatalf("Expected size == %d got %d", 3*f.BlkSize(), size)
    }
    if err := f.WriteBlock(4096, blk); err != nil {
        t.Fatal(err)
    }
    if err := f.Close(); err != nil {
        t.Fatal(err)
    }
    if err := f.Open(); err != nil {
        t.Fatal(err)
    }
    if rblk, err := f.ReadBlock(4096); err != nil {
        t.Fatal(err)
    } else if len(rblk) != int(f.BlkSize()) {
        t.Fatalf("Expected len(rblk) == %d got %d", f.BlkSize(), len(rblk))
    } else {
        for i, b := range rblk {
            if b != 0xf {
                t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
            }
        }
    }
}


func TestGenericWriteRead(t *testing.T) {
    tester := func(f BlockDevice) {
        var A, C int64
        var err error
        blk := make([]byte, f.BlkSize())
        for i := range blk {
            blk[i] = 0xf
        }

        if A, err = f.Allocate(); err != nil {
            t.Fatal(err)
        }
        if err := f.WriteBlock(A, blk); err != nil {
            t.Fatal(err)
        }
        if rblk, err := f.ReadBlock(A); err != nil {
            t.Fatal(err)
        } else if len(rblk) != int(f.BlkSize()) {
            t.Fatalf("Expected len(rblk) == %d got %d", f.BlkSize(), len(rblk))
        } else {
            for i, b := range rblk {
                if b != 0xf {
                    t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
                }
            }
        }

        if _, err = f.Allocate(); err != nil {
            t.Fatal(err)
        }

        if err = f.Free(A); err != nil {
            t.Fatal(err)
        }
        if C, err = f.Allocate(); err != nil {
            t.Fatal(err)
        } else if A != C {
            t.Fatalf("Expected A == C got %d != %d", A, C)
        }

        if err := f.WriteBlock(A, blk); err != nil {
            t.Fatal(err)
        }
        if rblk, err := f.ReadBlock(A); err != nil {
            t.Fatal(err)
        } else if len(rblk) != int(f.BlkSize()) {
            t.Fatalf("Expected len(rblk) == %d got %d", f.BlkSize(), len(rblk))
        } else {
            for i, b := range rblk {
                if b != 0xf {
                    t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
                }
            }
        }
    }


    bf := NewBlockFile(PATH, &buf.NoBuffer{})
    defer cleanup(bf.Path())
    if err := bf.Open(); err != nil {
        t.Fatal(err)
    }
    tester(bf)

    cf, err := NewCacheFile(PATH, CACHESIZE)
    if err != nil {
        t.Fatal(err)
    }
    defer cf.Close()
    tester(cf)
}

func TestPageOut(t *testing.T) {
    const ITEMS = 1000
    const CACHESIZE = 950
    f, err := NewCacheFile(PATH, 4096*CACHESIZE)
    if err != nil {
        t.Fatal(err)
    }
    defer f.Close()

    var keys []int64
    for i := 1; i <= ITEMS; i++ {
        var P int64
        if P, err = f.Allocate(); err != nil {
            t.Fatal(err)
        }
        keys = append(keys, P)
        blk := make([]byte, f.BlkSize())
        for i := range blk {
            blk[i] = byte(P)
        }

        if err := f.WriteBlock(P, blk); err != nil {
            t.Fatal(err)
        }


        R := keys[rand.Intn(len(keys)/2+1)]
        // t.Logf("key = %d", P)
        if rblk, err := f.ReadBlock(R); err != nil {
            t.Fatal(err)
        } else if len(rblk) != int(f.BlkSize()) {
            t.Fatalf("Expected len(rblk) == %d got %d", f.BlkSize(), len(rblk))
        } else {
            for i, b := range rblk {
                if b != byte(R) {
                    t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
                }
            }
        }

        if rblk, err := f.ReadBlock(P); err != nil {
            t.Fatal(err)
        } else if len(rblk) != int(f.BlkSize()) {
            t.Fatalf("Expected len(rblk) == %d got %d", f.BlkSize(), len(rblk))
        } else {
            for i, b := range rblk {
                if b != byte(P) {
                    t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
                }
            }
        }
    }

    for i := 1; i <= ITEMS*5; i++ {
        P := keys[rand.Intn(len(keys))]
        keys = append(keys, P)
        blk := make([]byte, f.BlkSize())
        for i := range blk {
            blk[i] = byte(P)
        }
        if err := f.WriteBlock(P, blk); err != nil {
            t.Fatal(err)
        }
    }

    for i := 1; i <= ITEMS*5; i++ {
        P := keys[rand.Intn(len(keys))]
        if rblk, err := f.ReadBlock(P); err != nil {
            t.Fatal(err)
        } else if len(rblk) != int(f.BlkSize()) {
            t.Fatalf("Expected len(rblk) == %d got %d", f.BlkSize(), len(rblk))
        } else {
            for i, b := range rblk {
                if b != byte(P) {
                    t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
                }
            }
        }
    }

    fmt.Println("Cache Keys")
    for _, item := range f.cache_keys.slice {
        fmt.Println(item.p, item.count)
    }
    fmt.Println()
    fmt.Println("Disk Keys")
    for _, item := range f.disk_keys.slice {
        fmt.Println(item.p, item.count)
    }
}

