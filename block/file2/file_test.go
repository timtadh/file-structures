package file2

import "testing"

import (
    "math/rand"
    "os"
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
const CACHESIZE = 4096 * 16

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
    } else if size != 2*uint64(f.BlockSize()) {
        t.Fatalf("Expected size == %d got %d", 2*f.BlockSize(), size)
    }
}

func TestWriteRead(t *testing.T) {
    f := NewBlockFile(PATH, &buf.NoBuffer{})
    defer cleanup(f.Path())
    if err := f.Open(); err != nil {
        t.Fatal(err)
    }

    control_data := []byte("Hi there!")
    if err := f.SetControlData(control_data); err != nil {
        t.Fatal(err)
    }

    if p, err := f.Allocate(); err != nil {
        t.Fatal(err)
    } else if p != 4096 {
        t.Fatalf("Expected p == 4096 got %d", p)
    }
    if size, err := f.Size(); err != nil {
        t.Fatal(err)
    } else if size != 2*uint64(f.BlockSize()) {
        t.Fatalf("Expected size == %d got %d", 2*f.BlockSize(), size)
    }
    blk := make([]byte, f.BlockSize())
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
    } else if len(rblk) != int(f.BlockSize()) {
        t.Fatalf("Expected len(rblk) == %d got %d", f.BlockSize(), len(rblk))
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
    } else if size != 3*uint64(f.BlockSize()) {
        t.Fatalf("Expected size == %d got %d", 3*f.BlockSize(), size)
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
    } else if len(rblk) != int(f.BlockSize()) {
        t.Fatalf("Expected len(rblk) == %d got %d", f.BlockSize(), len(rblk))
    } else {
        for i, b := range rblk {
            if b != 0xf {
                t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
            }
        }
    }

    if read_data, err := f.ControlData(); err != nil {
        t.Fatal(err)
    } else {
        for i, b := range control_data {
            if b != read_data[i] {
                t.Fatalf("Expected read_data[%d] == %d got %d", i,
                    b, read_data[i])
            }
        }
    }
}

func TestGenericWriteRead(t *testing.T) {
    tester := func(f BlockDevice) {
        var A, C int64
        var err error
        blk := make([]byte, f.BlockSize())
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
        } else if len(rblk) != int(f.BlockSize()) {
            t.Fatalf("Expected len(rblk) == %d got %d", f.BlockSize(), len(rblk))
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
        } else if len(rblk) != int(f.BlockSize()) {
            t.Fatalf("Expected len(rblk) == %d got %d", f.BlockSize(), len(rblk))
        } else {
            for i, b := range rblk {
                if b != 0xf {
                    t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
                }
            }
        }
    }

    bf := NewBlockFile(PATH, &buf.NoBuffer{})
    if err := bf.Open(); err != nil {
        t.Fatal(err)
    }
    tester(bf)
    cleanup(bf.Path())

    ibf := NewBlockFile(PATH, &buf.NoBuffer{})
    if err := ibf.Open(); err != nil {
        t.Fatal(err)
    }
    cf, err := NewCacheFile(ibf, CACHESIZE)
    if err != nil {
        t.Fatal(err)
    }
    defer cf.Close()
    tester(cf)
    tester(cf)
    tester(cf)
    tester(cf)
    tester(cf)
    tester(cf)
}

func TestPageOut(t *testing.T) {
    const ITEMS = 1000
    const CACHESIZE = 950
    ibf := NewBlockFile(PATH, &buf.NoBuffer{})
    if err := ibf.Open(); err != nil {
        t.Fatal(err)
    }
    f, err := NewCacheFile(ibf, 4096*CACHESIZE)
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
        blk := make([]byte, f.BlockSize())
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
        } else if len(rblk) != int(f.BlockSize()) {
            t.Fatalf("Expected len(rblk) == %d got %d", f.BlockSize(), len(rblk))
        } else {
            for i, b := range rblk {
                if b != byte(R) {
                    t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
                }
            }
        }

        if rblk, err := f.ReadBlock(P); err != nil {
            t.Fatal(err)
        } else if len(rblk) != int(f.BlockSize()) {
            t.Fatalf("Expected len(rblk) == %d got %d", f.BlockSize(), len(rblk))
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
        blk := make([]byte, f.BlockSize())
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
        } else if len(rblk) != int(f.BlockSize()) {
            t.Fatalf("Expected len(rblk) == %d got %d", f.BlockSize(), len(rblk))
        } else {
            for i, b := range rblk {
                if b != byte(P) {
                    t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
                }
            }
        }
    }
}
