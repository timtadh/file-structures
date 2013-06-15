package file2

import "testing"

import (
  "os"
)

import (
    buf "../buffers"
)

const PATH = "/tmp/__x"
const BLKSIZE = 4096

func cleanup(path string) {
    os.Remove(path)
}

func TestOen(t *testing.T) {
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
    if p, err := f.Allocate(BLKSIZE); err != nil {
        t.Fatal(err)
    } else if p != 0 {
        t.Fatalf("Expected p == 0 got %d", p)
    }
}

func TestSize(t *testing.T) {
    f := NewBlockFile(PATH, &buf.NoBuffer{})
    defer cleanup(f.Path())
    if err := f.Open(); err != nil {
        t.Fatal(err)
    }
    if p, err := f.Allocate(BLKSIZE); err != nil {
        t.Fatal(err)
    } else if p != 0 {
        t.Fatalf("Expected p == 0 got %d", p)
    }
    if size, err := f.Size(); err != nil {
        t.Fatal(err)
    } else if size != BLKSIZE {
        t.Fatalf("Expected size == %d got %d", BLKSIZE, size)
    }
}

func TestWriteRead(t *testing.T) {
    f := NewBlockFile(PATH, &buf.NoBuffer{})
    defer cleanup(f.Path())
    if err := f.Open(); err != nil {
        t.Fatal(err)
    }
    if p, err := f.Allocate(BLKSIZE); err != nil {
        t.Fatal(err)
    } else if p != 0 {
        t.Fatalf("Expected p == 0 got %d", p)
    }
    if size, err := f.Size(); err != nil {
        t.Fatal(err)
    } else if size != BLKSIZE {
        t.Fatalf("Expected size == %d got %d", BLKSIZE, size)
    }
    blk := make([]byte, BLKSIZE)
    for i := range blk {
        blk[i] = 0xf
    }
    if err := f.WriteBlock(0, blk); err != nil {
        t.Fatal(err)
    }
    if rblk, err := f.ReadBlock(0, BLKSIZE); err != nil {
        t.Fatal(err)
    } else if len(rblk) != BLKSIZE {
        t.Fatalf("Expected len(rblk) == %d got %d", BLKSIZE, len(rblk))
    } else {
        for i, b := range rblk {
            if b != 0xf {
                t.Fatalf("Expected rblk[%d] == 0xf got %d", i, b)
            }
        }
    }
}

