package varchar

import "testing"

import (
    "os"
    "math/rand"
    "fmt"
)

import (
    buf "file-structures/block/buffers"
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
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

    check := func(key int64, blocks []*block, err error, num_blocks int, length uint64) error {
        if err != nil {
            return err
        } else if len(blocks) != num_blocks {
            return fmt.Errorf("Expected len(blocks) == %d got %d; length=%d",
                num_blocks, len(blocks), varchar.length(key, blocks[0]))
        } else {
            l := varchar.length(key, blocks[0])
            if l != length {
                return fmt.Errorf("Expected length == %d got %d", length, l)
            }
        }
        return nil
    }

    var blocks []*block
    var k1, k2, k3, k4, k5, k5_2, k6, k7 int64
    var err error
    if k1, blocks, err = varchar.alloc(1234); err != nil {
        t.Fatal(err)
    } else if err := check(k1, blocks, err, 1, 1234); err != nil {
        t.Fatal(err)
    }
    if k2, blocks, err = varchar.alloc(231); err != nil {
        t.Fatal(err)
    } else if err := check(k2, blocks, err, 1, 231); err != nil {
        t.Fatal(err)
    }
    if k3, blocks, err = varchar.alloc(30131); err != nil {
        t.Fatal(err)
    } else if err := check(k3, blocks, err, 8, 30131); err != nil {
        t.Fatal(err)
    }
    if k4, blocks, err = varchar.alloc(42); err != nil {
        t.Fatal(err)
    } else if err := check(k4, blocks, err, 1, 42); err != nil {
        t.Fatal(err)
    }
    if k5, blocks, err = varchar.alloc(9232); err != nil {
        t.Fatal(err)
    } else if err := check(k5, blocks, err, 4, 9232); err != nil {
        t.Fatal(err)
    }
    if k6, blocks, err = varchar.alloc(7500); err != nil {
        t.Fatal(err)
    } else if err := check(k6, blocks, err, 2, 7500); err != nil {
        t.Fatal(err)
    }
    if k7, blocks, err = varchar.alloc(324); err != nil {
        t.Fatal(err)
    } else if err := check(k7, blocks, err, 1, 324); err != nil {
        t.Fatal(err)
    }

    if blocks, err := varchar.blocks(k1); err != nil {
        t.Fatal(err)
    } else if err := check(k1, blocks, err, 1, 1234); err != nil {
        t.Fatal(err)
    }

    if blocks, err := varchar.blocks(k2); err != nil {
        t.Fatal(err)
    } else if err := check(k2, blocks, err, 1, 231); err != nil {
        t.Fatal(err)
    }

    if blocks, err := varchar.blocks(k3); err != nil {
        t.Fatal(err)
    } else if err := check(k3, blocks, err, 8, 30131); err != nil {
        t.Fatal(err)
    }

    if blocks, err := varchar.blocks(k4); err != nil {
        t.Fatal(err)
    } else if err := check(k4, blocks, err, 1, 42); err != nil {
        t.Fatal(err)
    }

    if blocks, err := varchar.blocks(k5); err != nil {
        t.Fatal(err)
    } else if err := check(k5, blocks, err, 4, 9232); err != nil {
        t.Fatal(err)
    }

    if blocks, err := varchar.blocks(k6); err != nil {
        t.Fatal(err)
    } else if err := check(k6, blocks, err, 2, 7500); err != nil {
        t.Fatal(err)
    }

    if blocks, err := varchar.blocks(k7); err != nil {
        t.Fatal(err)
    } else if err := check(k7, blocks, err, 1, 324); err != nil {
        t.Fatal(err)
    }



    // fmt.Println("\nfree k2", k2)
    if err = varchar.free(k2); err != nil {
        t.Fatal(err)
    }

    // fmt.Println("\nfree k4", k4)
    if err = varchar.free(k4); err != nil {
        t.Fatal(err)
    }

    // fmt.Println("\nfree k5", k5)
    if err = varchar.free(k5); err != nil {
        t.Fatal(err)
    }

    // fmt.Println("\nalloc k5_2")
    if k5_2, blocks, err = varchar.alloc(9000); err != nil {
        t.Fatal(err)
    // } else if k5 - 42 - 8!= k5_2 {
    //     t.Fatalf("Expected key == key2 got %d != %d", k5 - 42 - 8, k5_2)
    } else if err := check(k5_2, blocks, err, 3, 9000); err != nil {
        t.Fatal(err)
    }

    // fmt.Println("\nread k5_2", k5_2)
    if blocks, err := varchar.blocks(k5_2); err != nil {
        t.Fatal(err)
    } else if err := check(k5_2, blocks, err, 3, 9000); err != nil {
        t.Fatal(err)
    }


    // fmt.Println("\nfree k6", k6)
    if err = varchar.free(k6); err != nil {
        t.Fatal(err)
    }
    // fmt.Println("\nfree k1", k1)
    if err = varchar.free(k1); err != nil {
        t.Fatal(err)
    }
    // fmt.Println("\nfree k7", k7)
    if err = varchar.free(k7); err != nil {
        t.Fatal(err)
    }
    // fmt.Println("\nfree k3", k3)
    if err = varchar.free(k3); err != nil {
        t.Fatal(err)
    }
    // fmt.Println("\nfree k5", k5_2)
    if err = varchar.free(k5_2); err != nil {
        t.Fatal(err)
    }

    if varchar.ctrl.free_len != 1 {
        t.Fatalf("Expected free_len == 1 got %d", varchar.ctrl.free_len)
    }
}

func TestReadWriteUpdateRemove(t *testing.T) {
    varchar, _ := NewVarchar(testfile(t))
    defer varchar.Close()

    randslice := func(length int) bs.ByteSlice {
        if urandom, err := os.Open("/dev/urandom"); err != nil {
            panic(err)
        } else {
            slice := make([]byte, length)
            if _, err := urandom.Read(slice); err != nil {
                panic(err)
            }
            return slice
        }
        panic("unreachable")
    }

    var err error
    var k1, k2, k3, k4, k5, k6, k7 int64
    const l1 = 3412
    const l2 = 79841
    const l3 = 98413
    const l4 = 8413
    const l5 = 7500
    const l6 = 123
    const l7 = 1234

    v1 := randslice(l1)
    v2 := randslice(l2)
    v2_2 := randslice(l2)
    v3 := randslice(l3)
    v4 := randslice(l5)
    v4_2 := randslice(l5)
    v5 := randslice(l5)
    v6 := randslice(l6)
    v7 := randslice(l7)

    if k1, err = varchar.Write(v1); err != nil { t.Fatal(err) }
    if k2, err = varchar.Write(v2); err != nil { t.Fatal(err) }
    if k3, err = varchar.Write(v3); err != nil { t.Fatal(err) }
    if k4, err = varchar.Write(v4); err != nil { t.Fatal(err) }
    if k5, err = varchar.Write(v5); err != nil { t.Fatal(err) }
    if k6, err = varchar.Write(v6); err != nil { t.Fatal(err) }
    if k7, err = varchar.Write(v7); err != nil { t.Fatal(err) }

    check_values := func(v2, v4 bs.ByteSlice) {
        if rv1, err := varchar.Read(k1); err != nil {
            t.Fatal(err)
        } else if !rv1.Eq(v1) {
            t.Fatal("slices not equal")
        }

        if rv2, err := varchar.Read(k2); err != nil {
            t.Fatal(err)
        } else if !rv2.Eq(v2) {
            t.Fatal("slices not equal")
        }

        if rv3, err := varchar.Read(k3); err != nil {
            t.Fatal(err)
        } else if !rv3.Eq(v3) {
            t.Fatal("slices not equal")
        }

        if rv3, err := varchar.Read(k3); err != nil {
            t.Fatal(err)
        } else if !rv3.Eq(v3) {
            t.Fatal("slices not equal")
        }

        if rv4, err := varchar.Read(k4); err != nil {
            t.Fatal(err)
        } else if !rv4.Eq(v4) {
            t.Fatal("slices not equal")
        }

        if rv5, err := varchar.Read(k5); err != nil {
            t.Fatal(err)
        } else if !rv5.Eq(v5) {
            t.Fatal("slices not equal")
        }

        if rv6, err := varchar.Read(k6); err != nil {
            t.Fatal(err)
        } else if !rv6.Eq(v6) {
            t.Fatal("slices not equal")
        }

        if rv7, err := varchar.Read(k7); err != nil {
            t.Fatal(err)
        } else if !rv7.Eq(v7) {
            t.Fatal("slices not equal")
        }
    }
    check_values(v2, v4)

    if err = varchar.Remove(k3); err != nil { t.Fatal(err) }
    if err = varchar.Remove(k6); err != nil { t.Fatal(err) }
    if k3, err = varchar.Write(v3); err != nil { t.Fatal(err) }
    if k6, err = varchar.Write(v6); err != nil { t.Fatal(err) }

    check_values(v2, v4)

    if err = varchar.Update(k2, v2_2); err != nil { t.Fatal(err) }
    if err = varchar.Update(k4, v4_2); err != nil { t.Fatal(err) }

    check_values(v2_2, v4_2)

    if err = varchar.Remove(k1); err != nil { t.Fatal(err) }
    if err = varchar.Remove(k2); err != nil { t.Fatal(err) }
    if err = varchar.Remove(k3); err != nil { t.Fatal(err) }
    if err = varchar.Remove(k4); err != nil { t.Fatal(err) }
    if err = varchar.Remove(k5); err != nil { t.Fatal(err) }
    if err = varchar.Remove(k6); err != nil { t.Fatal(err) }
    if err = varchar.Remove(k7); err != nil { t.Fatal(err) }

    if varchar.ctrl.free_len != 1 {
        t.Fatalf("Expected free_len == 1 got %d", varchar.ctrl.free_len)
    }
}

