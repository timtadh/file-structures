package bucket

import "testing"

import (
    "os"
    "math/rand"
    "fmt"
)

import (
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

func TestNewHeader(t *testing.T) {
    h1 := new_header(4, 8, false)
    h2 := new_header(4, 8, true)

    b1 := h1.Bytes()
    b2 := h2.Bytes()

    h1_2, _ := load_header(b1)
    h2_2, _ := load_header(b2)

    if !bs.ByteSlice(h1_2.Bytes()).Eq(b1) {
        t.Fatal("h1 != h1_2")
    }
    if !bs.ByteSlice(h2_2.Bytes()).Eq(b2) {
        t.Fatal("h2 != h2_2")
    }

    o1 := h1_2.flags()
    o2 := h2_2.flags()

    if o1 { t.Fatal("h1 flag not set properly") }
    if !o2 { t.Fatal("h2 flag not set properly") }
}

func TestBytesKVStore(t *testing.T) {


    test := func(store KVStore) {
        check := func(bytes, key, val bs.ByteSlice) (err error) {
            k2, v2, err := store.Get(bytes)
            if err != nil {
                return err
            }
            if !key.Eq(k2) {
                return fmt.Errorf("read key does equal put key")
            }
            if !val.Eq(v2) {
                return fmt.Errorf("read val does equal put val")
            }
            return nil
        }

        k1, v1, v1_2 := randslice(12), randslice(134), randslice(123)
        k2, v2, v2_2 := randslice(17), randslice(14), randslice(23)
        k3, v3, v3_2 := randslice(31), randslice(132), randslice(17)
        k4, v4, v4_2 := randslice(23), randslice(12), randslice(57)
        k5, v5, v5_2 := randslice(13), randslice(73), randslice(31)

        b1, err := store.Put(k1, v1)
        if err != nil { t.Fatal(err) }
        b2, err := store.Put(k2, v2)
        if err != nil { t.Fatal(err) }
        b3, err := store.Put(k3, v3)
        if err != nil { t.Fatal(err) }
        b4, err := store.Put(k4, v4)
        if err != nil { t.Fatal(err) }
        b5, err := store.Put(k5, v5)
        if err != nil { t.Fatal(err) }

        if err := check(b1, k1, v1); err != nil { t.Fatal(err) }
        if err := check(b2, k2, v2); err != nil { t.Fatal(err) }
        if err := check(b3, k3, v3); err != nil { t.Fatal(err) }
        if err := check(b4, k4, v4); err != nil { t.Fatal(err) }
        if err := check(b5, k5, v5); err != nil { t.Fatal(err) }

        b1_2, err := store.Update(b1, k1, v1_2)
        if err != nil { t.Fatal(err) }
        b2_2, err := store.Update(b2, k2, v2_2)
        if err != nil { t.Fatal(err) }
        b3_2, err := store.Update(b3, k3, v3_2)
        if err != nil { t.Fatal(err) }
        b4_2, err := store.Update(b4, k4, v4_2)
        if err != nil { t.Fatal(err) }
        b5_2, err := store.Update(b5, k5, v5_2)
        if err != nil { t.Fatal(err) }

        if err := check(b1_2, k1, v1_2); err != nil { t.Fatal(err) }
        if err := check(b2_2, k2, v2_2); err != nil { t.Fatal(err) }
        if err := check(b3_2, k3, v3_2); err != nil { t.Fatal(err) }
        if err := check(b4_2, k4, v4_2); err != nil { t.Fatal(err) }
        if err := check(b5_2, k5, v5_2); err != nil { t.Fatal(err) }
    }

    var err error
    bs, err := NewBytesStore(32, 200)
    if err != nil { panic(err) }
    test(bs)

    f := testfile(t, "/tmp/__varchar_store")
    defer f.Close()
    varchar, err := NewVarcharStore(f)
    if err != nil { panic(err) }
    test(varchar)



}

