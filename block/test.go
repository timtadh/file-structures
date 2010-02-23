package main

import "fmt"
import . "file"
import . "keyblock"
import . "buffers"
import . "byteslice"

func main() {
    fmt.Println("hi")
    positions := make([][]byte, 4)
    dim, _ := NewBlockDimensions(RECORDS|POINTERS, 4096, 8, 8, &([3]uint32{1, 1, 2}))
    t, _ := NewBlockFile("hello.btree", NewLFU(3))
    fmt.Println(t)
    fmt.Println(t.Open())

    if b, ok := NewKeyBlock(t, dim); ok {
        positions[0] = b.Position()

        r := b.NewRecord(ByteSlice64(2))
        r.Set(2, []byte{1, 2})
        if i, ok := b.Add(r); ok {
            b.InsertPointer(i, ByteSlice64(0x0101010101010101))
            b.InsertPointer(i+1, ByteSlice64(2))
        }
        fmt.Println(b)

        r = b.NewRecord(ByteSlice64(1))
        r.Set(2, []byte{1, 2})
        if i, ok := b.Add(r); ok {
            b.InsertPointer(i, ByteSlice64(0x0101010101010101))
            b.InsertPointer(i+1, ByteSlice64(2))
        }
        fmt.Println(b)

        r = b.NewRecord(ByteSlice64(9))
        r.Set(2, []byte{1, 2})
        if i, ok := b.Add(r); ok {
            b.InsertPointer(i, ByteSlice64(0x0101010101010101))
            b.InsertPointer(i+1, ByteSlice64(2))
        }
        fmt.Println(b)
        r = b.NewRecord(ByteSlice64(7))
        r.Set(2, []byte{1, 2})
        if i, ok := b.Add(r); ok {
            b.InsertPointer(i, ByteSlice64(0x0101010101010101))
            b.InsertPointer(i+1, ByteSlice64(2))
        }
        fmt.Println(b)
        r = b.NewRecord(ByteSlice64(4))
        r.Set(2, []byte{1, 2})
        if i, ok := b.Add(r); ok {
            b.InsertPointer(i, ByteSlice64(0x0101010101010101))
            b.InsertPointer(i+1, ByteSlice64(2))
        }
        fmt.Println(b)
        r = b.NewRecord(ByteSlice64(0))
        r.Set(2, []byte{1, 2})
        if i, ok := b.Add(r); ok {
            b.InsertPointer(i, ByteSlice64(0x0101010101010101))
            b.InsertPointer(i+1, ByteSlice64(2))
        }
        b.SerializeToFile()
        fmt.Println(b)
    }

    if b, ok := NewKeyBlock(t, dim); ok {
        positions[1] = b.Position()
        r := b.NewRecord(ByteSlice64(3))
        r.Set(2, []byte{3, 4})
        if i, ok := b.Add(r); ok {
            b.InsertPointer(i, ByteSlice64(5))
            b.InsertPointer(i+1, ByteSlice64(2))
        }
        b.SerializeToFile()
        //         fmt.Println(b)
    }

    if b, ok := NewKeyBlock(t, dim); ok {
        positions[2] = b.Position()
        r := b.NewRecord(ByteSlice64(0x0900000000000000))
        r.Set(2, []byte{6, 12})
        if i, ok := b.Add(r); ok {
            b.InsertPointer(i, ByteSlice64(3))
            b.InsertPointer(i+1, ByteSlice64(3))
        }
        b.SerializeToFile()
        //         fmt.Println(b)
    }

    if b, ok := NewKeyBlock(t, dim); ok {
        positions[3] = b.Position()
        r := b.NewRecord(ByteSlice64(0x1001001001001001))
        r.Set(2, []byte{6, 12})
        if i, ok := b.Add(r); ok {
            b.InsertPointer(i, ByteSlice64(3))
            b.InsertPointer(i+1, ByteSlice64(4))
        }
        b.SerializeToFile()
        //         fmt.Println(b)
    }
    DeserializeFromFile(t, dim, positions[0])
    DeserializeFromFile(t, dim, positions[0])
    DeserializeFromFile(t, dim, positions[0])
    DeserializeFromFile(t, dim, positions[0])
    DeserializeFromFile(t, dim, positions[0])
    DeserializeFromFile(t, dim, positions[0])
    DeserializeFromFile(t, dim, positions[1])
    DeserializeFromFile(t, dim, positions[2])
    DeserializeFromFile(t, dim, positions[3])
    DeserializeFromFile(t, dim, positions[0])
    DeserializeFromFile(t, dim, positions[1])
    DeserializeFromFile(t, dim, positions[2])
    DeserializeFromFile(t, dim, positions[3])
    rb, ok := DeserializeFromFile(t, dim, positions[0])
    fmt.Println(ok)
    fmt.Println(rb)
    {
        i, _, _, _, _ := rb.Find(ByteSlice64(6))
        fmt.Println(i)
    }
    s, _ := t.Size()
    fmt.Println(s)
    fmt.Println(t.Close())
}
