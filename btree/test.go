package main

import "fmt"
import . "btree"
// import . "block/file"
// import . "block/keyblock"
// import . "block/buffers"
import . "block/byteslice"

func main() {
    
    var fac func(int) int 
    fac = func(i int) int {
        if i <= 1 { return 1 }
        return i*fac(i-1)
    }


    fmt.Println("test2 yoyo")
    btree, _ := NewBTree("hello.btree", 4, &([3]uint32{1, 1, 2}))
    rec := &([3][]byte{&[1]byte{1}, &[1]byte{1}, &[2]byte{1, 2}})
    //     fmt.Println(btree)
    fmt.Println(btree.Insert(ByteSlice32(1), rec))
    fmt.Println(btree.Insert(ByteSlice32(5), rec))
    fmt.Println(btree.Insert(ByteSlice32(9), rec))
//     fmt.Println(btree.Insert(ByteSlice32(5), rec))
        fmt.Println(btree)
    fmt.Println(btree.Insert(ByteSlice32(3), rec))
    fmt.Println(btree.Insert(ByteSlice32(7), rec))
    fmt.Println(btree.Insert(ByteSlice32(8), rec))
    fmt.Println(btree)
    fmt.Println(btree.Insert(ByteSlice32(15), rec))
    fmt.Println(btree.Insert(ByteSlice32(6), rec))
    fmt.Println(btree)
    fmt.Println(btree.Insert(ByteSlice32(12), rec))
    fmt.Println(btree)
    
    
    fmt.Println(fac(5))
}
