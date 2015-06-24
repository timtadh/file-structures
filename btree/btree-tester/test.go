package main

import "fmt"
import . "file-structures/btree"
import . "file-structures/block/byteslice"

func main() {

	var fac func(int) int
	fac = func(i int) int {
		if i <= 1 {
			return 1
		}
		return i * fac(i-1)
	}

	fmt.Println("test2 yoyo")
	btree, _ := NewBTree("hello.btree", 4, ([]uint32{1, 1, 2}))
	rec := []ByteSlice{[]byte{1}, []byte{1}, []byte{1, 2}}
	//     fmt.Println(btree)
	fmt.Println(btree.Insert(ByteSlice32(1), rec))
	fmt.Println(btree.Insert(ByteSlice32(5), rec))
	fmt.Println(btree.Insert(ByteSlice32(20), rec))
	fmt.Println(btree)
	fmt.Println(btree.Insert(ByteSlice32(9), rec))
	fmt.Println(btree.Insert(ByteSlice32(11), rec))
	fmt.Println(btree.Insert(ByteSlice32(3), rec))
	fmt.Println(btree.Insert(ByteSlice32(7), rec))
	fmt.Println(btree.Insert(ByteSlice32(8), rec))
	fmt.Println(btree)
	fmt.Println(btree.Insert(ByteSlice32(12), rec))
	fmt.Println(btree.Insert(ByteSlice32(30), rec))
	fmt.Println(btree.Insert(ByteSlice32(35), rec))
	fmt.Println(btree.Insert(ByteSlice32(15), rec))
	fmt.Println(btree.Insert(ByteSlice32(43), rec))
	fmt.Println(btree.Insert(ByteSlice32(31), rec))
	fmt.Println(btree.Insert(ByteSlice32(24), rec))
	fmt.Println(btree.Insert(ByteSlice32(6), rec))
	fmt.Println(btree.Insert(ByteSlice32(21), rec))
	fmt.Println(btree.Insert(ByteSlice32(26), rec))
	fmt.Println(btree.Insert(ByteSlice32(16), rec))
	fmt.Println(btree.Insert(ByteSlice32(44), rec))
	fmt.Println(btree.Insert(ByteSlice32(14), rec))
	fmt.Println(btree.Insert(ByteSlice32(40), rec))
	fmt.Println(btree.Insert(ByteSlice32(28), rec))
	fmt.Println(btree.Insert(ByteSlice32(34), rec))
	fmt.Println(btree.Insert(ByteSlice32(22), rec))
	fmt.Println(btree.Insert(ByteSlice32(17), rec))
	fmt.Println(btree.Insert(ByteSlice32(10), rec))
	fmt.Println(btree.Insert(ByteSlice32(25), rec))
	fmt.Println(btree.Insert(ByteSlice32(41), rec))
	fmt.Println(btree.Insert(ByteSlice32(29), rec))
	fmt.Println(btree.Insert(ByteSlice32(18), rec))
	fmt.Println(btree.Insert(ByteSlice32(33), rec))
	fmt.Println(btree.Insert(ByteSlice32(42), rec))
	fmt.Println(btree.Insert(ByteSlice32(13), rec))
	fmt.Println(btree.Insert(ByteSlice32(27), rec))
	fmt.Println(btree.Insert(ByteSlice32(23), rec))
	fmt.Println(btree.Insert(ByteSlice32(19), rec))
	fmt.Println(btree.Insert(ByteSlice32(32), rec))
	//     fmt.Println(btree)
	fmt.Println(btree)
	//     fmt.Println(btree.Insert(ByteSlice32(2), rec))
	//     fmt.Println(btree.Insert(ByteSlice32(4), rec))
	//     fmt.Println(btree.Insert(ByteSlice32(19), rec))
	//     fmt.Println(btree)

	fmt.Println(fac(5))
	Dotty("out.dot", btree)
}
