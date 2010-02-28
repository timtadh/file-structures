package btree

import "testing"
import "fmt"
import . "block/keyblock"
import . "block/byteslice"

const ORDER_2 = 45
const ORDER_3 = 65
const ORDER_4 = 85
const ORDER_5 = 105

func TestOrder(t *testing.T) {
    fmt.Println("\n\n\n------  TestOrder  ------")
    order2 := makebtree(ORDER_2)
    if order2.node.KeysPerBlock() != 2 {
        t.Error("order 2 btree not order 2 it is order", order2.node.KeysPerBlock())
    }
    cleanbtree(order2)
    
    order3 := makebtree(ORDER_3)
    if order3.node.KeysPerBlock() != 3 {
        t.Error("order 2 btree not order 2 it is order", order3.node.KeysPerBlock())
    }
    cleanbtree(order3)
    
    order4 := makebtree(ORDER_4)
    if order4.node.KeysPerBlock() != 4 {
        t.Error("order 2 btree not order 2 it is order", order4.node.KeysPerBlock())
    }
    cleanbtree(order4)
    
    order5 := makebtree(ORDER_5)
    if order5.node.KeysPerBlock() != 5 {
        t.Error("order 2 btree not order 2 it is order", order5.node.KeysPerBlock())
    }
    cleanbtree(order5)
}

func insert(self *BTree, a *KeyBlock, key ByteSlice) bool {
    r := self.node.NewRecord(key)
    for i, f := range rec {
        r.Set(uint32(i), f)
    }
    j, b := a.Add(r)
    if b {
        if j == 0 {
            a.InsertPointer(j, ByteSlice64(uint64(key.Int32()+1)))
        }
        a.InsertPointer(j+1, ByteSlice64(uint64(key.Int32()+2)))
    }
    return b
}

func testBalanceBlocks(self *BTree, t *testing.T) {
    a := self.getblock(self.root)
    b := self.allocate()
    
    for i := uint32(0); int(i) < self.node.KeysPerBlock(); i++ {
        if !insert(self, a, ByteSlice32(i)) {
            t.Errorf("failed inserting ith, %v, value in block of order %v", i+1, self.node.KeysPerBlock())
        }
    }
    self.balance_blocks(a, b)
    if a.RecordCount() > b.RecordCount() {
        t.Errorf("a.RecordCount() > b.RecordCount()")
    }
    if a.PointerCount() < b.PointerCount() {
        t.Errorf("a.PointerCount() < b.PointerCount()")
    }
    if a.RecordCount() != b.RecordCount() && a.PointerCount()+1 != b.RecordCount()+1 {
        t.Errorf("record balance off")
    }
    if a.PointerCount() != b.PointerCount() && a.PointerCount() != b.PointerCount()+1 {
        t.Errorf("pointer balance off")
    }
}

func TestBalanceBlocksO2(t *testing.T) {
    fmt.Println("\n\n\n------  TestBalanceBlocksO2  ------")
    self := makebtree(ORDER_2)
    defer cleanbtree(self)
    testBalanceBlocks(self, t)
}

func TestBalanceBlocksO3(t *testing.T) {
    fmt.Println("\n\n\n------  TestBalanceBlocksO3  ------")
    self := makebtree(ORDER_3)
    defer cleanbtree(self)
    testBalanceBlocks(self, t)
}

func TestBalanceBlocksO4(t *testing.T) {
    fmt.Println("\n\n\n------  TestBalanceBlocksO4  ------")
    self := makebtree(ORDER_4)
    defer cleanbtree(self)
    testBalanceBlocks(self, t)
}

func TestBalanceBlocksO5(t *testing.T) {
    fmt.Println("\n\n\n------  TestBalanceBlocksO5  ------")
    self := makebtree(ORDER_5)
    defer cleanbtree(self)
    testBalanceBlocks(self, t)
}
