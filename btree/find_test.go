package btree

import "testing"
import . "block/byteslice"

func TestFind(t *testing.T) {
//     fmt.Println("\n------  TestFind  ------")
    self := makebtree(ORDER_5)
    defer cleanbtree(self)
    order := 5
    n := order*(order+2)
    constructCompleteLevel2(self, order, n)
    self.Insert(ByteSlice32(uint32(n)), rec)

    for i := 1; i <= n; i++ {
        r, ok := self.Find(ByteSlice32(uint32(i)))
        if !ok {
            t.Errorf("could not find i in block")
        }
        if int(r.GetKey().Int32()) != i {
            t.Errorf("key of the returned record not the one searched for")
        }
    }
}
