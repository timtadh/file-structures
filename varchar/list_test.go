package varchar

import "testing"

import (
    "math/rand"
)

import (
    bs "file-structures/block/byteslice"
)

func TestMakeCloseVarcharList(t *testing.T) {
    list := MakeVarcharList(testfile(t))
    defer list.Close()
}

func TestNewPushReadFreeVarcharList(t *testing.T) {
    list := MakeVarcharList(testfile(t))
    defer list.Close()

    lists := make([][]bs.ByteSlice, 750)
    list_keys := make([]int64, 750)
    for i := range list_keys {
        key, err := list.New()
        if err != nil { t.Fatal(err) }
        list_keys[i] = key
    }

    for j := 0; j < rand.Intn(200)+5; j++ {
        for i := range lists {
            item := randslice(rand.Intn(2000) + 3000)
            lists[i] = append(lists[i], item)
            err := list.Push(list_keys[i], item)
            if err != nil { t.Fatal(err) }
        }
    }

    for j, data_list := range lists {
        // for i, list := range data_list {
            // t.Log(i, len(list))
        // }
        read_list, err := list.GetList(list_keys[j])
        if err != nil { t.Fatal(err) }
        if len(read_list) != len(data_list) {
            t.Log(read_list, data_list)
            t.Fatal("List sizes should match")
        }
        for i := range data_list {
            if !read_list[i].Eq(data_list[i]) {
                t.Log(read_list[i], data_list[i])
                t.Fatal("List items should match")
            }
        }
    }

    for _, key := range list_keys {
        err := list.Free(key)
        if err != nil { t.Fatal(err) }
    }
}

