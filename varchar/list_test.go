package varchar

import "testing"

import (
    "math/rand"
)

import (
    bs "file-structures/block/byteslice"
)

func TestMakeCloseVarcharList(t *testing.T) {
    varchar, _ := NewVarchar(testfile(t))
    list := MakeVarcharList(varchar)
    defer list.Close()
}

func TestNewPushReadFreeVarcharList(t *testing.T) {
    varchar, _ := NewVarchar(testfile(t))
    list := MakeVarcharList(varchar)
    defer list.Close()

    lists := make([][]bs.ByteSlice, 1000)
    list_keys := make([]int64, 1000)
    for i := range list_keys {
        key, err := list.New()
        if err != nil { t.Fatal(err) }
        list_keys[i] = key
    }

    for j := 0; j < 20; j++ {
        for i := range lists {
            item := randslice(rand.Intn(20)+20)
            lists[i] = append(lists[i], item)
            err := list.Push(list_keys[i], item)
            if err != nil { t.Fatal(err) }
        }
    }

    for j, data_list := range lists {
        read_list, err := list.GetList(list_keys[j])
        if err != nil { t.Fatal(err) }
        if len(read_list) != len(data_list) {
            t.Fatal("List sizes should match")
        }
        for i := range data_list {
            if !read_list[i].Eq(data_list[i]) {
                t.Fatal("List items should match")
            }
        }
    }

    for _, key := range list_keys {
        err := list.Free(key)
        if err != nil { t.Fatal(err) }
    }
}

