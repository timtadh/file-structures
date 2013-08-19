package varchar

import "testing"

import (
    "math/rand"
)

import (
    file "file-structures/block/file2"
    buf "file-structures/block/buffers"
    bs "file-structures/block/byteslice"
)

func TestMakeCloseVarcharList(t *testing.T) {
    f := testfile(t)
    list := MakeVarcharList(f)
    defer func() {
        err := list.Close()
        if err != nil {
            panic(err)
        }
        err = f.Remove()
        if err != nil {
            panic(err)
        }
    }()
}

func TestCompleteNewPushReadFreeList(t *testing.T) {
    my_testfile := func(t *testing.T) file.RemovableBlockDevice {
        const CACHESIZE = 100000
        ibf := file.NewBlockFileCustomBlockSize("/tmp/__testCompleteNewPushReadFreeList", &buf.NoBuffer{}, 4096)
        if err := ibf.Open(); err != nil {
            t.Fatal(err)
        }
        return ibf
    }
    f := my_testfile(t)
    list := MakeVarcharList(f)
    defer func() {
        err := list.Close()
        if err != nil {
            panic(err)
        }
        err = f.Remove()
        if err != nil {
            panic(err)
        }
    }()

    lists := make([][]bs.ByteSlice, 1)
    list_keys := make([]int64, 1)
    for i := range list_keys {
        key, err := list.New()
        if err != nil {
            t.Fatal(err)
        }
        list_keys[i] = key
    }

    for j := 0; j < 4096*5; j++ {
        for i := range lists {
            item := randslice(j + 1)
            lists[i] = append(lists[i], item)
            err := list.Push(list_keys[i], item)
            if err != nil {
                t.Fatal(err)
            }

            for k := 0; k < 10; k++ {
                item = randslice(rand.Intn(20) + 20)
                lists[i] = append(lists[i], item)
                err = list.Push(list_keys[i], item)
                if err != nil {
                    t.Fatal(err)
                }
            }
        }
    }

    for j, data_list := range lists {
        // for i, list := range data_list {
        // t.Log(i, len(list))
        // }
        read_list, err := list.GetList(list_keys[j])
        if err != nil {
            t.Fatal(err)
        }
        if len(read_list) != len(data_list) {
            t.Log(len(read_list), len(data_list))
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
        if err != nil {
            t.Fatal(err)
        }
    }
}

func TestNewPushReadFreeVarcharList(t *testing.T) {
    my_testfile := func(t *testing.T) file.RemovableBlockDevice {
        const CACHESIZE = 100000
        ibf := file.NewBlockFileCustomBlockSize("/tmp/__testNewPushReadFreeVarchar", &buf.NoBuffer{}, 4096)
        if err := ibf.Open(); err != nil {
            t.Fatal(err)
        }
        return ibf
    }
    f := my_testfile(t)
    list := MakeVarcharList(f)
    defer func() {
        err := list.Close()
        if err != nil {
            panic(err)
        }
        err = f.Remove()
        if err != nil {
            panic(err)
        }
    }()

    lists := make([][]bs.ByteSlice, 100)
    list_keys := make([]int64, 100)
    for i := range list_keys {
        key, err := list.New()
        if err != nil {
            t.Fatal(err)
        }
        list_keys[i] = key
    }

    for j := 0; j < rand.Intn(100)+5; j++ {
        for i := range lists {
            item := randslice(rand.Intn(1) + 17473)
            lists[i] = append(lists[i], item)
            err := list.Push(list_keys[i], item)
            if err != nil {
                t.Fatal(err)
            }

            item = randslice(rand.Intn(2000) + 3000)
            lists[i] = append(lists[i], item)
            err = list.Push(list_keys[i], item)
            if err != nil {
                t.Fatal(err)
            }

            for k := 0; k < 10; k++ {
                item = randslice(rand.Intn(20) + 20)
                lists[i] = append(lists[i], item)
                err = list.Push(list_keys[i], item)
                if err != nil {
                    t.Fatal(err)
                }
            }

            for k := 0; k < 10; k++ {
                item = randslice(rand.Intn(200) + 300)
                lists[i] = append(lists[i], item)
                err = list.Push(list_keys[i], item)
                if err != nil {
                    t.Fatal(err)
                }
            }
        }
    }

    for j, data_list := range lists {
        // for i, list := range data_list {
        // t.Log(i, len(list))
        // }
        read_list, err := list.GetList(list_keys[j])
        if err != nil {
            t.Fatal(err)
        }
        if len(read_list) != len(data_list) {
            t.Log(len(read_list), len(data_list))
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
        if err != nil {
            t.Fatal(err)
        }
    }
}
