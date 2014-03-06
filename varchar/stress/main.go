package main

import (
    "os"
    "math/rand"
    "fmt"
    "strconv"
    "bufio"
    "io"
    "strings"
    "time"
    "runtime/pprof"
)

import (
    "github.com/timtadh/getopt"
    buf "file-structures/block/buffers"
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
    "file-structures/varchar"
)

var ErrorCodes map[string]int = map[string]int{
    "usage":1,
    "version":2,
    "opts":3,
    "config":4,
    "badint":5,
    "file-create":6,
    "file-open":7,
    "file-read":8,
    "getlist":9,
}

var profile_writer io.Writer

var UsageMessage string = "stress --mode=<mode> <list-path> <keys-path>"
var ExtendedMessage string = `

Options
    -h, --help                          print this message

Specs
    <path>
        A file system path to a file
    <mode>
        create, read
`

func Usage(code int) {
    fmt.Fprintln(os.Stderr, UsageMessage)
    if code == 0 {
        fmt.Fprintln(os.Stderr, ExtendedMessage)
        code = ErrorCodes["usage"]
    } else {
        fmt.Fprintln(os.Stderr, "Try -h or --help for help")
    }
    os.Exit(code)
}

func parse_int(str string) int {
    i, err := strconv.Atoi(str)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(ErrorCodes["badint"])
    }
    return i
}

func assert_file_does_not_exist(path string) {
    _, err := os.Stat(path)
    if err == nil {
        fmt.Fprintln(os.Stderr, "File already exists", path)
        Usage(ErrorCodes["opts"])
    } else if os.IsNotExist(err) {
        return
    } else {
        fmt.Fprintln(os.Stderr, "Stat Error", err.Error())
        Usage(ErrorCodes["opts"])
    }
}

func assert_file_exist(path string) {
    _, err := os.Stat(path)
    if err == nil {
        return
    } else if os.IsNotExist(err) {
        fmt.Fprintln(os.Stderr, "File does not exists", path)
        Usage(ErrorCodes["opts"])
    } else {
        fmt.Fprintln(os.Stderr, "Stat Error", err.Error())
        Usage(ErrorCodes["opts"])
    }
}

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

func testfile(path string) file.RemovableBlockDevice {
    ibf := file.NewBlockFileCustomBlockSize(path, &buf.NoBuffer{}, 4096*1)
    if err := ibf.Open(); err != nil {
        panic(err)
    }
    return ibf
}

const item_base_size = 4096 // 100
const item_variance = 1 // 4096*5
const list_base_size = 830 // 23
const list_variance = 1 // 830*2
const list_count = 10

func create(list_path, keys_path string) {
    ibf := testfile(list_path)
    // cf, err := file.OpenLRUCacheFile(ibf, 1024*1024*1024*2)
    // if err != nil { panic(err) }

    list := varchar.MakeVarcharList(ibf)
    defer list.Close()

    list_keys := make([]int64, list_count)
    list_size := make([]int, list_count)
    item_size := item_base_size + rand.Intn(item_variance)
    item := randslice(item_size)

    err := pprof.StartCPUProfile(profile_writer)
    if err != nil { panic(err) }

    start := time.Now()
    /*
    for j, key := range list_keys {
        for i := 0; i < list_size[j]; i++ {
            item_size := item_base_size + rand.Intn(item_variance)
            err := list.Push(key, randslice(item_size))
            if err != nil { panic(err) }
        }
        fmt.Fprintln(os.Stderr, "j", j)
    }
    */
    max_extra_small := 100
    max_extra_big := 5
    i := 0
    for {
        any_left := false
        for j, key := range list_keys {
            if key == 0 {
                var err error
                list_keys[j], err = list.New()
                if err != nil { panic(err) }
                list_size[j] = list_base_size + rand.Intn(list_variance)
                if j == 7 {
                    list_size[j] += rand.Intn(list_variance)*2
                }
                key = list_keys[j]
            }
            if list_size[j] <= 0 {
                continue
            } else {
                list_size[j] -= 1
                any_left = true
            }
            err := list.Push(key, item)
            if err != nil { panic(err) }
            if (j*i + int(key)) % 17 == 0 && max_extra_small > 0 {
                var err error
                list_key, err := list.New()
                if err != nil { panic(err) }
                list_keys = append(list_keys, list_key)
                list_size = append(list_size, 1)
                max_extra_small += 1
            }
            if (j*i + int(key)) % 57 == 0 && max_extra_big > 0 {
                var err error
                list_key, err := list.New()
                if err != nil { panic(err) }
                list_keys = append(list_keys, list_key)
                list_size = append(list_size, list_base_size)
                max_extra_big -= 1
            }
        }
        if i % 20 == 0 {
            fmt.Fprintln(os.Stderr, "i", i)
        }
        i += 1
        if !any_left {
            break
        }
    }
    end := time.Now()
    fmt.Println("duration", end.Sub(start).Seconds())

    pprof.StopCPUProfile()

    f, err := os.Create(keys_path)
    if err != nil {
        fmt.Fprintln(os.Stderr, err.Error())
        os.Exit(ErrorCodes["file-create"])
    }

    for i := range list_keys {
        j := rand.Intn(i+1)
        list_keys[i], list_keys[j] = list_keys[j], list_keys[i]
    }

    for _, key := range list_keys {
        fmt.Fprintln(f, key)
    }

    // if err := cf.Persist(); err != nil {
        // panic(err)
    // }
}

func read(list_path, keys_path string) {
    f, err := os.Open(keys_path)
    if err != nil {
        fmt.Fprintln(os.Stderr, err.Error())
        os.Exit(ErrorCodes["file-open"])
    }
    bf := bufio.NewReader(f)

    var keys []int64
    parse_key := func(s string) int64 {
        return int64(parse_int(s))
    }

    var line string
    for err != io.EOF {
        line, err = bf.ReadString('\n')

        if err != nil && err != io.EOF {
            fmt.Fprintln(os.Stderr, err.Error())
            os.Exit(ErrorCodes["file-read"])
        }

        line = strings.TrimSpace(line)
        if line != "" {
            keys = append(keys, parse_key(line))
        }
    }

    ibf := testfile(list_path)
    cf, err := file.OpenLRUCacheFile(ibf, 1024*1024*12)
    if err != nil { panic(err) }

    list := varchar.MakeVarcharList(cf)
    defer list.Close()

    err = pprof.StartCPUProfile(profile_writer)
    if err != nil { panic(err) }

    start := time.Now()
    for _, key := range keys {
        start := time.Now()
        list, err := list.GetList(key)
        if err != nil {
            fmt.Fprintln(os.Stderr, err.Error())
            os.Exit(ErrorCodes["getlist"])
        }
        end := time.Now()
        diff := end.Sub(start).Seconds()
        if diff > .1 {
            sum := 0
            for _, item := range list {
                sum += len(item)
            }
            fmt.Println("key", key, "duration", diff, len(list), sum)
        }
    }
    end := time.Now()
    fmt.Println("duration", end.Sub(start).Seconds())
    pprof.StopCPUProfile()
}

func main() {
    short := "hm:"
    long := []string{
        "help", "mode=",
    }

    args, optargs, err := getopt.GetOpt(os.Args[1:], short, long)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        Usage(ErrorCodes["opts"])
    }

    var mode string
    for _, oa := range optargs {
        switch oa.Opt() {
        case "-h", "--help": Usage(0)
        case "-m", "--mode":
            switch oa.Arg() {
            case "create", "read":
                mode = oa.Arg()
            default:
                fmt.Fprintf(os.Stderr, "mode %v not supported\n", oa.Arg())
                Usage(ErrorCodes["opts"])
            }
        }

    }

    if len(args) != 2 {
        fmt.Fprintf(os.Stderr, "Must supply exactly two file paths")
        Usage(ErrorCodes["opts"])
    }
    list_path := args[0]
    keys_path := args[1]

    f, err := os.Create("stress-"+mode+".prof")
    if err != nil { panic(err) }
    defer f.Close()
    profile_writer = f


    switch mode {
    case "create":
        assert_file_does_not_exist(list_path)
        assert_file_does_not_exist(keys_path)
        create(list_path, keys_path)
    case "read":
        assert_file_exist(list_path)
        assert_file_exist(keys_path)
        read(list_path, keys_path)
    }
}

