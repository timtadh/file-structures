package main;

import . "file-structures/block/byteslice"
import "file-structures/bptree"
import "os"
import "bufio"
import "fmt"
import "encoding/json"
import "encoding/binary"
import "runtime"
import "runtime/pprof"
import "io/ioutil"

type Metadata struct {
    Path string
    Keysize uint32
    Fieldsizes []uint32
}

/*
type Command struct {
    Op string
    LeftKey []byte
    RightKey []byte
    Fields [][]byte
    FileName string
}
*/

const (
    insert = iota
    find
    quit
    size
    contains
)

const (
    cont = iota
    stop
)

func init() {
    runtime.GOMAXPROCS(2);
}

func main() {

    if false {
        if f, err := ioutil.TempFile(".", "b+bot.profile"); err != nil {
            panic(err)
        } else {
            pprof.StartCPUProfile(f)
            defer pprof.StopCPUProfile()
        }
    }

    // Read the string
    input := bufio.NewReader(os.Stdin)
    output := os.Stdout

    var info = Metadata{"", uint32(0), nil}
    // var cmd = Command{"", nil, nil, nil, ""}

    infoJson, err := input.ReadBytes('\n')
    if err != nil {
        panic(err)
    } else {
        json.Unmarshal(infoJson, &info)
    }

    bpt, bperr := bptree.NewBpTree(info.Path, info.Keysize, info.Fieldsizes)
    if !bperr {
        panic("Failed B+ tree creation")
    } else {
        fmt.Println("ok")
    }

    to_byte_slice := func (bytes []byte) []ByteSlice {
        B := make([]ByteSlice, 0, len(info.Fieldsizes))
        offset := 0
        for i := 0; i < len(info.Fieldsizes); i++ {
            size := int(info.Fieldsizes[i])
            bs := make([]byte, 0, size)
            for j := offset; j < offset+size; j++ {
                bs = append(bs, bytes[j])
            }
            B = append(B, ByteSlice(bs))
            offset += size
        }
        return B
    }

    var total_field_bytes int
    for i := range info.Fieldsizes {
        total_field_bytes += int(info.Fieldsizes[i])
    }

    serveloop:
    for {
        var cmd_type byte
        if err := binary.Read(input, binary.LittleEndian, &cmd_type); err != nil {
            break
        }
        if cmd_type == quit {
            break
        } else if cmd_type == insert {
            key := make([]byte, info.Keysize)
            fields := make([]byte, total_field_bytes)
            if err := binary.Read(input, binary.LittleEndian, &key); err != nil {
                break
            }
            if err := binary.Read(input, binary.LittleEndian, &fields); err != nil {
                break
            }
            /*
            fmt.Fprintf(os.Stderr, "Key = '%v'\n", key)
            fmt.Fprintf(os.Stderr, "Fields = '%v'\n", fields)
            bfields := to_byte_slice(fields)
            for i := range bfields {
                fmt.Fprintf(os.Stderr, "bytesliced field (%v) = '%v'\n", i, bfields[i])
            }
            */
            result := bpt.Insert(key[:], to_byte_slice(fields))
            fmt.Println(result)
        } else if cmd_type == size {
            size := ByteSlice64(bpt.Size())
            output.Write(size)
        } else if cmd_type == contains {
            key := make([]byte, info.Keysize)
            if err := binary.Read(input, binary.LittleEndian, &key); err != nil {
                break
            }
            if bpt.Contains(key) {
                output.Write(ByteSlice8(1))
            } else {
                output.Write(ByteSlice8(0))
            }
        } else if cmd_type == find {
            leftkey := make([]byte, info.Keysize)
            rightkey := make([]byte, info.Keysize)
            if err := binary.Read(input, binary.LittleEndian, &leftkey); err != nil {
                break
            }
            if err := binary.Read(input, binary.LittleEndian, &rightkey); err != nil {
                break
            }
            records := bpt.Find(leftkey, rightkey)
            for record := range records {
                if err := binary.Write(output, binary.LittleEndian, byte(cont)); err != nil {
                    fmt.Fprintln(os.Stderr, err)
                    break serveloop
                }
                if _, err := output.Write(record.Bytes()); err != nil {
                    fmt.Fprintln(os.Stderr, err)
                    break serveloop
                }
            }
            if err := binary.Write(output, binary.LittleEndian, byte(stop)); err != nil {
                fmt.Fprintln(os.Stderr, err)
                break serveloop
            }
        } else {
            fmt.Fprintf(os.Stderr, "Bad command type %v\n", cmd_type)
        }
    }
    fmt.Println("exited")
}

// Determine which file and schema is being opened
//  (filename string, keysize uint32, fields []uint32)

// insert(key Byteslice, record []Byteslice)
// find(leftkey, right key) returns channel with all matching keys+records (Record structs)

