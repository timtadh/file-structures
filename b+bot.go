package main;

// import "bptree"
import . "block/byteslice"
import "os"
import "bufio"
import "fmt"
import "json"
import "log"

type Metadata struct {
    Filename string
    Keysize uint32
    Fieldsizes []uint32
}

type Command struct {
    Op string
    LeftKey ByteSlice
    RightKey ByteSlice
    Fields []ByteSlice
}

func main() {
    // Read the string
    inputReader := bufio.NewReader(os.Stdin)
    
    var info = Metadata{"", uint32(0), nil}
    var cmd = Command{"", nil, nil, nil}
    
    infoJson, err := inputReader.ReadString('\n')
    if err != nil {
        log.Exit(err)
    } else {
        json.Unmarshal(infoJson, &info)
    }
    fmt.Println(info.Filename)
    
    alive := true;
    
    for alive {
        cmdJson, err := inputReader.ReadString('\n')
        if err != nil {
            log.Exit(err)
        }
        if cmdJson == "q\n" {
            alive = false
        } else {
            json.Unmarshal(cmdJson, &cmd)
            fmt.Println(cmd.Fields)
        }
    }
    fmt.Println("exited")
}

// Determine which file and schema is being opened
//  (filename string, keysize uint32, fields []uint32)

// insert(key Byteslice, record []Byteslice)
// find(leftkey, right key) returns channel with all matching keys+records (Record structs)
