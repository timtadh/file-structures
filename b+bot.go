package main;

// import "bptree"
// import "block/byteslice"
import "os"
import "bufio"
import "fmt"
// import "json"
import "log"

func main() {
    // Read the string
    inputReader := bufio.NewReader(os.Stdin)
    
    alive := true;
    
    for alive {
        testString, err1 := inputReader.ReadString('\n')
        if err1 != nil {
            log.Exit(err1)
        }
        if testString == "q\n" {
            alive = false
        } else {
            fmt.Print(testString)
        }
    }
    fmt.Print("exited")
}

// Determine which file and schema is being opened
//  (filename string, keysize uint32, fields []uint32)

// insert(key Byteslice, record []Byteslice)
// find(leftkey, right key) returns channel with all matching keys+records (Record structs)
