package file

import "os"
import "fmt"
// import "syscall"

// const OPENFLAG = os.O_RDWR | os.O_CREAT | syscall.O_DIRECT
var OPENFLAG = os.O_RDWR | os.O_CREAT

func (self *BlockFile) Open() bool {
    // the O_DIRECT flag turns off os buffering of pages allow us to do it manually
    // when using the O_DIRECT block size must be a multiple of 2048
    if f, err := os.Open(self.filename, OPENFLAG, 0666); err != nil {
        fmt.Println(err)
    } else {
        self.file = f
        self.opened = true
    }
    return self.opened
}
