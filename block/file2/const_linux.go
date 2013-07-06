// +build linux

package file2

import "os"

/*
The O_DIRECT flag seems to lower performance. So I am turning it off. O_SYNC
flag REALLY lowers performance.
*/
// import "syscall"
// var OPENFLAG = os.O_RDWR | os.O_CREATE | syscall.O_DIRECT // | os.O_SYNC

var OPENFLAG = os.O_RDWR | os.O_CREATE

func (self *BlockFile) open() error {
    // the O_DIRECT flag turns off os buffering of pages allow us to do it manually
    // when using the O_DIRECT block size must be a multiple of 2048
    if f, err := os.OpenFile(self.path, OPENFLAG, 0666); err != nil {
        return err
    } else {
        self.file = f
        self.opened = true
    }
    return nil
}

