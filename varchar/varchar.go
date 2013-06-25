package varchar

import (
    "fmt"
)

import (
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
)

type Varchar struct {
    file file.BlockDevice
}

func NewVarchar(file file.BlockDevice) *Varchar {
    return &Varchar{
        file: file,
    }
}

func (self *Varchar) Close() error {
    return self.file.Close()
}

func (self *Varchar) Write(bytes bs.ByteSlice) (key int64, err error) {
    return 0, fmt.Errorf("Unimplemented")
}

func (self *Varchar) Update(key int64, bytes bs.ByteSlice) (err error) {
    return fmt.Errorf("Unimplemented")
}

func (self *Varchar) Read(key int64) (bytes bs.ByteSlice, err error) {
    return nil, fmt.Errorf("Unimplemented")
}

func (self *Varchar) Remove(key int64) (err error) {
    return fmt.Errorf("Unimplemented")
}

