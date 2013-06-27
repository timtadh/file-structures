package linhash

import (
    "fmt"
)

import (
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
)


type ctrlblk struct {
}

const CONTROLSIZE = 0
func (self *ctrlblk) Bytes() []byte {
    bytes := make([]byte, CONTROLSIZE)
    return bytes
}

func load_ctrlblk(bytes []byte) (cb *ctrlblk, err error) {
    if len(bytes) < CONTROLSIZE {
        return nil, fmt.Errorf("len(bytes) < %d", CONTROLSIZE)
    }
    cb = &ctrlblk{
    }
    return cb, nil
}

type LinearHash struct {
    file file.BlockDevice
    ctrl ctrlblk
}

func NewLinearHash(file file.BlockDevice) (self *LinearHash, err error) {
    self = &LinearHash{
        file: file,
        ctrl: ctrlblk{
        },
    }
    return self, self.write_ctrlblk()
}

func OpenLinearHash(file file.BlockDevice) (self *LinearHash, err error) {
    self = &LinearHash{
        file: file,
    }
    if err := self.read_ctrlblk(); err != nil {
        return nil, err
    }
    return self, nil
}

func (self *LinearHash) Close() error {
    return self.file.Close()
}

func (self *LinearHash) write_ctrlblk() error {
    return self.file.SetControlData(self.ctrl.Bytes())
}

func (self *LinearHash) read_ctrlblk() error {
    if bytes, err := self.file.ControlData(); err != nil {
        return err
    } else {
        if cb, err := load_ctrlblk(bytes); err != nil {
            return err
        } else {
            self.ctrl = *cb
        }
    }
    return nil
}

func (self *LinearHash) Has(key bs.ByteSlice) (has bool, error error) {
    return false, fmt.Errorf("Has Unimplemented")
}

func (self *LinearHash) Put(key bs.ByteSlice, value bs.ByteSlice) (err error) {
    return fmt.Errorf("Put Unimplemented")
}

func (self *LinearHash) Get(key bs.ByteSlice) (value bs.ByteSlice, err error) {
    return nil, fmt.Errorf("Get Unimplemented")
}

func (self *LinearHash) DefaultGet(key bs.ByteSlice, default_value bs.ByteSlice) (value bs.ByteSlice, err error) {
    return nil, fmt.Errorf("DefaultGet Unimplemented")
}

func (self *LinearHash) Remove(key bs.ByteSlice) (err error) {
    return fmt.Errorf("Remove Unimplemented")
}

