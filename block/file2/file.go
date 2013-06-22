package file2

import (
    "fmt"
    "hash/crc32"
    "os"
)

import . "file-structures/block/buffers"
import . "file-structures/block/byteslice"

type ctrlblk struct {
    blksize    uint32
    free_chain uint64
    free_len   uint32
}

func (self *ctrlblk) Bytes() []byte {
    var bytes [20]byte
    copy(bytes[4:8], ByteSlice32(self.blksize))
    copy(bytes[8:16], ByteSlice64(self.free_chain))
    copy(bytes[16:20], ByteSlice32(self.free_len))
    copy(bytes[0:4], ByteSlice32(crc32.ChecksumIEEE(bytes[4:])))
    return bytes[:]
}

func (self *ctrlblk) Block() []byte {
    blk := make([]byte, self.blksize)
    copy(blk, self.Bytes())
    return blk
}

func load_ctrlblk(bytes []byte) (cb *ctrlblk, err error) {
    chksum := ByteSlice(bytes[0:4]).Int32()
    new_chksum := crc32.ChecksumIEEE(bytes[4:20])
    if new_chksum != chksum {
        return nil, fmt.Errorf("Bad control block checksum %x != %x", new_chksum, chksum)
    }
    cb = &ctrlblk{
        blksize:    ByteSlice(bytes[4:8]).Int32(),
        free_chain: ByteSlice(bytes[8:16]).Int64(),
        free_len:   ByteSlice(bytes[16:20]).Int32(),
    }
    return cb, nil
}

type BlockFile struct {
    path   string
    opened bool
    buf    Buffer
    file   *os.File
    ctrl   ctrlblk
}

func NewBlockFile(path string, buf Buffer) *BlockFile {
    return &BlockFile{
        path: path,
        buf:  buf,
        ctrl: ctrlblk{
            blksize:    4096,
            free_chain: 0,
        },
    }
}

func (self *BlockFile) Open() error {
    if err := self.open(); err != nil {
        return err
    }
    if size, err := self.Size(); err != nil {
        return err
    } else if size == 0 {
        if _, err := self.Allocate(); err != nil {
            return err
        } else {
            if err := self.write_ctrlblk(); err != nil {
                return err
            }
        }
    } else {
        if err := self.read_ctrlblk(); err != nil {
            return err
        }
    }
    return nil
}

func (self *BlockFile) Close() error {
    if err := self.file.Close(); err != nil {
        return err
    } else {
        self.file = nil
        self.opened = false
    }
    return nil
}

func (self *BlockFile) write_ctrlblk() error {
    return self.WriteBlock(0, self.ctrl.Block())
}

func (self *BlockFile) read_ctrlblk() error {
    if bytes, err := self.ReadBlock(0); err != nil {
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

func (self *BlockFile) Path() string { return self.path }

func (self *BlockFile) BlkSize() uint32 { return self.ctrl.blksize }

func (self *BlockFile) Size() (uint64, error) {
    if !self.opened {
        return 0, fmt.Errorf("File is not open")
    }
    dir, err := os.Stat(self.path)
    if err != nil {
        return 0, err
    }
    return uint64(dir.Size()), nil
}

func (self *BlockFile) resize(size int64) error {
    return self.file.Truncate(size)
}

func (self *BlockFile) Free(pos int64) error {
    head := ByteSlice64(self.ctrl.free_chain)
    blk := make(ByteSlice, self.ctrl.blksize)
    copy(blk, head)
    if err := self.WriteBlock(pos, blk); err != nil {
        return err
    }
    self.ctrl.free_chain = uint64(pos)
    self.ctrl.free_len += 1
    return self.write_ctrlblk()
}

func (self *BlockFile) pop_free() (pos int64, err error) {
    if self.ctrl.free_chain == 0 && self.ctrl.free_len == 0 {
        return 0, fmt.Errorf("No blocks free")
    }
    pos = int64(self.ctrl.free_chain)
    if bytes, err := self.ReadBlock(pos); err != nil {
        return 0, err
    } else {
        self.ctrl.free_chain = bytes[0:8].Int64()
    }
    self.ctrl.free_len -= 1
    if err := self.write_ctrlblk(); err != nil {
        return 0, err
    }
    return pos, err
}

func (self *BlockFile) alloc() (pos int64, err error) {
    var size uint64
    if size, err = self.Size(); err != nil {
        return 0, err
    }
    if err := self.resize(int64(size + uint64(self.ctrl.blksize))); err != nil {
        return 0, err
    }
    return int64(size), nil
}

func (self *BlockFile) Allocate() (pos int64, err error) {
    if self.ctrl.free_len == 0 {
        return self.alloc()
    }
    return self.pop_free()
}

func (self *BlockFile) WriteBlock(p int64, block ByteSlice) error {
    if !self.opened {
        return fmt.Errorf("File is not open")
    }
    if b, ok := self.buf.Read(p, uint32(len(block))); ok {
        if ByteSlice(b).Eq(block) {
            // skip write no change in block from what is in cache
            return nil
        }
    }
    for pos, err := self.file.Seek(p, 0); pos != p; pos, err = self.file.Seek(p, 0) {
        if err != nil {
            return err
        }
    }
    if _, err := self.file.Write(block); err != nil {
        return err
    }
    self.buf.Update(p, block)
    return nil
}

func (self *BlockFile) ReadBlock(p int64) (ByteSlice, error) {
    if !self.opened {
        return nil, fmt.Errorf("File is not open")
    }
    if b, ok := self.buf.Read(p, self.ctrl.blksize); ok {
        return b, nil
    }
    block := make([]byte, self.ctrl.blksize)
    for pos, err := self.file.Seek(p, 0); pos != p; pos, err = self.file.Seek(p, 0) {
        if err != nil {
            return nil, err
        }
    }
    if _, err := self.file.Read(block); err != nil {
        return nil, err
    }
    self.buf.Update(p, block)
    return block, nil
}
