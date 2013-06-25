package file2

import . "file-structures/block/byteslice"

type BlockSizer interface {
    BlkSize() uint32
}

type BlockReader interface {
    BlockSizer
    ReadBlock(key int64) (block ByteSlice, err error)
}

type BlockWriter interface {
    BlockSizer
    WriteBlock(key int64, block ByteSlice) error
}

type BlockReadWriter interface {
    BlockSizer
    ReadBlock(key int64) (block ByteSlice, err error)
    WriteBlock(key int64, block ByteSlice) error
}

type BlockAllocator interface {
    BlockSizer
    Free(key int64) error
    Allocate() (key int64, err error)
}

type Closer interface {
    Close() error
}

type Removable interface {
    Remove() error
}

type RootController interface {
    ControlData() (block ByteSlice, err error)
    SetControlData(block ByteSlice) (err error)
}

type BlockDevice interface {
    BlockReadWriter
    Free(key int64) error
    Allocate() (key int64, err error)
    Closer
    RootController
}

type RemovableBlockDevice interface {
    BlockDevice
    Removable
}

