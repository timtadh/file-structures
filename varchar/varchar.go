package varchar

import (
    "fmt"
)

import (
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
)

const LENSIZE = 8
const PTRSIZE = 8

const METADATASIZE = 8
type metadata struct {
    next int64
}

func (self *metadata) Bytes() []byte {
    bytes := make([]byte, METADATASIZE)
    copy(bytes[0:8], bs.ByteSlice64(uint64(self.next)))
    return bytes
}

func load_metadata(bytes bs.ByteSlice) (md *metadata, err error) {
    if len(bytes) < METADATASIZE {
        return nil, fmt.Errorf("len(bytes) < %d", METADATASIZE)
    }
    md = &metadata{
        next: int64(bytes[0:8].Int64()),
    }
    return md, nil
}

type block struct {
    key int64
    block bs.ByteSlice
    data bs.ByteSlice
    metadata bs.ByteSlice
}

func datasize(file file.BlockDevice) int64 {
    return int64(file.BlockSize()) - METADATASIZE
}

func load_block(key int64, bytes bs.ByteSlice) (blk *block) {
    size := len(bytes)
    offset := size - METADATASIZE
    return &block{
        key: key,
        block: bytes,
        data: bytes[:offset],
        metadata: bytes[offset:],
    }
}

func (self *block) Metadata() *metadata {
    md, err := load_metadata(self.metadata)
    if err != nil {
        panic(err)
    }
    return md
}

func (self *block) SetMetadata(md *metadata) {
    copy(self.metadata, md.Bytes())
}

func (self *block) WriteBlock(file file.BlockDevice) error {
    return file.WriteBlock(self.key, self.block)
}

func readBlock(file file.BlockDevice, key int64) (blk *block, err error) {
    bytes, err := file.ReadBlock(key)
    if err != nil {
        return nil, err
    }
    return load_block(key, bytes), err
}

func allocBlock(file file.BlockDevice) (blk *block, err error) {
    key, err := file.Allocate()
    if err != nil {
        return nil, err
    }
    size := file.BlockSize()
    offset := size - METADATASIZE
    bytes := make(bs.ByteSlice, size)
    blk = &block{
        key: key,
        block: bytes,
        data: bytes[:offset],
        metadata: bytes[offset:],
    }
    fmt.Println("allocated", key)
    return blk, nil
}

const FREE_VARCHAR_SIZE = 16
type free_varchar struct {
    key int64
    length uint64
    next int64
}

func (self *free_varchar) Bytes() []byte {
    bytes := make([]byte, FREE_VARCHAR_SIZE)
    copy(bytes[0:8], bs.ByteSlice64(self.length))
    copy(bytes[8:16], bs.ByteSlice64(uint64(self.next)))
    return bytes
}

func load_free_varchar(bytes bs.ByteSlice, key int64) (fv *free_varchar, err error) {
    if len(bytes) < FREE_VARCHAR_SIZE {
        return nil, fmt.Errorf("len(bytes) < %d", FREE_VARCHAR_SIZE)
    }
    fv = &free_varchar{
        key: key,
        length: bytes[0:8].Int64(),
        next: int64(bytes[8:16].Int64()),
    }
    return fv, nil
}

func (self *free_varchar) writeFreeVarchar(blk *block) {
    offset := self.key - blk.key
    copy(blk.data[offset:offset+FREE_VARCHAR_SIZE], self.Bytes())
}

func readFreeVarchar(blk *block, key int64) (fv *free_varchar, err error) {
    offset := key - blk.key
    return load_free_varchar(blk.data[offset:offset+FREE_VARCHAR_SIZE], key)
}

type ctrlblk struct {
    end int64
    free_head int64
    free_len  uint32
}

const CONTROLSIZE = 20
func (self *ctrlblk) Bytes() []byte {
    bytes := make([]byte, CONTROLSIZE)
    copy(bytes[0:8], bs.ByteSlice64(uint64(self.end)))
    copy(bytes[8:16], bs.ByteSlice64(uint64(self.free_head)))
    copy(bytes[16:20], bs.ByteSlice32(uint32(self.free_len)))
    return bytes
}

func load_ctrlblk(bytes []byte) (cb *ctrlblk, err error) {
    if len(bytes) < CONTROLSIZE {
        return nil, fmt.Errorf("len(bytes) < %d", CONTROLSIZE)
    }
    cb = &ctrlblk{
        end:       int64(bs.ByteSlice(bytes[0:8]).Int64()),
        free_head: int64(bs.ByteSlice(bytes[8:16]).Int64()),
        free_len:  bs.ByteSlice(bytes[16:20]).Int32(),
    }
    return cb, nil
}

type Varchar struct {
    file file.BlockDevice
    ctrl ctrlblk
}

func NewVarchar(file file.BlockDevice) (self *Varchar, err error) {
    if blk, err := allocBlock(file); err != nil {
        return nil, err
    } else {
        self = &Varchar{
            file: file,
            ctrl: ctrlblk{
                end: blk.key,
            },
        }
    }
    return self, self.write_ctrlblk()
}

func OpenVarchar(file file.BlockDevice) (self *Varchar, err error) {
    self = &Varchar{
        file: file,
    }
    if err := self.read_ctrlblk(); err != nil {
        return nil, err
    }
    return self, nil
}

func (self *Varchar) Close() error {
    return self.file.Close()
}

func (self *Varchar) write_ctrlblk() error {
    return self.file.SetControlData(self.ctrl.Bytes())
}

func (self *Varchar) read_ctrlblk() error {
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

func (self *Varchar) block_key(key int64) int64 {
    size := int64(self.file.BlockSize())
    return key - (key % size)
}

func (self *Varchar) alloc_new(length uint64) (key int64, blocks []*block, err error) {
    fmt.Println("allocating varchar of", length)
    fmt.Println("self.ctrl.end", self.ctrl.end)
    var start_blk *block
    block_size := datasize(self.file)
    if (FREE_VARCHAR_SIZE + self.ctrl.end) % block_size > block_size {
        // we have to allocate a new block no matter what
        start_blk, err = allocBlock(self.file)
        if err != nil {
            return 0, nil, err
        }
        self.ctrl.end = start_blk.key
    } else {
        start_blk_key := self.block_key(self.ctrl.end)
        start_blk, err = readBlock(self.file, start_blk_key)
        if err != nil {
            return 0, nil, err
        }
    }
    key = self.ctrl.end

    if err := self.set_length(key, start_blk, length); err != nil {
        return 0, nil, err
    }

    true_length := LENSIZE + length
    if uint64(key) + true_length < uint64(key) {
        return 0, nil, fmt.Errorf("Length of varchar overflowed the block pointer")
    }

    blocks = append(blocks, start_blk)
    end := self.ctrl.end
    if (uint64(key) + true_length) <= uint64(key) + uint64(block_size) {
        fmt.Println("no need to alloc")
        // we fit in the currently allocated block !
        end += int64(true_length)
    } else {
        fmt.Println("about to alloc")
        // we need to allocate more blocks and link them together
        allocated := uint64(block_size - (key % block_size))
        start_alloc := allocated
        num_blocks := uint64(1)
        for allocated < true_length {
            blk, err := allocBlock(self.file)
            if err != nil {
                return 0, nil, err
            }
            prev := blocks[len(blocks)-1]
            pm := prev.Metadata()
            pm.next = blk.key
            prev.SetMetadata(pm)

            blocks = append(blocks, blk)
            allocated += uint64(block_size)
            num_blocks += 1
        }
        final_offset := true_length - start_alloc - (num_blocks-2)*uint64(block_size)
        last := blocks[len(blocks)-1]
        end = last.key + int64(final_offset)
    }
    self.ctrl.end = end

    for _, blk := range blocks {
        if err := blk.WriteBlock(self.file); err != nil {
            return 0, nil, err
        }
    }

    fmt.Println()
    return key, blocks, self.write_ctrlblk()
}

func (self *Varchar) alloc(length uint64) (key int64, blocks []*block, err error) {
    if self.ctrl.free_len == 0 {
        return self.alloc_new(length)
    }
    return self.alloc_free(length)
}

func (self *Varchar) alloc_free(length uint64) (key int64, blocks []*block, err error) {
    defer func() {
        if e := recover(); e != nil {
            key = 0
            blocks = nil
            err = e.(error)
        }
    }()

    var dirty []*free_varchar

    write := self.panic_write

    find_split := func(fv *free_varchar, length uint64) (new_free *free_varchar) {
        block_size := datasize(self.file)
        true_length := uint64(LENSIZE + fv.length)
        start_alloc := uint64(block_size - (fv.key % block_size))
        blocks, err := self.blocks(fv.key)
        if err != nil { panic(err) }
        last_block := blocks[len(blocks)-1]
        full_blocks := (uint64(len(blocks))-2)*uint64(block_size)

        if start_alloc < true_length {
            // fits in first block
            return &free_varchar{
                key: fv.key + int64(true_length),
                length: fv.length - true_length,
                next: fv.next,
            }
        } else if start_alloc + full_blocks < true_length {
            // fits in last block
            return &free_varchar{
                key: last_block.key + int64(true_length),
                length: fv.length - true_length,
                next: fv.next,
            }
        } else {
            // is somewhere in the run
            left := true_length - start_alloc
            for _, blk := range blocks[1:] {
                if left + uint64(block_size) < true_length {
                    // found it
                    return &free_varchar{
                        key: blk.key + int64(true_length),
                        length: fv.length - true_length,
                        next: fv.next,
                    }
                }
                left += true_length
            }
        }
        panic(fmt.Errorf("couldn't find free_varchar split"))
    }

    pfv, free, err := self.firstfit(length)
    if err != nil {
        return 0, nil, err
    }
    if free == nil {
        return self.alloc_new(length)
    }

    if free.length == length {
        // If the selected block is the same size as the freeblk remove it from
        // the list.
        self.ctrl.free_len -= 1
        pfv.next = free.next
        dirty = append(dirty, pfv)
    } else if free.length - length < FREE_VARCHAR_SIZE {
        // Removing the amt from the block would result in a undersized free block
        // so remove it from the list and allocate the extra space to the
        // allocated block.
        length = free.length
        self.ctrl.free_len -= 1
        pfv.next = free.next
        dirty = append(dirty, pfv)
    } else {
        // Split the block
        newfree := find_split(free, length) // find free + length
        pfv.next = newfree.key
        dirty = append(dirty, pfv, newfree)
    }

    key = free.key
    if blk, err := readBlock(self.file, self.block_key(key)); err != nil {
        return 0, nil, err
    } else {
        if err := self.set_length(key, blk, length); err != nil {
            return 0, nil, err
        }
    }

    if blocks, err = self.blocks(key); err != nil {
        return 0, nil, err
    }

    // write out the dirty blocks
    for _, dirt := range dirty {
        write(dirt)
    }

    if err := self.write_ctrlblk(); err != nil {
        return 0, nil, err
    }

    return key, blocks, nil
}

func (self *Varchar) firstfit(length uint64) (pfv, cfv *free_varchar, err error) {
    defer func() {
        if e := recover(); e != nil {
            pfv = nil
            cfv = nil
            err = e.(error)
        }
    }()
    load := self.panic_load
    cur := self.ctrl.free_head
    pfv = load(cur)
    for i := 0; i < int(self.ctrl.free_len); i++ {
        cfv := load(cur)
        if cfv.length >= length {
            return pfv, cfv, nil
        }
        cur = cfv.next
        pfv = cfv
    }
    return nil, nil, nil
}

func (self *Varchar) panic_load(key int64) *free_varchar {
    blk, err := readBlock(self.file, self.block_key(key))
    if err != nil { panic(err) }
    fv, err := readFreeVarchar(blk, key)
    if err != nil { panic(err) }
    return fv
}

func (self *Varchar) panic_write(fv *free_varchar) {
    blk, err := readBlock(self.file, self.block_key(fv.key))
    if err != nil { panic(err) }
    fv.writeFreeVarchar(blk)
    err = blk.WriteBlock(self.file)
    if err != nil { panic(err) }
}

func (self *Varchar) insert_free(fv *free_varchar) (err error) {
    var dirty []*free_varchar
    defer func() {
        if e := recover(); e != nil {
            err = e.(error)
        }
    }()

    load := self.panic_load

    write := self.panic_write

    find_end := func(fv *free_varchar) int64 {
        block_size := datasize(self.file)
        true_length := uint64(LENSIZE + fv.length)
        start_alloc := uint64(block_size - (fv.key % block_size))
        blocks, err := self.blocks(fv.key)
        if err != nil { panic(err) }
        final_offset := true_length - start_alloc - (uint64(len(blocks))-2)*uint64(block_size)
        return blocks[len(blocks)-1].key + int64(final_offset)
    }

    combine := func() {
        if self.ctrl.free_len <= 1 {
            return
        }
        key := self.ctrl.free_head
        pfv := load(key)
        cfv := load(pfv.next)

        // Starting at the second block go through the list
        for i := 1; i < int(self.ctrl.free_len); i++ {
            if find_end(pfv) == cfv.key {
                pfv.length += cfv.length
                pfv.next = cfv.next
                self.ctrl.free_len -= 1
                dirty = append(dirty, pfv)
                cfv = load(cfv.next)
                i -= 1 // we essentially "redo" this iteration
            } else {
                pfv = cfv
                cfv = load(pfv.next)
            }
        }
    }

    dirty = append(dirty, fv)
    fv_end := find_end(fv)
    if self.ctrl.free_len == 0 {
        // The list is empty
        self.ctrl.free_head = fv.key
        self.ctrl.free_len = 1
    } else if fv_end < self.ctrl.free_head {
        // first block in the list
        fv.next = self.ctrl.free_head 
        self.ctrl.free_head = fv.key
        self.ctrl.free_len += 1
    } else {
        // Nominal case, this block goes somewhere in the list
        pfv := load(self.ctrl.free_head)
        var cfv *free_varchar
        prev := self.ctrl.free_head
        cur := pfv.next
        var i int


        // Start at the second block, and find the spot where this block goes.
        for i = 1; i < int(self.ctrl.free_len); i++ {
            pfv = load(prev)
            cfv = load(cur)
            if fv_end <= cfv.key {
                // we found the spot
                self.ctrl.free_len += 1
                pfv.next = fv.key
                fv.next = cfv.key
                dirty = append(dirty, pfv, cfv)
                break
            }
            prev = cur
            cur = cfv.key
        }

        // It goes at the end of the list
        if i == int(self.ctrl.free_len) {
            self.ctrl.free_len += 1
            pfv = load(prev)
            pfv.next = fv.key
            fv.next = 0
        }
    }

    combine() // combine adjecent blocks

    // write out the dirty blocks
    for _, dirt := range dirty {
        write(dirt)
    }

    return self.write_ctrlblk()
}

func (self *Varchar) free(key int64) (err error) {
    start_blk_key := self.block_key(key)
    start_blk, err := readBlock(self.file, start_blk_key)
    if err != nil {
        return err
    }
    length := self.length(key, start_blk)
    fv := &free_varchar{key: key, length:length}
    // insert the freed varchar into the list
    // keep the list key order
    return self.insert_free(fv)
}

func (self *Varchar) length(key int64, blk *block) (length uint64) {
    block_size := datasize(self.file)
    offset := key % block_size
    return blk.data[offset:offset+LENSIZE].Int64()
}

func (self *Varchar) set_length(key int64, blk *block, length uint64) (err error) {
    block_size := datasize(self.file)
    if (LENSIZE + key) % block_size > block_size {
        return fmt.Errorf("Would write length off the end of the block")
    }

    offset := key % block_size
    copy(blk.data[offset:offset+LENSIZE], bs.ByteSlice64(length))
    return nil
}

func (self *Varchar) blocks(key int64) (blocks []*block, err error) {
    block_size := datasize(self.file)
    start_blk_key := self.block_key(key)
    start_blk, err := readBlock(self.file, start_blk_key)
    if err != nil {
        return nil, err
    }
    length := self.length(key, start_blk)
    true_length := LENSIZE + length
    left := (uint64(key) + true_length) - uint64(key)
    num_blocks := int64(left / uint64(block_size))
    //overflow := left % uint64(block_size)
    //if overflow > 0 {
    //    num_blocks += 1
    //}
    blocks = append(blocks, start_blk)
    for i := int64(0); i < num_blocks; i++ {
        prev := blocks[len(blocks)-1]
        pm := prev.Metadata()
        blk, err := readBlock(self.file, pm.next)
        if err != nil {
            return nil, err
        }
        blocks = append(blocks, blk)
    }
    return blocks, nil
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

