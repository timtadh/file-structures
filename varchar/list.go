package varchar

import (
    "fmt"
)

import (
    bs "file-structures/block/byteslice"
    file "file-structures/block/file2"
)

const RUN_SIZE = 512
const LIST_HEADER_LEN = 52

type list_header struct {
    next          int64
    head          int64
    tail          int64
    insert_point  int64
    block_count   uint32
    list_length   uint32
    run_remaining uint32
    next_block    int64
}

type list_block struct {
    file   file.BlockDevice
    key    int64
    bytes  bs.ByteSlice
    data   bs.ByteSlice
    header *list_header
}

type list_blocks []*list_block

func new_list_block(file file.BlockDevice, key int64, new_list bool) (self *list_block, err error) {
    bytes := make(bs.ByteSlice, file.BlockSize())
    data := bytes[LIST_HEADER_LEN:]
    var header *list_header
    if new_list {
        header = &list_header{
            next:          0,
            head:          key,
            tail:          key,
            insert_point:  key + LIST_HEADER_LEN,
            block_count:   1,
            list_length:   0,
            run_remaining: 0,
            next_block:    0,
        }
    } else {
        header = &list_header{next: 0}
    }
    self = &list_block{
        file:   file,
        key:    key,
        bytes:  bytes,
        data:   data,
        header: header,
    }
    return self, nil
}

func load_list_block(file file.BlockDevice, key int64) (self *list_block, err error) {
    bytes, err := file.ReadBlock(key)
    if err != nil {
        return nil, err
    }
    data := bytes[LIST_HEADER_LEN:]
    header := load_list_header(bytes)
    self = &list_block{
        file:   file,
        key:    key,
        bytes:  bytes,
        data:   data,
        header: header,
    }
    return self, nil
}

func (self *list_block) Bytes() []byte {
    bytes := self.bytes
    hbytes := self.header.Bytes()
    copy(bytes[:LIST_HEADER_LEN], hbytes)
    return bytes
}

func (self *list_block) Write() (err error) {
    return self.file.WriteBlock(self.key, self.Bytes())
}

func (self *list_block) Free() (err error) {
    return self.file.Free(self.key)
}

func (self list_blocks) Write() (err error) {
    for _, blk := range self {
        err = blk.Write()
        if err != nil {
            return err
        }
    }
    return nil
}

func (self list_blocks) Free() (err error) {
    for _, blk := range self {
        err = blk.Free()
        if err != nil {
            return err
        }
    }
    return nil
}

func load_list_header(bytes bs.ByteSlice) *list_header {
    return &list_header{
        next:          int64(bytes[0:8].Int64()),
        head:          int64(bytes[8:16].Int64()),
        tail:          int64(bytes[16:24].Int64()),
        insert_point:  int64(bytes[24:32].Int64()),
        block_count:   bytes[32:36].Int32(),
        list_length:   bytes[36:40].Int32(),
        run_remaining: bytes[40:44].Int32(),
        next_block:    int64(bytes[44:52].Int64()),
    }
}

func (self *list_header) Bytes() []byte {
    bytes := make(bs.ByteSlice, LIST_HEADER_LEN)
    copy(bytes[0:8], bs.ByteSlice64(uint64(self.next)))
    copy(bytes[8:16], bs.ByteSlice64(uint64(self.head)))
    copy(bytes[16:24], bs.ByteSlice64(uint64(self.tail)))
    copy(bytes[24:32], bs.ByteSlice64(uint64(self.insert_point)))
    copy(bytes[32:36], bs.ByteSlice32(self.block_count))
    copy(bytes[36:40], bs.ByteSlice32(self.list_length))
    copy(bytes[40:44], bs.ByteSlice32(self.run_remaining))
    copy(bytes[44:52], bs.ByteSlice64(uint64(self.next_block)))
    return bytes
}

type list_element struct {
    bytes   bs.ByteSlice
    _length bs.ByteSlice
    data    bs.ByteSlice
}

func new_element(data bs.ByteSlice) *list_element {
    bytes := make(bs.ByteSlice, len(data)+4)
    copy(bytes[0:4], bs.ByteSlice32(uint32(len(data))))
    copy(bytes[4:], data)
    return &list_element{
        bytes:   bytes,
        _length: bytes[:4],
        data:    bytes[4:],
    }
}

func load_element(bytes bs.ByteSlice) *list_element {
    return &list_element{
        bytes:   bytes,
        _length: bytes[0:4],
        data:    bytes[4:],
    }
}

func (self *list_element) length() uint32 {
    return self._length.Int32()
}

func (self *list_element) Bytes() []byte {
    return self.bytes
}

type VarcharList struct {
    file file.BlockDevice
}

func MakeVarcharList(file file.BlockDevice) (self *VarcharList) {
    return &VarcharList{
        file,
    }
}

func (self *VarcharList) Close() error {
    return self.file.Close()
}

func (self *VarcharList) New() (key int64, err error) {
    block_key, err := self.file.Allocate()
    if err != nil {
        return 0, err
    }
    blk, err := new_list_block(self.file, block_key, true)
    if err != nil {
        return 0, err
    }
    err = blk.Write()
    if err != nil {
        return 0, err
    }
    return blk.key, nil
}

// hblk will be dirtied by this function.

func (self *VarcharList) alloc_block(hblk *list_block) (block_key int64, err error) {
    defer func() {
        if e := recover(); e != nil {
            block_key = 0
            err = e.(error)
        }
        return
    }()
    alloc_run := func() (run []int64) {
        run = make([]int64, RUN_SIZE)
        pkey := int64(0)
        for i := range run {
            key, err := self.file.Allocate()
            if err != nil { panic(key) }
            if pkey != 0 && pkey + int64(self.file.BlockSize()) != key {
                panic(fmt.Errorf("Expected key to be in run, it was outside of it"))
            }
            run[i] = key
        }
        return run
    }

    if hblk.header.run_remaining > 0 {
        block_key = hblk.header.next_block
        hblk.header.next_block = block_key + int64(self.file.BlockSize())
        hblk.header.run_remaining -= 1
        if hblk.header.run_remaining <= 0 {
            hblk.header.next_block = 0
        }
        return block_key, nil
    } else {
        run := alloc_run()
        hblk.header.next_block = run[0]
        hblk.header.run_remaining = uint32(len(run))
        return self.alloc_block(hblk)
    }
}

// hblk will be dirtied by this function. hblk will be dirtied but not added to the dirt list.
// the new block will be in the dirty list.

func (self *VarcharList) new_list_block(hblk *list_block) (block *list_block, dirty list_blocks, err error) {
    block_key, err := self.alloc_block(hblk)
    if err != nil {
        panic(err)
    }
    blk, err := new_list_block(self.file, block_key, false)
    if err != nil {
        panic(err)
    }
    dirty = append(dirty, blk)
    return blk, dirty, nil
}

// note all allocated blocks will be in the dirty list but not all dirty will be
// in the allocated list. The correct way to use this function is to: 1)
// allocate the blocks you need. 2) write the data into those blocks. 3) write
// all of the dirty blocks by calling `dirty.Write()`. This ensures there will
// be no double writes.

func (self *VarcharList) alloc(list_key int64, amt int64) (item_key int64, hblk *list_block, dirty, allocated list_blocks, err error) {
    defer func() {
        if e := recover(); e != nil {
            item_key = 0
            hblk = nil
            dirty = nil
            allocated = nil
            err = e.(error)
        }
        return
    }()
    hblk, err = load_list_block(self.file, list_key)
    if err != nil {
        return 0, nil, nil, nil, err
    }
    dirty = append(dirty, hblk)

    var tail *list_block
    if hblk.key != hblk.header.tail {
        tail, err = load_list_block(self.file, hblk.header.tail)
        if err != nil {
            return 0, nil, nil, nil, err
        }
        dirty = append(dirty, tail)
    } else {
        tail = hblk
    }

    append_block := func() *list_block {
        blk, dirt, err := self.new_list_block(hblk)
        if err != nil {
            panic(err)
        }
        for _, blk := range dirt {
            dirty = append(dirty, blk)
        }

        tail.header.next = blk.key
        hblk.header.tail = blk.key
        hblk.header.block_count += 1
        hblk.header.insert_point = blk.key + LIST_HEADER_LEN

        tail = blk
        return blk
    }

    calc_start := func() int64 {
        start := hblk.header.insert_point - tail.key
        if start < LIST_HEADER_LEN {
            panic(fmt.Errorf("VarcharList.alloc insert_point is non-sense %v %v", hblk.header.insert_point, start))
        }
        return start
    }

    start := calc_start()
    if start+FREE_VARCHAR_SIZE > int64(self.file.BlockSize()) {
        // we will have to allocate a new block and we will have to start the
        // item at the beginning of the new block
        tail = append_block()
        start = calc_start()
    }
    allocated = append(allocated, tail)
    item_key = start + tail.key

    var end_offset int64
    if start+amt <= int64(self.file.BlockSize()) {
        // we fit in the currenlty allocated block
        end_offset = start + amt
        // fmt.Println("exact alloc", start, amt, end_offset)
    } else {
        // we need to allocate more blocks and link them together
        amt_allocated := int64(self.file.BlockSize()) - start
        start_alloc := amt_allocated
        num_blocks := int64(1)
        // fmt.Println("start_alloc", start_alloc, amt)
        for amt_allocated < amt {
            // fmt.Println("left", amt - amt_allocated)
            blk := append_block()
            allocated = append(allocated, blk)
            amt_allocated += int64(self.file.BlockSize()) - LIST_HEADER_LEN
            num_blocks += 1
        }
        // fmt.Println(blocks)
        end_offset = amt - start_alloc - (num_blocks-2)*int64(self.file.BlockSize()-LIST_HEADER_LEN) + LIST_HEADER_LEN
        if tail.key != hblk.header.tail || tail.key != allocated[len(allocated)-1].key {
            panic(fmt.Errorf("tail is not setup correctly!"))
        }
    }
    hblk.header.insert_point = tail.key + end_offset
    // fmt.Println("VarcharList.alloc end", end_offset + allocated[len(allocated)-1].key, hblk.header.insert_point, end_offset)

    return item_key, hblk, dirty, allocated, nil
}

func (self *VarcharList) block_offset(key int64) int64 {
    size := int64(self.file.BlockSize())
    return key % size
}

func (self *VarcharList) data_offset(key int64) int64 {
    // fmt.Println("VarcharList.data_offset", key, self.block_offset(key), self.block_offset(key - LIST_HEADER_LEN))
    return self.block_offset(key - LIST_HEADER_LEN)
}

func (self *VarcharList) _find_end_algo(blocks list_blocks, item_key int64, length uint32) (end int64) {
    offset := self.block_offset(item_key)
    block_size := int64(len(blocks[0].data))
    start_alloc := uint32(block_size - offset)
    if len(blocks) == 1 {
        end = blocks[0].key + offset + int64(length)
        // fmt.Println( "VarcharList._find_end_algo", item_key, end)
    } else if len(blocks) == 2 {
        end = blocks[1].key + int64(length-start_alloc)
        // fmt.Println( "VarcharList._find_end_algo", item_key, end)
    } else {
        full_blocks := (uint32(len(blocks)) - 2) * uint32(block_size)
        final_offset := length - start_alloc - full_blocks
        end = blocks[len(blocks)-1].key + int64(final_offset)
        // fmt.Println( "VarcharList._find_end_algo", item_key, end, final_offset)
    }
    return
}

func (self *VarcharList) Push(key int64, raw_bytes bs.ByteSlice) (err error) {
    // fmt.Println()
    element := new_element(raw_bytes)
    bytes := element.Bytes()
    item_key, hblk, dirty, blocks, err := self.alloc(key, int64(len(bytes)))
    if err != nil {
        return err
    }
    start_offset := int(self.data_offset(item_key))
    end_offset := int(self.data_offset(self._find_end_algo(blocks, item_key, uint32(len(bytes)))))
    // fmt.Println( "VarcharList.Push", key, len(blocks[0].data), start_offset, end_offset, len(bytes))
    if len(blocks) == 1 {
        // fmt.Println("VarcharList.Push", "only one block just copy")
        copy(blocks[0].data[start_offset:end_offset], bytes)
    } else {
        // fmt.Println("VarcharList.Push", "several blocks")
        var strings []string
        start_bytes_offset := len(blocks[0].data) - start_offset
        s := fmt.Sprint("start ", len(blocks), len(bytes), start_offset, start_bytes_offset)
        strings = append(strings, s)
        copy(blocks[0].data[start_offset:], bytes[0:start_bytes_offset])
        offset := start_bytes_offset
        for i, blk := range blocks[1 : len(blocks)-1] {
            s := fmt.Sprint("middle ", i, len(blocks), len(bytes), offset, offset+len(blk.data))
            strings = append(strings, s)
            copy(blk.data, bytes[offset:offset+len(blk.data)])
            offset += len(blk.data)
        }
        if offset > len(bytes) {
            fmt.Println(len(blocks), len(bytes), offset)
            panic(fmt.Errorf("offset out of bounds on bytes"))
        }
        if end_offset > len(blocks[len(blocks)-1].data) {
            for _, s := range strings {
                fmt.Println(s)
            }
            fmt.Println(len(blocks), len(bytes), len(blocks[len(blocks)-1].data), end_offset)
            panic(fmt.Errorf("offset out of bounds on blocks[len(blocks)-1].data")) // this is the trigger!
        }
        copy(blocks[len(blocks)-1].data[:end_offset], bytes[offset:]) // BUG HERE
    }
    hblk.header.list_length += 1
    err = dirty.Write()
    if err != nil {
        return err
    }
    start_blk := blocks[0]
    offset := item_key - start_blk.key
    ramt := start_blk.bytes[offset : offset+4].Int32()
    ramt2 := start_blk.data[self.data_offset(item_key) : self.data_offset(item_key)+4].Int32()
    // fmt.Println("VarcharList.Push", "cal_offset", offset, self.data_offset(item_key), self.data_offset(item_key) + 40)
    if ramt != element.length() || ramt2 != ramt {
        panic(fmt.Errorf("Written amount incorrect! %v != %v, %v \n %v", element.length(), ramt, bs.ByteSlice(element.Bytes()), blocks[0].data))
    }
    return nil
}

func (self *VarcharList) get_blocks(list_key int64) (blocks list_blocks, err error) {
    hblk, err := load_list_block(self.file, list_key)
    if err != nil {
        return nil, err
    }
    blocks = make(list_blocks, 0, hblk.header.block_count)
    blocks = append(blocks, hblk)
    cur := hblk
    for i := uint32(1); i < hblk.header.block_count; i++ {
        blk, err := load_list_block(self.file, cur.header.next)
        if err != nil {
            return nil, err
        }
        blocks = append(blocks, blk)
        cur = blk
    }
    return blocks, nil
}

func (self *VarcharList) GetList(key int64) (bytes_list []bs.ByteSlice, err error) {
    // fmt.Println()
    blocks, err := self.get_blocks(key)
    if err != nil {
        return nil, err
    }
    header := blocks[0].header
    read := func(offset int64, amt uint32, block *list_block) (left uint32, bytes bs.ByteSlice) {
        inblock := uint32(len(block.data)) - uint32(offset)
        if amt < inblock {
            return 0, block.data[offset : offset+int64(amt)]
        } else {
            return amt - inblock, block.data[offset:]
        }
    }
    read_item := func(offset int64, blocks list_blocks) (item bs.ByteSlice, stop int64, rblocks list_blocks) {
        // fmt.Println("VarcharList.GetList.read_item", blocks[0].data)
        // fmt.Println("VarcharList.GetList.offset", offset)
        length := blocks[0].data[offset : offset+4].Int32()
        // fmt.Println("VarcharList.GetList.read_item", offset, len(blocks), length, bs.ByteSlice32(length))
        bytes := make(bs.ByteSlice, length)
        bytes_offset := 0
        left := length
        offset += 4
        i := 0
        for left > 0 {
            var block_bytes bs.ByteSlice
            left, block_bytes = read(offset, left, blocks[i])
            copy(bytes[bytes_offset:bytes_offset+len(block_bytes)], block_bytes)
            if left == 0 {
                stop = offset + int64(len(block_bytes))
                // This mysterious condition does the same check that is in alloc to ensure it
                // won't place an item on boundry in the item_header (ensuring the item header
                // can always be read from exactly one block). In the case it is true we need
                // to get the increment i so next time we get the next block and put stop at 0
                // so that are new offset is that the beginning of the new block. This would be
                // slightly less confusing if the items were internally linked together...
                if stop+LIST_HEADER_LEN+FREE_VARCHAR_SIZE > int64(self.file.BlockSize()) {
                    i += 1
                    stop = 0
                }
            }
            bytes_offset += len(block_bytes)
            offset = 0
            i += 1
        }
        // fmt.Println("VarcharList.GetList.read_item", i, length)
        return bytes, stop, blocks[i-1:] //wrong
    }
    offset := int64(0)
    bytes_list = make([]bs.ByteSlice, 0, header.list_length)
    // fmt.Println("VarcharList.GetList", blocks[0].bytes)
    for i := uint32(0); i < header.list_length; i++ {
        // fmt.Println()
        var item bs.ByteSlice
        item, offset, blocks = read_item(offset, blocks)
        bytes_list = append(bytes_list, item)
        // fmt.Println("VarcharList.GetList len(bytes_list)", len(bytes_list), len(item))
    }
    return bytes_list, nil
}

func (self *VarcharList) Free(key int64) (err error) {
    blocks, err := self.get_blocks(key)
    if err != nil {
        return err
    }
    return blocks.Free()
}
