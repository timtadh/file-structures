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
	key      int64
	block    bs.ByteSlice
	data     bs.ByteSlice
	metadata bs.ByteSlice
}

func datasize(file file.BlockDevice) int64 {
	return int64(file.BlockSize()) - METADATASIZE
}

func (self *block) datasize() int64 {
	return int64(len(self.data))
}

func (self *block) blocksize() int64 {
	return int64(len(self.block))
}

func load_block(key int64, bytes bs.ByteSlice) (blk *block) {
	size := len(bytes)
	offset := size - METADATASIZE
	return &block{
		key:      key,
		block:    bytes,
		data:     bytes[:offset],
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
		key:      key,
		block:    bytes,
		data:     bytes[:offset],
		metadata: bytes[offset:],
	}
	// fmt.Println("allocated", key)
	return blk, nil
}

const FREE_VARCHAR_SIZE = 16

type free_varchar struct {
	key    int64
	length uint64
	next   int64
}

func (self *free_varchar) Bytes() []byte {
	bytes := make([]byte, FREE_VARCHAR_SIZE)
	copy(bytes[0:8], bs.ByteSlice64(self.length))
	copy(bytes[8:16], bs.ByteSlice64(uint64(self.next)))
	return bytes
}

func (self *free_varchar) String() string {
	return fmt.Sprintf("<free_varchar key=%d length=%d next=%d>",
		self.key, self.length, self.next)
}

func load_free_varchar(bytes bs.ByteSlice, key int64) (fv *free_varchar, err error) {
	if len(bytes) < FREE_VARCHAR_SIZE {
		return nil, fmt.Errorf("len(bytes) < %d", FREE_VARCHAR_SIZE)
	}
	fv = &free_varchar{
		key:    key,
		length: bytes[0:8].Int64(),
		next:   int64(bytes[8:16].Int64()),
	}
	return fv, nil
}

func (self *free_varchar) writeFreeVarchar(blk *block) {
	offset := self.key % blk.blocksize()
	copy(blk.data[offset:offset+FREE_VARCHAR_SIZE], self.Bytes())
}

func readFreeVarchar(blk *block, key int64) (fv *free_varchar, err error) {
	offset := key % blk.blocksize()
	return load_free_varchar(blk.data[offset:offset+FREE_VARCHAR_SIZE], key)
}

type ctrlblk struct {
	end       int64
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

func (self *Varchar) block_offset(key int64) int64 {
	size := int64(self.file.BlockSize())
	return key % size
}

func (self *Varchar) alloc_new(length uint64) (key int64, blocks []*block, err error) {
	// fmt.Println("allocating varchar of", length)
	// fmt.Println("self.ctrl.end", self.ctrl.end)
	var start_blk *block
	block_size := datasize(self.file)
	if (FREE_VARCHAR_SIZE+self.ctrl.end)-self.block_key(self.ctrl.end) >= block_size {
		// we have to allocate a new block no matter what
		// fmt.Println("alloc new block")
		start_blk, err = allocBlock(self.file)
		if err != nil {
			return 0, nil, err
		}
		self.ctrl.end = start_blk.key
	} else {
		// fmt.Println("append to old block", self.ctrl.end)
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

	if self.length(key, start_blk) != length {
		return 0, nil, fmt.Errorf("alloc_new quick sanity length not set correctly")
	}

	true_length := LENSIZE + length
	if uint64(key)+true_length < uint64(key) {
		return 0, nil, fmt.Errorf("Length of varchar overflowed the block pointer")
	}

	blocks = append(blocks, start_blk)
	end := self.ctrl.end
	if (uint64(key) + true_length) <= uint64(start_blk.key)+uint64(block_size) {
		// fmt.Println("no need to alloc", uint64(key) + true_length)
		// we fit in the currently allocated block !
		end += int64(true_length)
	} else {
		// fmt.Println("about to alloc")
		// we need to allocate more blocks and link them together
		allocated := uint64(block_size - self.block_offset(key))
		start_alloc := allocated
		num_blocks := uint64(1)
		// fmt.Println("start_alloc", start_alloc)
		for allocated < true_length {
			// fmt.Println("left", true_length - allocated)
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
		// fmt.Println(blocks)
		final_offset := true_length - start_alloc - (num_blocks-2)*uint64(block_size)
		last := blocks[len(blocks)-1]
		end = last.key + int64(final_offset)
	}
	self.ctrl.end = end

	for _, blk := range blocks {
		// fmt.Println("write blk", blk.key)
		if err := blk.WriteBlock(self.file); err != nil {
			return 0, nil, err
		}
	}

	if self.length(key, start_blk) != length {
		// fmt.Println(blocks, length, self.length(key, start_blk), key, start_blk.key, blocks[0].key)
		return 0, nil, fmt.Errorf("alloc_new length not set correctly")
	}

	// fmt.Println("final key", key)
	// fmt.Println("final blocks", blocks)
	// fmt.Println()
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
		true_length := uint64(LENSIZE + length)
		start_alloc := uint64(block_size - self.block_offset(fv.key))
		blocks, err := self.blocks(fv.key)
		if err != nil {
			panic(err)
		}
		last_block := blocks[len(blocks)-1]
		full_blocks := (uint64(len(blocks)) - 2) * uint64(block_size)
		if len(blocks) < 2 {
			full_blocks = 0
		}

		// fmt.Println("start_alloc", start_alloc, "true_length", true_length)
		if true_length < start_alloc {
			// fits in first block
			// fmt.Println("split if")
			return &free_varchar{
				key:    fv.key + int64(true_length),
				length: fv.length - true_length,
				next:   fv.next,
			}
		} else if start_alloc+full_blocks < true_length {
			// fits in last block
			// fmt.Println("full_blocks", full_blocks)
			offset := int64(true_length - start_alloc - full_blocks)
			// fmt.Println("split else if")
			return &free_varchar{
				key:    last_block.key + offset,
				length: fv.length - true_length,
				next:   fv.next,
			}
		} else {
			// is somewhere in the run
			// fmt.Println("split else")
			alloc := start_alloc
			for _, blk := range blocks[1:] {
				// fmt.Println("alloc",alloc)
				if true_length < alloc+uint64(block_size) {
					offset := int64(true_length - alloc)
					// found it
					return &free_varchar{
						key:    blk.key + int64(offset),
						length: fv.length - true_length,
						next:   fv.next,
					}
				}
				alloc += uint64(block_size)
			}
		}
		panic(fmt.Errorf("couldn't find free_varchar split"))
	}

	write_head, pfv, free, err := self.firstfit(length)
	if err != nil {
		return 0, nil, err
	}
	if free == nil {
		return self.alloc_new(length)
	}

	var nextkey int64
	if free.length == length {
		// If the selected block is the same size as the freeblk remove it from
		// the list.
		self.ctrl.free_len -= 1
		nextkey = free.next
		dirty = append(dirty, pfv)
	} else if free.length-length < FREE_VARCHAR_SIZE {
		// Removing the amt from the block would result in a undersized free block
		// so remove it from the list and allocate the extra space to the
		// allocated block.
		length = free.length
		self.ctrl.free_len -= 1
		nextkey = free.next
		dirty = append(dirty, pfv)
	} else {
		// Split the block
		start_length := free.length
		newfree := find_split(free, length) // find free + length
		if start_length < newfree.length {
			panic(fmt.Errorf("split failed"))
		}
		nextkey = newfree.key
		dirty = append(dirty, pfv, newfree)
	}
	if write_head {
		self.ctrl.free_head = nextkey
	} else {
		pfv.next = nextkey
	}

	key = free.key

	// write out the dirty blocks
	for _, dirt := range dirty {
		write(dirt)
	}

	if err := self.write_ctrlblk(); err != nil {
		return 0, nil, err
	}

	if blk, err := readBlock(self.file, self.block_key(key)); err != nil {
		return 0, nil, err
	} else {
		if err := self.set_length(key, blk, length); err != nil {
			return 0, nil, err
		}
		if err := blk.WriteBlock(self.file); err != nil {
			return 0, nil, err
		}
	}

	if blocks, err = self.blocks(key); err != nil {
		return 0, nil, err
	}

	if self.length(key, blocks[0]) != length {
		return 0, nil, fmt.Errorf("alloc_free length not set correctly")
	}

	// fmt.Println("final key", key)
	// fmt.Println("final blocks", blocks)
	// fmt.Println("length", self.length(key, blocks[0]))

	return key, blocks, nil
}

func (self *Varchar) firstfit(length uint64) (write_head bool, pfv, cfv *free_varchar, err error) {
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
	write_head = true
	for i := 0; i < int(self.ctrl.free_len); i++ {
		cfv := load(cur)
		if cfv.length >= length {
			return write_head, pfv, cfv, nil
		}
		cur = cfv.next
		pfv = cfv
		write_head = false
	}
	return false, nil, nil, nil
}

func (self *Varchar) print_free() {
	load := self.panic_load
	ptr := self.ctrl.free_head

	fmt.Println("FREE LIST")
	fmt.Println("free_len", self.ctrl.free_len)
	for i := uint32(0); i < self.ctrl.free_len; i++ {
		cur := load(ptr)
		fmt.Println(cur, self.panic_find_end(cur))
		ptr = cur.next
	}
	fmt.Println()
}

func (self *Varchar) panic_load(key int64) *free_varchar {
	if key == 0 {
		panic(fmt.Errorf("key == 0 in panic load"))
	}
	blk, err := readBlock(self.file, self.block_key(key))
	if err != nil {
		panic(err)
	}
	fv, err := readFreeVarchar(blk, key)
	if err != nil {
		panic(err)
	}
	return fv
}

func (self *Varchar) panic_write(fv *free_varchar) {
	blk, err := readBlock(self.file, self.block_key(fv.key))
	if err != nil {
		panic(err)
	}
	fv.writeFreeVarchar(blk)
	err = blk.WriteBlock(self.file)
	if err != nil {
		panic(err)
	}
}

func (self *Varchar) _find_end_algo(blocks []*block, key int64, length uint64) (end int64) {
	block_size := datasize(self.file)
	true_length := uint64(LENSIZE + length)
	start_alloc := uint64(block_size - self.block_offset(key))
	if len(blocks) == 1 {
		end = key + int64(true_length)
	} else if len(blocks) == 2 {
		end = blocks[1].key + int64(true_length-start_alloc)
	} else {
		full_blocks := (uint64(len(blocks)) - 2) * uint64(block_size)
		final_offset := true_length - start_alloc - full_blocks
		end = blocks[len(blocks)-1].key + int64(final_offset)
	}
	return
}

func (self *Varchar) panic_find_end(fv *free_varchar) (end int64) {
	// fmt.Println("start find end", fv)
	blocks, err := self.blocks(fv.key)
	if err != nil {
		panic(err)
	}
	return self._find_end_algo(blocks, fv.key, fv.length)
}

func (self *Varchar) insert_free(fv *free_varchar) (err error) {
	// fmt.Println("call insert_free", fv.key)
	var dirty []*free_varchar
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()

	load := self.panic_load
	write := self.panic_write
	find_end := self.panic_find_end

	combine := func() {
		// fmt.Println("***********start combine******")
		if self.ctrl.free_len < 1 {
			return
		}
		if self.ctrl.free_head == 0 {
			return
		}
		key := self.ctrl.free_head
		pfv := load(key)
		ptr := pfv.next

		// self.print_free()

		// Starting at the second block go through the list
		for i := 1; i < int(self.ctrl.free_len); i++ {
			// fmt.Println("ptr", ptr)
			cfv := load(ptr)
			// fmt.Println("cfv", cfv)
			if find_end(pfv) == cfv.key {
				// fmt.Println("found combine match", pfv, cfv)
				pfv.length += cfv.length + LENSIZE
				pfv.next = cfv.next
				self.ctrl.free_len -= 1
				write(pfv)
				ptr = cfv.next
				i -= 1 // we essentially "redo" this iteration
			} else {
				pfv = cfv
				ptr = pfv.next
			}
		}

		// self.print_free()
		// fmt.Println("********end combine*******")
	}

	// self.print_free()
	// fmt.Println("about to insert")
	// fmt.Println("free_len", self.ctrl.free_len, "free_head",
	//     self.ctrl.free_head, "item", fv)

	dirty = append(dirty, fv)
	fv_end := find_end(fv)
	if self.ctrl.free_len == 0 {
		// The list is empty
		self.ctrl.free_head = fv.key
		self.ctrl.free_len = 1
	} else if fv_end <= self.ctrl.free_head {
		// first block in the list
		fv.next = self.ctrl.free_head
		self.ctrl.free_head = fv.key
		self.ctrl.free_len += 1
	} else {
		// Nominal case, this block goes somewhere in the list
		if self.ctrl.free_head == 0 {
			panic(fmt.Errorf("free_head == 0 unexpectedly"))
		}
		pfv := load(self.ctrl.free_head)
		var cfv *free_varchar
		prev := self.ctrl.free_head
		cur := pfv.next
		var i int
		// fmt.Printf("pfv=%v, fv=%v, cfv=%v\n", pfv, fv, cfv)

		if prev == 0 {
			panic(fmt.Errorf("prev == 0 unexpectedly at loop start"))
		}
		// if cur == 0 { panic(fmt.Errorf( "cur == 0 unexpectedly at loop start, len == %d", self.ctrl.free_len)) }
		// Start at the second block, and find the spot where this block goes.
		// fmt.Println("start", i, self.ctrl.free_len)
		for i = 1; i < int(self.ctrl.free_len); i++ {
			// fmt.Println("loop", i, self.ctrl.free_len)
			if i == 1 && self.ctrl.free_len == 1 {
				panic(fmt.Errorf("entered loop unexpectedly"))
			}
			if prev == 0 {
				panic(fmt.Errorf("prev == 0 unexpectedly"))
			}
			if cur == 0 {
				panic(fmt.Errorf("cur == 0 unexpectedly"))
			}
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
			cur = cfv.next
			// fmt.Println("next prev", prev, "cur", cur)
		}

		// It goes at the end of the list
		if i == int(self.ctrl.free_len) {
			// fmt.Println("insert at end", prev)
			if prev == 0 {
				panic(fmt.Errorf("prev == 0 unexpectedly at loop start"))
			}
			self.ctrl.free_len += 1
			pfv = load(prev)
			// fmt.Println(pfv)
			pfv.next = fv.key
			fv.next = 0
			dirty = append(dirty, pfv)
		}
	}

	// write out the dirty blocks
	for _, dirt := range dirty {
		write(dirt)
	}

	if self.ctrl.free_len > 1 {
		combine() // combine adjecent blocks
		// fmt.Println(combine)
	}

	// self.print_free()

	// fmt.Println()
	// fmt.Println()
	// fmt.Println()
	// fmt.Println()
	return self.write_ctrlblk()
}

func (self *Varchar) free(key int64) (err error) {
	start_blk_key := self.block_key(key)
	start_blk, err := readBlock(self.file, start_blk_key)
	if err != nil {
		return err
	}
	length := self.length(key, start_blk)
	fv := &free_varchar{key: key, length: length}
	// insert the freed varchar into the list
	// keep the list key order
	return self.insert_free(fv)
}

func (self *Varchar) length(key int64, blk *block) (length uint64) {
	offset := self.block_offset(key)
	return blk.data[offset : offset+LENSIZE].Int64()
}

func (self *Varchar) set_length(key int64, blk *block, length uint64) (err error) {
	block_size := datasize(self.file)
	offset := self.block_offset(key)
	if offset > block_size {
		return fmt.Errorf("Would write length off the end of the block")
	}

	copy(blk.data[offset:offset+LENSIZE], bs.ByteSlice64(length))
	return nil
}

func (self *Varchar) blocks(key int64) (blocks []*block, err error) {
	// fmt.Println("start blocks", key)
	block_size := datasize(self.file)
	start_blk_key := self.block_key(key)
	start_blk, err := readBlock(self.file, start_blk_key)
	if err != nil {
		return nil, err
	}
	offset := self.block_offset(key)
	length := self.length(key, start_blk)
	// fmt.Println("length", length)
	true_length := LENSIZE + length
	// fmt.Println("true_length", true_length)
	space_in_first_blk := uint64(block_size - offset)
	left := true_length
	if space_in_first_blk > left {
		left = 0
	} else {
		left = left - space_in_first_blk
	}
	num_blocks := int64(left / uint64(block_size))
	if left%uint64(block_size) > 0 {
		num_blocks += 1
	}
	blocks = append(blocks, start_blk)
	for i := int64(0); i < num_blocks; i++ {
		prev := blocks[len(blocks)-1]
		pm := prev.Metadata()
		blk, err := readBlock(self.file, pm.next)
		// fmt.Println("got err", err)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, blk)
	}
	// fmt.Println("end blocks")
	return blocks, nil
}

func (self *Varchar) write(blocks []*block, key int64, bytes bs.ByteSlice) (err error) {
	length := uint64(len(bytes))
	start_offset := int(self.block_offset(key)) + LENSIZE
	end_offset := int(self.block_offset(self._find_end_algo(blocks, key, length)))
	if len(blocks) == 1 {
		// fmt.Println(
		// "Varchar.write", self.block_key(key), len(blocks[0].data), start_offset, end_offset,
		// len(bytes))
		copy(blocks[0].data[start_offset:end_offset], bytes)
	} else {
		start_bytes_offset := len(blocks[0].data) - start_offset
		copy(blocks[0].data[start_offset:], bytes[0:start_bytes_offset])
		offset := start_bytes_offset
		for _, blk := range blocks[1 : len(blocks)-1] {
			// fmt.Println(i, len(blocks), len(bytes), offset, offset + len(blk.data))
			copy(blk.data, bytes[offset:offset+len(blk.data)])
			offset += len(blk.data)
		}
		copy(blocks[len(blocks)-1].data[:end_offset], bytes[offset:])
	}
	for _, blk := range blocks {
		if err := blk.WriteBlock(self.file); err != nil {
			return err
		}
	}
	return nil
}

func (self *Varchar) Write(bytes bs.ByteSlice) (key int64, err error) {
	length := uint64(len(bytes))
	key, blocks, err := self.alloc(length)
	if err != nil {
		return 0, err
	}
	if err := self.write(blocks, key, bytes); err != nil {
		return 0, err
	}
	// fmt.Println("start_offset", start_offset, "end_offset", end_offset)
	return key, nil
}

func (self *Varchar) Update(key int64, bytes bs.ByteSlice) (err error) {
	blocks, err := self.blocks(key)
	if err != nil {
		return err
	}
	length := self.length(key, blocks[0])
	if uint64(len(bytes)) != length {
		return fmt.Errorf("len(bytes) != %d", length)
	}
	if err := self.write(blocks, key, bytes); err != nil {
		return err
	}
	return nil
}

func (self *Varchar) Read(key int64) (bytes bs.ByteSlice, err error) {
	blocks, err := self.blocks(key)
	if err != nil {
		return nil, err
	}
	length := self.length(key, blocks[0])
	bytes = make(bs.ByteSlice, length)
	start_offset := int(self.block_offset(key)) + LENSIZE
	end_offset := int(self.block_offset(self._find_end_algo(blocks, key, length)))
	if len(blocks) == 1 {
		copy(bytes, blocks[0].data[start_offset:end_offset])
	} else {
		start_bytes_offset := len(blocks[0].data) - start_offset
		copy(bytes[0:start_bytes_offset], blocks[0].data[start_offset:])
		offset := start_bytes_offset
		for _, blk := range blocks[1 : len(blocks)-1] {
			// fmt.Println(i, len(blocks), len(bytes), offset, offset + len(blk.data))
			copy(bytes[offset:offset+len(blk.data)], blk.data)
			offset += len(blk.data)
		}
		copy(bytes[offset:], blocks[len(blocks)-1].data[:end_offset])
	}
	return bytes, nil
}

func (self *Varchar) Remove(key int64) (err error) {
	return self.free(key)
}
