package varchar

import (
    bs "file-structures/block/byteslice"
)

const LIST_HEADER_LEN = 20
type list_header struct {
    varchar *Varchar
    head int64
    tail int64
    length uint32
    key int64
}

func new_header(varchar *Varchar) (self *list_header, err error) {
    self = &list_header{varchar:varchar, head:0, tail:0, length:0}
    bytes := self.Bytes()
    key, err := self.varchar.Write(bytes)
    if err != nil {
        return nil, err
    }
    self.key = key
    return self, nil
}

func load_header(varchar *Varchar, key int64) (self *list_header, err error) {
    bytes, err := varchar.Read(key)
    if err != nil {
        return nil, err
    }
    self = &list_header{
        varchar:varchar,
        key:key,
        head:int64(bytes[0:8].Int64()),
        tail:int64(bytes[8:16].Int64()),
        length:bytes[16:20].Int32(),
    }
    return self, nil
}

func (self *list_header) Bytes() []byte {
    bytes := make(bs.ByteSlice, LIST_HEADER_LEN)
    copy(bytes[0:8], bs.ByteSlice64(uint64(self.head)))
    copy(bytes[8:16], bs.ByteSlice64(uint64(self.tail)))
    copy(bytes[16:20], bs.ByteSlice32(self.length))
    return bytes
}

func (self *list_header) write() (err error) {
    return self.varchar.Update(self.key, self.Bytes())
}

type list_element struct {
    varchar *Varchar
    key int64
    bytes bs.ByteSlice
    _next bs.ByteSlice
    data bs.ByteSlice
}

func new_element(varchar *Varchar, data bs.ByteSlice) (self *list_element, err error) {
    bytes := make(bs.ByteSlice, len(data) + 8)
    copy(bytes[0:8], bs.ByteSlice64(0))
    copy(bytes[8:], data)
    self = &list_element{
        varchar:varchar,
        bytes:bytes,
        _next:bytes[0:8],
        data:bytes[8:],
    }
    key, err := self.varchar.Write(bytes)
    if err != nil {
        return nil, err
    }
    self.key = key
    return self, nil
}

func load_element(varchar *Varchar, key int64) (self *list_element, err error) {
    bytes, err := varchar.Read(key)
    if err != nil {
        return nil, err
    }
    self = &list_element{
        varchar:varchar,
        key:key,
        bytes:bytes,
        _next:bytes[0:8],
        data:bytes[8:],
    }
    return self, nil
}

func set_next(varchar *Varchar, key, next int64) (err error) {
    self, err := load_element(varchar, key)
    if err != nil {
        return err
    }
    copy(self._next, bs.ByteSlice64(uint64(next)))
    return self.write()
}

func (self *list_element) next() int64 {
    return int64(self._next.Int64())
}

func (self *list_element) Bytes() []byte {
    return self.bytes
}

func (self *list_element) write() (err error) {
    return self.varchar.Update(self.key, self.Bytes())
}

type VarcharList struct {
    varchar *Varchar
}

func MakeVarcharList(varchar *Varchar) (self *VarcharList) {
    return &VarcharList{
        varchar,
    }
}

func (self *VarcharList) Close() error {
    return self.varchar.Close()
}

func (self *VarcharList) New() (key int64, err error) {
    header, err := new_header(self.varchar)
    if err != nil {
        return 0, err
    }
    return header.key, err
}

func (self *VarcharList) Push(key int64, bytes bs.ByteSlice) (err error) {
    header, err := load_header(self.varchar, key)
    if err != nil {
        return err
    }
    elem, err := new_element(self.varchar, bytes)
    if err != nil {
        return err
    }
    if header.length == 0 {
        header.head = elem.key
    } else {
        if err := set_next(self.varchar, header.tail, elem.key); err != nil {
            return err
        }
    }
    header.tail = elem.key
    header.length += 1
    if err := header.write(); err != nil {
        return err
    }
    return nil
}

func (self *VarcharList) get_list(key int64) (header *list_header, list []*list_element, err error) {
    header, err = load_header(self.varchar, key)
    if err != nil {
        return nil, nil, err
    }
    cur := header.head
    for i := uint32(0); i < header.length; i++ {
        elem, err := load_element(self.varchar, cur)
        if err != nil {
            return nil, nil, err
        }
        list = append(list, elem)
        cur = elem.next()
    }
    return header, list, nil
}

func (self *VarcharList) GetList(key int64) (bytes_list []bs.ByteSlice, err error) {
    _, list, err := self.get_list(key)
    if err != nil {
        return nil, err
    }
    for _, elem := range list {
        bytes_list = append(bytes_list, elem.data)
    }
    return bytes_list, nil
}

func (self *VarcharList) Free(key int64) (err error) {
    header, list, err := self.get_list(key)
    if err != nil {
        return err
    }
    for _, elem := range list {
        err := self.varchar.Remove(elem.key)
        if err != nil {
            return err
        }
    }
    err = self.varchar.Remove(header.key)
    if err != nil {
        return err
    }
    return nil
}

