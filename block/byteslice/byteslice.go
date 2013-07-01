package byteslice

import "fmt"

type ByteSlice []byte

func ByteSlice8(i uint8) ByteSlice {
    b := make(ByteSlice, 1)
    b[0] = i
    return b
}

func (b ByteSlice) Int8() uint8 {
    return b[0]
}

func ByteSlice16(i uint16) ByteSlice {
    b := make(ByteSlice, 2)
    s := len(b) - 1
    for j := s; j >= 0; j-- {
        b[j] = uint8(i & 0x00ff)
        i >>= 8
    }
    return b
}

func (b ByteSlice) Int16() uint16 {
    i := uint16(0)
    for j := 0; j < len(b) && j < 2; j++ {
        i |= 0x00ff & uint16(b[j])
        if j+1 < len(b) {
            i <<= 8
        }
    }
    return i
}

func ByteSlice32(i uint32) ByteSlice {
    b := make(ByteSlice, 4)
    s := len(b) - 1
    for j := s; j >= 0; j-- {
        b[j] = uint8(i & 0x00000000000000ff)
        i >>= 8
    }
    return b
}

func (b ByteSlice) Int32() uint32 {
    i := uint32(0)
    for j := 0; j < len(b) && j < 4; j++ {
        i |= 0x00000000000000ff & uint32(b[j])
        if j+1 < len(b) {
            i <<= 8
        }
    }
    return i
}

func ByteSlice64(i uint64) ByteSlice {
    b := make(ByteSlice, 8)
    s := len(b) - 1
    for j := s; j >= 0; j-- {
        b[j] = uint8(i & 0x00000000000000ff)
        i >>= 8
    }
    return b
}

func (b ByteSlice) Int64() uint64 {
    i := uint64(0)
    for j := 0; j < len(b) && j < 8; j++ {
        i |= 0x00000000000000ff & uint64(b[j])
        if j+1 < len(b) {
            i <<= 8
        }
    }
    return i
}

func (bytes ByteSlice) Zero() bool {
    for _, b := range bytes {
        if b != 0 {
            return false
        }
    }
    return true
}

func (a ByteSlice) Eq(b ByteSlice) bool {
    if len(a) != len(b) {
        return false
    }
    r := byte(0)
    for i := range a {
        r = r | (a[i] ^ b[i])
    }
    return r == byte(0)
}

func (a ByteSlice) Lt(b ByteSlice) bool { return b.Gt(a) }

func (a ByteSlice) Gt(b ByteSlice) bool {
    if len(a) < len(b) {
        return false
    }
    if len(a) > len(b) {
        return true
    }
    r := true
    t := false
    for i, _ := range a {
        t = t || r && (a[i] > b[i])
        r = r && (a[i] == b[i])
    }
    //     fmt.Printf("%v > %v == %v\n", a, b, t)
    return t
}

func (self ByteSlice) Copy() ByteSlice {
    bytes := make(ByteSlice, len(self))
    for i,b := range self {
        bytes[i] = b
    }
    return bytes
}

func (self ByteSlice) Inc() ByteSlice {
    bytes := self.Copy()
    inc := true
    for i := len(bytes) - 1; i >= 0; i-- {
        if inc {
            bytes[i] = self[i] + 1
            if bytes[i] != 0 { inc = false }
        } else {
            bytes[i] = self[i]
        }
    }
    return bytes
}

func (self ByteSlice) And(b ByteSlice) (result ByteSlice) {
    if len(self) <= len(b) {
        result = make(ByteSlice, len(self))
    } else {
        result = make(ByteSlice, len(b))
    }
    for i := 0; i < len(result); i++ {
        result[i] = self[i] & b[i]
    }
    return result
}

func (self ByteSlice) Concat(b ByteSlice) ByteSlice {
    bytes := make(ByteSlice, len(self)+len(b))
    copy(bytes, self)
    copy(bytes[len(self):], b)
    return bytes
}

func (self ByteSlice) String() string {
    if self == nil {
        return "<nil>"
    }
    buf := "0x"
    for i := range self {
        byt3 := fmt.Sprintf("%x", self[i])
        if len(byt3) != 2 {
            buf += "0" + byt3
        } else {
            buf += byt3
        }
    }
    return buf
}

