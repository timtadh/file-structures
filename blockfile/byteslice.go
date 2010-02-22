package byteslice

import "fmt"

type ByteSlice []byte

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

func (a ByteSlice) Eq(b ByteSlice) bool {
    if len(a) != len(b) {
        return false
    }
    r := true
    for i, _ := range a {
        r = r && (a[i] == b[i])
    }
    return r
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
    fmt.Printf("%v > %v == %v\n", a, b, t)
    return t
}

func (b ByteSlice) String() string {
    if b == nil {
        return "<nil>"
    }
    return fmt.Sprintf("0x%x", b.Int64())
}
