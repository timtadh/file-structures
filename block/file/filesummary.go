package file

import(
    "bytes"
    "fmt"
)

const(
    HEAD = iota
    TAIL
    ALLOC
    DEALLOC
)

// Must be >= 1
const initialCapacity = 1

type FileSummary struct {
    fileSize int
    runs int
    runTypes []int
    runLengths []int
}

func NewFileSummary() *FileSummary {
    fs := new(FileSummary)
    fs.fileSize   = 0
    fs.runs       = 0
    fs.runTypes   = make([]int, initialCapacity)
    fs.runLengths = make([]int, initialCapacity)
    return fs
}

func (self *FileSummary) Add(runLength int, runType int) {
    if runLength == 0 { return }
    if self.runs >= len(self.runLengths) { self.resizeRuns() }
    self.runLengths[self.runs] = runLength
    self.runTypes[self.runs] = runType
    self.runs++
    self.fileSize += runLength
}

func (self *FileSummary) resizeRuns() {
    newSize := 2*len(self.runLengths)
    newLengths, newTypes := make([]int, newSize), make([]int, newSize)
    for i := 0; i < self.runs; i++ {
        newLengths[i], newTypes[i] = self.runLengths[i], self.runTypes[i]
    }
    self.runLengths, self.runTypes = newLengths, newTypes
}

func (self *FileSummary) Size() int {
    return self.fileSize
}

func (self *FileSummary) String() string {
    if self.fileSize == 0 { return "<0>" }

    buffer := bytes.NewBufferString("")
    str := "<%d:"
    fmt.Fprintf(buffer, str, self.fileSize)
    for i := 0; i < self.runs; i++ {
        switch self.runTypes[i] {
        case HEAD:
            str = "%d|"
        case TAIL:
            str = "|%d"
        case ALLOC:
            str = "%d"
        case DEALLOC:
            str = "(%d)"
        default:
            str = "?%d?"
        }
        fmt.Fprintf(buffer, str, self.runLengths[i])
    }
    fmt.Fprint(buffer, ">")
    return string(buffer.Bytes())
}

func (self *FileSummary) Equals(other *FileSummary) (isEqual bool) {
    isEqual = false
    if self.fileSize != other.fileSize ||
       self.runs != other.runs {
        return
    }
    for i := 0; i < self.runs; i++ {
        if self.runLengths[i] != other.runLengths[i] ||
           self.runTypes[i] != other.runTypes[i] {
            return
        }
    }
    return true
}
