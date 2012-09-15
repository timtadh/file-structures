package file

import(
    "os"
    "testing"
    "runtime"
    . "file-structures/block/buffers"
)

const testFile = "test.file"
const testBlockSize = uint32(64)

func createFile(open bool, t *testing.T) *BlockFile {
    os.Remove(testFile)
    OPENFLAG = os.O_RDWR | os.O_CREATE
    bf, ok := NewBlockFile(testFile, NewLRU(1000))
    if ok && open {
        ok = ok && bf.Open()
    }
    if ok {
        return bf
    }
    t.Fatalf("Unable to create file \"%s\"", testFile)
    return nil
}

func destroyFile(file *BlockFile) {
    os.Remove(file.Filename())
}

func callerLineNumber() int {
    _, _, line, _ := runtime.Caller(3)
    return line
}

func compare(t *testing.T, file *BlockFile, expectedTypes ... int) {
    actual := file.Summarize()
    expected := buildExpected(([]int(expectedTypes))...)
    if !actual.Equals(expected) {
        t.Logf("Expected %s at line %d, got %s", expected.String(), callerLineNumber(), actual.String())
        t.Fail()
    }
}

func allocateTestBlocks(file *BlockFile, nBlocks int, t *testing.T) {
    var ok bool;
    for i := 0; i < nBlocks; i++ {
        if _, ok = file.Allocate(testBlockSize); !ok {
            t.Fatalf("Unable to allocate at line %d", callerLineNumber())
        }
    }
}

func deallocateTestBlock(file *BlockFile, p int, t *testing.T) {
    if ok := file.Deallocate(PNTR_SIZE64 + int64(p*int(testBlockSize)), testBlockSize); !ok {
        t.Fatalf("Unable to deallocate at line %d", callerLineNumber())
    }
}

func buildExpected(runTypes ... int) *FileSummary {
    expected := NewFileSummary()
    lastType, lastLength := -1, 0
    for _, runType := range runTypes {
        if lastType != runType {
            if lastLength >= 0 {
                expected.Add(lastLength, lastType)
            }
            lastType, lastLength = runType, 0
        }
        switch runType {
        case HEAD, TAIL:
            lastLength += int(PNTR_SIZE64)
        case ALLOC, DEALLOC:
            lastLength += int(testBlockSize)
        }
    }
    if lastLength >= 0 {
        expected.Add(lastLength, lastType)
    }
    return expected
}

func TestEmptyFile(t *testing.T) {
    file := createFile(false, t)
    compare(t, file)
}

func TestNewFile(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    compare(t, file, HEAD, TAIL)
}

func TestFileAllocate(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    allocateTestBlocks(file, 1, t)

    compare(t, file, HEAD, ALLOC, TAIL)

    allocateTestBlocks(file, 1, t)

    compare(t, file, HEAD, ALLOC, ALLOC, TAIL)
}

func TestDeallocateLeftmost(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    allocateTestBlocks(file, 3, t)

    deallocateTestBlock(file, 0, t)

    compare(t, file, HEAD, DEALLOC, ALLOC, ALLOC, TAIL)
}

func TestDeallocateRightmost(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    allocateTestBlocks(file, 3, t)

    deallocateTestBlock(file, 2, t)

    compare(t, file, HEAD, ALLOC, ALLOC, TAIL)
}

func TestDeallocateIsolated(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    allocateTestBlocks(file, 3, t)

    deallocateTestBlock(file, 1, t)

    compare(t, file, HEAD, ALLOC, DEALLOC, ALLOC, TAIL)
}

func TestDeallocateAll(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    allocateTestBlocks(file, 1, t)

    deallocateTestBlock(file, 0, t)

    compare(t, file, HEAD, TAIL)
}

func TestDeallocateAllLtoR(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    allocateTestBlocks(file, 2, t)

    deallocateTestBlock(file, 0, t)
    deallocateTestBlock(file, 1, t)

    compare(t, file, HEAD, TAIL)
}

func TestDeallocateAllRtoL(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    allocateTestBlocks(file, 2, t)

    deallocateTestBlock(file, 1, t)
    deallocateTestBlock(file, 0, t)

    compare(t, file, HEAD, TAIL)
}

func TestDeallocateMergeLeft(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    allocateTestBlocks(file, 5, t)

    deallocateTestBlock(file, 1, t)
    deallocateTestBlock(file, 2, t)

    compare(t, file, HEAD, ALLOC, DEALLOC, DEALLOC, ALLOC, ALLOC, TAIL)
}

func TestDeallocateMergeRight(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    allocateTestBlocks(file, 5, t)

    deallocateTestBlock(file, 3, t)
    deallocateTestBlock(file, 2, t)

    compare(t, file, HEAD, ALLOC, ALLOC, DEALLOC, DEALLOC, ALLOC, TAIL)
}

func TestDeallocateMergeBoth(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    allocateTestBlocks(file, 5, t)

    deallocateTestBlock(file, 1, t)
    deallocateTestBlock(file, 3, t)
    deallocateTestBlock(file, 2, t)

    compare(t, file, HEAD, ALLOC, DEALLOC, DEALLOC, DEALLOC, ALLOC, TAIL)
}

func TestBadDeallocateInput(t *testing.T) {
    file := createFile(true, t)
    defer destroyFile(file)

    allocateTestBlocks(file, 1, t)

    if file.Deallocate(-1, testBlockSize) {
        t.Fatal("File allowed deallocation at p = -1")
    }

    if file.Deallocate(PNTR_SIZE64 - 1, testBlockSize) {
        t.Fatal("File allowed deallocation within previously deallocated block")
    }

    if file.Deallocate(PNTR_SIZE64 + int64(testBlockSize), testBlockSize) {
        t.Fatal("File allowed deallocation at previously deallocated block")
    }

    if file.Deallocate(PNTR_SIZE64 + int64(testBlockSize) - 1, testBlockSize) {
        t.Fatal("File allowed deallocation into previously deallocated block")
    }

    if file.Deallocate(PNTR_SIZE64 + int64(testBlockSize) + PNTR_SIZE64, testBlockSize) {
        t.Fatal("File allowed deallocation beyond file size")
    }

    if file.Deallocate(PNTR_SIZE64, uint32(PNTR_SIZE64 - 1)) {
        t.Fatal("File allowed deallocation of smaller than allowed block size")
    }

    if file.Deallocate(PNTR_SIZE64 + 1, testBlockSize - 1) {
        t.Fatal("File allowed deallocation with too-small gap on left")
    }

    if file.Deallocate(PNTR_SIZE64, testBlockSize - 1) {
        t.Fatal("File allowed deallocation with too-small gap on right")
    }

    allocateTestBlocks(file, 1, t)

    if file.Deallocate(PNTR_SIZE64 + 1, testBlockSize) {
        t.Fatal("File allowed deallocation with too-small gap on left, gap on right")
    }

    if file.Deallocate(PNTR_SIZE64 + int64(testBlockSize - 1), testBlockSize) {
        t.Fatal("File allowed deallocation with too-small gap on right, gap on left")
    }
}
