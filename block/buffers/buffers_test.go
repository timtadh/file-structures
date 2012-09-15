package buffers

import(
    "testing"
)

const bufferTestSize = 1000
const testBlock = "TESTBLOCK"

func buffersToTest() (buffers []Buffer, names []string) {
    buffers = make([]Buffer, 3)
    names = make([]string, 3)
    buffers[0], names[0] = new(NoBuffer), "NoBuffer"
    buffers[1], names[1] = NewLRU(bufferTestSize), "LRU"
    buffers[2], names[2] = NewLFU(bufferTestSize), "LFU"
    return
}

func TestBufferRemove(t *testing.T) {
    buffers, names := buffersToTest()
    for i, buf := range buffers {
        testBufferRemove(t, buf, names[i])
    }
}

func testBufferRemove(t *testing.T, toTest Buffer, name string) {
    toTest.Update(0, []byte(testBlock))
    toTest.Remove(0)
    if data, ok := toTest.Read(0, uint32(len(testBlock))); ok {
        t.Logf("Was able to read \"%s\" from %s after removing.", data, name)
        t.Fail()
    }
}

