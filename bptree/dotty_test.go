package bptree

import "fmt"
import "os"
import "log"
import "block/keyblock"
import . "block/byteslice"
import "container/list"

var header string = "digraph btree {\n"
var footer string = "}\n\n"

var subgraph string = "\n    subgraph graph0 {\n        graph[rank=same];\n"

func Dotty(filename string, tree *BpTree) {
    s := ""
    file, _ := os.Open(filename, os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0666)
    s += header

    label := func(vals []string, size int) string {
        s := ""
        for i := 0; i < size; i++ {
            if i < len(vals) {
                s += vals[i]
            }
            if i+1 < size {
                s += "|"
            }
        }
        return s
    }

    c := 0
    edges := list.New()
    names := make(map[uint64] string)
    external := list.New()
    var traverse func(*keyblock.KeyBlock, int) string

    values := func(name string, height int, block *keyblock.KeyBlock, edges *list.List) []string {
        vals := make([]string, block.RecordCount())
        i := 0
        for ; i < int(block.RecordCount()); i++ {
            rec, _, _, ok := block.Get(i)
            if !ok {
                log.Exitf("could not get rec, %v, from block with %v records\n", i, block.RecordCount())
            }
            if p, ok := block.GetPointer(i); ok {
                nblock := tree.getblock(p)
                if nblock == nil {
                    log.Exitf("nil block returned by self.getblock(p)", i, block.RecordCount())
                }
                c++
                edges.PushBack(fmt.Sprintf("    %v->%v", name, traverse(nblock, height - 1)))
            }
            vals[i] = fmt.Sprintf("%v", rec.GetKey().Int32())
        }
        if p, ok := block.GetPointer(i); ok {
            nblock := tree.getblock(p)
            if nblock == nil {
                log.Exitf("nil block returned by self.getblock(p)", i, block.RecordCount())
            }
            c++
            edges.PushBack(fmt.Sprintf("    %v->%v", name, traverse(nblock, height - 1)))
        }
        return vals
    }

    traverse = func(block *keyblock.KeyBlock, height int) string {
        c++
        name := fmt.Sprintf("node%v", c)
        names[block.Position().Int64()] = name
        vals := values(name, height, block, edges)
        if height > 0 {
            s += fmt.Sprintf("    %v[shape=record, label=\"%v\"]\n", name, label(vals, int(block.MaxRecordCount())))
        } else {
            external.PushBack(fmt.Sprintf("        %v[shape=record, label=\"%v\"]",
                                          name, label(vals, int(block.MaxRecordCount()))))
        }
        return name
    }
    traverse(tree.getblock(tree.info.Root()), tree.info.Height()-1)
    var first func(*keyblock.KeyBlock, int) *keyblock.KeyBlock
    first = func(block *keyblock.KeyBlock, height int) *keyblock.KeyBlock {
        if height > 0 {
            p, _ := block.GetPointer(0)
            return first(tree.getblock(p), height-1)
        }
        return block
    }
    block := first(tree.getblock(tree.info.Root()), tree.info.Height()-1)
    p, _ := block.GetExtraPtr()
    for !p.Eq(ByteSlice64(0)) {
        if _,ok := names[block.Position().Int64()]; !ok {
            c++
            name := fmt.Sprintf("node%v", c)
            names[block.Position().Int64()] = name
            vals := values(name, 0, block, edges)
            external.PushBack(fmt.Sprintf("        %v[shape=record, label=\"%v\"]",
                                          name, label(vals, int(block.MaxRecordCount()))))
        }
        edges.PushBack(fmt.Sprintf("    %v->%v", names[block.Position().Int64()], names[p.Int64()]))
        block = tree.getblock(p)
        p, _ = block.GetExtraPtr()
    }
    s += subgraph
    for e := range external.Iter() {
        if node, ok := e.(string); ok {
            s += fmt.Sprintln(node)
        }
    }
    s += "    " + footer
    for e := range edges.Iter() {
        if edge, ok := e.(string); ok {
            s += fmt.Sprintln(edge)
        }
    }
    s += footer
    fmt.Fprint(file, s)
    file.Close()
}
