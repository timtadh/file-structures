package bptree

import "fmt"
import "os"
import "file-structures/block/keyblock"
import . "file-structures/block/byteslice"
import "container/list"

var header string = "digraph btree {\n"
var footer string = "}\n\n"

var subgraph string = "\n    subgraph graph0 {\n        graph[rank=same];\n"

func Dotty(filename string, tree *BpTree) {
    s := ""
    file, _ := os.Create(filename)
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
                msg := fmt.Sprintf(
                    "could not get rec, %v, from block with %v records\n", i, block.RecordCount())
                panic(msg)
            }
            if p, ok := block.GetPointer(i); ok {
                nblock := tree.getblock(p)
                if nblock == nil {
                    msg := fmt.Sprint(
                        "nil block returned by self.getblock(p)", i, block.RecordCount())
                    panic(msg)
                }
                c++
                edges.PushBack(fmt.Sprintf("    %v->%v", name, traverse(nblock, height - 1)))
            }
            vals[i] = fmt.Sprintf("%v", rec.GetKey().Int32())
        }
        if p, ok := block.GetPointer(i); ok {
            nblock := tree.getblock(p)
            if nblock == nil {
                msg := fmt.Sprint(
                    "nil block returned by self.getblock(p)", i, block.RecordCount())
                panic(msg)
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
        if _,ok := names[p.Int64()]; !ok {
            c++
            name := fmt.Sprintf("node%v", c)
            names[p.Int64()] = name
            vals := values(name, 0, tree.getblock(p), edges)
            external.PushBack(fmt.Sprintf("        %v[shape=record, label=\"%v\"]",
                                          name, label(vals, int(block.MaxRecordCount()))))
        }
        edges.PushBack(fmt.Sprintf("    %v->%v", names[block.Position().Int64()], names[p.Int64()]))
        block = tree.getblock(p)
        p, _ = block.GetExtraPtr()
    }
    s += subgraph

    for e := external.Front(); e != nil; e = e.Next() {
        if node, ok := e.Value.(string); ok {
            s += fmt.Sprintln(node)
        }
    }
    s += "    " + footer
    
    for e := edges.Front(); e != nil; e = e.Next() {
        if edge, ok := e.Value.(string); ok {
            s += fmt.Sprintln(edge)
        }
    }
    s += footer
    fmt.Fprint(file, s)
    file.Close()
}
