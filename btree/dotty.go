package btree

import "fmt"
import "os"
import "file-structures/block/keyblock"
import "container/list"

var header string = "digraph btree {\n"
var footer string = "}\n"

func Dotty(filename string, tree *BTree) {

	file, _ := os.Create(filename)
	fmt.Fprintln(file, header)

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
	var traverse func(*keyblock.KeyBlock) string
	traverse = func(block *keyblock.KeyBlock) string {
		vals := make([]string, block.RecordCount())
		c++
		name := fmt.Sprintf("node%v", c)
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
				edges.PushBack(fmt.Sprintf("%v->%v", name, traverse(nblock)))
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
			edges.PushBack(fmt.Sprintf("%v->%v", name, traverse(nblock)))
		}
		fmt.Fprintf(file, "%v[shape=record, label=\"%v\"]\n", name, label(vals, int(block.MaxRecordCount())))
		return name
	}
	traverse(tree.getblock(tree.info.Root()))
	for e := edges.Front(); e != nil; e = e.Next() {
		if edge, ok := e.Value.(string); ok {
			fmt.Fprintln(file, edge)
		}
	}
	fmt.Fprintln(file, footer)
	file.Close()
}
