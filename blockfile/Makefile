
fmt:
	gofmt -w *.go
	../scripts/tabs_to_spaces.sh *.go
heap:
	6g -I . -o heap.6 heap.go
byteslice:
	6g -I . -o byteslice.6 byteslice.go
buffers: byteslice heap
	6g -I . -o buffers.6 buffers.go
blockfile_osx: buffers
	6g -I . -o blockfile.6 file.go osxconst.go
blockfile_linux: buffers
	6g -I . -o blockfile.6 file.go linuxconst.go
keyblock_osx: blockfile_osx byteslice
	6g -I . -o keyblock.6 block.go record.go blockdim.go
keyblock_linux: blockfile_linux byteslice
	6g -I . -o keyblock.6 block.go record.go blockdim.go
main_osx: keyblock_osx blockfile_osx
	6g -I . -o main.6 test.go
main_linux: keyblock_linux blockfile_linux
	6g -I . -o main.6 test.go
build_test_osx: main_osx
	6l -o test main.6
build_test_linux: main_linux
	6l -o test main.6
mac: build_test_osx
	-rm hello.btree
	./test
linux: build_test_linux
	-rm hello.btree
	./test

.PHONY : clean
clean :
		-rm hello.btree test *.6
		ls
