

blockfile_mac:
	cd ../block; $(MAKE) keyblock_osx
btree_mac:
	6g -I ../ -I . -o btree.6 btree.go insert.go util.go
build_mac: blockfile_mac btree_mac
	6g -I ../ -I . -o main.6 test.go
mac: build_mac
	6l -o test main.6
	./test

blockfile_linux:
	cd ../block; $(MAKE) keyblock_linux	
btree_linux:
	6g -I ../ -I . -o btree.6 btree.go insert.go util.go
build_linux: blockfile_linux btree_linux
	6g -I ../ -I . -o main.6 test.go
	-rm hello.btree
linux: build_linux
	6l -o test main.6
	./test




.PHONY : clean
clean :
		cd ../block; $(MAKE) clean
		-rm hello.btree test *.6
		ls


fmt:
	gofmt -w *.go
	../../scripts/tabs_to_spaces.sh *.go
