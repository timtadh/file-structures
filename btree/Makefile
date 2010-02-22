
blockfile_linux:
	cd ../block; $(MAKE) keyblock_linux	
build_linux: blockfile_linux
	6g -I ../ -o main.6 test.go
linux: build_linux
	6l -o test main.6
	./test

.PHONY : clean
clean :
		-rm hello.btree test *.6
		ls

