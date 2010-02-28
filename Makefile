

build_linux:
	gobuild -ignore="block/file/osxconst.go" -a
build_mac:
	gobuild -ignore="block/file/linuxconst.go" -a


.PHONY : clean
clean :
		-find -name "*.6" | xargs --replace="%s" rm %s
		-rm hello.btree _testmain block/test btree/test 2> /dev/null
		ls
