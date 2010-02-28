

# if [ "$GOOS" == "linux" ]; 
# then 
# # else if [ "$GOOS" == "darwin" ]; then ignore="block/file/linuxconst.go"; else ignore=""; fi; 
# fi;

ifeq ($(GOOS), linux)
	ignore="block/file/const_darwin.go"; 
endif
ifeq ($(GOOS), darwin)
	ignore="block/file/const_linux.go"; 
endif

build: clean
	gobuild -a -ignore=$(ignore)

block: build
	./block/test

btree: build
	./btree/test

test:
	gobuild -run -t -ignore=$(ignore)

.PHONY : clean
clean :
		- rm %s
		-rm hello.btree _testmain block/test btree/test *.6 2> /dev/null
		ls

fmt:
	find -name "*.go" | xargs --replace="%s" gofmt -w %s
	find -name "*.go" | xargs --replace="%s" ../scripts/tabs_to_spaces.sh %s
