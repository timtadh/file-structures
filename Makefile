

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

runtest:
	-rm test.btree
	-rm -rf dot png *.dot *.png
	gobuild -run -t -ignore=$(ignore)
	-rm _testmain *.6

test: runtest
	-rm -rf dot png
	mkdir dot
	mkdir png
	-mv *.dot dot/
	-for file in dot/*.dot; do echo $$file | cut -d "/" -f 2 - | xargs --replace="%s" dot -Tpng $$file -o png/%s.png; done

.PHONY : clean
clean :
	-rm -rf dot png *.dot *.png
	-find -name "*.6" | xargs --replace="%s" rm %s
	-find -name "hello.btree" | xargs --replace="%s" rm %s
	-rm hello.btree _testmain block/test btree/test *.6 2> /dev/null
	ls

fmt:
	find -name "*.go" | xargs --replace="%s" gofmt -w %s
	find -name "*.go" | xargs --replace="%s" ../scripts/tabs_to_spaces.sh %s
