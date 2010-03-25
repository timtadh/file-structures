
ifeq ($(GOOS), linux)
	ignore="block/file/const_darwin.go";
endif
ifeq ($(GOOS), darwin)
	ignore="block/file/const_linux.go";
endif

build:
	gobuild -a -ignore=$(ignore)

block: build
	-rm hello.btree
	./block/test

btree: build
	-rm hello.btree
	./btree/test

test:
	-rm -rf dot png *.dot *.png
	-rm test.btree test.bptree
	gobuild -run -t -match="TestDuplicate" -ignore=$(ignore)
	-rm _testmain *.6
	-rm -rf dot png
	mkdir dot
	-mv *.dot dot/

pic:
	-mkdir dot
	-mv *.dot dot/
	-rm -rf png
	mkdir png
	-for file in dot/*.dot; do echo $$file | cut -d "/" -f 2 - | xargs -I"%s" dot -Tpng $$file -o png/%s.png; done

pictest: test pic
	echo "pictest"

.PHONY : clean
clean :
	-rm -rf dot png *.dot *.png
	-find . -name "*.6" | xargs -I"%s" rm %s
	-find . -name "hello.btree" | xargs -I"%s" rm %s
	-rm hello.btree _testmain block/test btree/test *.6 2> /dev/null
	ls

fmt:
	find . -name "*.go" | xargs -I"%s" gofmt -w %s
	find . -name "*.go" | xargs -I"%s" ../scripts/tabs_to_spaces.sh %s
