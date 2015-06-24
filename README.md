A B+Tree for Go
===============

By Tim Henderson

**NOTE:** I now have a [new repository](https://github.com/timtadh/fs2) which
covers *much the same functionality* as this repository. Except better. [Check
out fs2 now!](https://github.com/timtadh/fs2)

Long ago, when Go 1 was just a twinkle in Rob Pike's eye, I developed a B+Tree
for Go. It didn't support removal, but it did support duplicate keys "the right
way" (tm). Although I was pretty proud of my B+Tree, I didn't have much use for
it after the project it was developed for ended. I wrote an
[article](http://blog.hackthology.com/lessons-learned-while-implementing-a-btree)
on it and then moved on with my life.

However, today it now works with Go 1. It is as of yet basically undocumented in
how to use it. There *are* python bindings and they work reasonably well. It
still doesn't have removal and I still don't need it too. You can't "go get" it
yet but it does work with go install.

    cd $GOPATH/src
    git clone https://github.com/timtadh/file-structures.git
    ## the rpc interface the python bindings use
    go install file-structures/b+bot
    ## install
    go install file-structures/bptree ## the actual b+tree

You can then install the python RPC bindings with:

    cd file-structures
    python setup.py install

### Note

As of July 1st 2013, there is now also a Linear Virtual Hashing implementation.
It is also underdocumented but it should be fairly tested. Checkout:
`file-structures/linhash`.

