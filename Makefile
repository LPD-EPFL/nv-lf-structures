.PHONY:	all

LFS = src/skiplist src/linkedlist src/bst

.PHONY:	clean all $(LFS)

default: linkedlist skiplist bst $(LFS)

all:	linkedlist skiplist bst $(LFS)


linkedlist:
	$(MAKE) -C src/linkedlist

skiplist:
	$(MAKE) -C src/skiplist

bst:
	$(MAKE) -C src/bst

clean:
	$(MAKE) -C src/skiplist clean
	$(MAKE) -C src/linkedlist clean
	rm -rf build

