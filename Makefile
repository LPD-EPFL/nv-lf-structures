.PHONY:	all

LFS = src/skiplist src/linkedlist src/bst src/bst-aravind

.PHONY:	clean all $(LFS)

default: linkedlist skiplist bst bst-aravind $(LFS)

all:	linkedlist skiplist bst bst-aravind $(LFS)


linkedlist:
	$(MAKE) -B -C src/linkedlist

skiplist:
	$(MAKE) -B -C src/skiplist

bst:
	$(MAKE) -B -C src/bst

bst-aravind:
	$(MAKE) -B -C src/bst-aravind

clean:
	$(MAKE) -B -C src/skiplist clean
	$(MAKE) -B -C src/linkedlist clean
	$(MAKE) -B -C src/bst clean
	$(MAKE) -B -C src/bst-aravind clean
	rm -rf build

