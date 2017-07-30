.PHONY:	all

LFS = src/skiplist src/linkedlist src/bst src/bst-aravind src/hashtable

.PHONY:	clean all $(LFS)

default: linkedlist skiplist bst-aravind hashtable $(LFS)

all:	linkedlist skiplist bst-aravind hashtable $(LFS)


linkedlist:
	$(MAKE) -B -C src/linkedlist

skiplist:
	$(MAKE) -B -C src/skiplist

bst:
	$(MAKE) -B -C src/bst

bst-aravind:
	$(MAKE) -B -C src/bst-aravind

hashtable:
	$(MAKE) -B -C src/hashtable


clean:
	$(MAKE) -B -C src/skiplist clean
	$(MAKE) -B -C src/linkedlist clean
	$(MAKE) -B -C src/bst clean
	$(MAKE) -B -C src/bst-aravind clean
	$(MAKE) -B -C src/hashtable clean
	rm -rf build

