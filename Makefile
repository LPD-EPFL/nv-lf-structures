.PHONY:	all

LFS = src/skiplist src/linkedlist src/bst src/bst-aravind src/hashtable
LBS = src/linkedlist-lazy src/hashtable-lazy

.PHONY:	clean all $(LFS)

default: linkedlist skiplist bst-aravind hashtable linkedlist-lazy hashtable-lazy $(LFS) $(LBS)

all:	linkedlist skiplist bst-aravind hashtable linkedlist-lazy hashtable-lazy bst-tk skiplist-herlihy-lb $(LFS) $(LBS)


linkedlist:
	$(MAKE) -B -C src/linkedlist

linkedlist-lazy:
	$(MAKE) -B -C src/linkedlist-lazy

hashtable-lazy:
	$(MAKE) "G=GL" -B -C src/hashtable-lazy

skiplist:
	$(MAKE) -B -C src/skiplist

skiplist-herlihy-lb:
	$(MAKE) -B -C src/skiplist-herlihy-lb

bst:
	$(MAKE) -B -C src/bst

bst-tk:
	$(MAKE) -B -C src/bst-tk

bst-aravind:
	$(MAKE) -B -C src/bst-aravind

hashtable:
	$(MAKE) -B -C src/hashtable


clean:
	$(MAKE) -B -C src/skiplist clean
	$(MAKE) -B -C src/linkedlist clean
	$(MAKE) -B -C src/linkedlist-lazy clean
	$(MAKE) -B -C src/hashtable-lazy clean
	$(MAKE) -B -C src/bst clean
	$(MAKE) -B -C src/bst-aravind clean
	$(MAKE) -B -C src/hashtable clean
	rm -rf build

