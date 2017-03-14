.PHONY:	all

LFS = src/skiplist src/linkedlist

.PHONY:	clean all $(LFS)

default: linkedlist skiplist $(LFS)

all:	linkedlist skiplist $(LFS)


linkedlist:
	$(MAKE) -C src/linkedlist

skiplist:
	$(MAKE) -C src/skiplist

clean:
	$(MAKE) -C src/skiplist clean
	$(MAKE) -C src/linkedlist clean
	rm -rf build

