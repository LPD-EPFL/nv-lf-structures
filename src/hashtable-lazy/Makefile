ROOT = ../..

include $(ROOT)/common/Makefile.common

ifeq ($(RO_FAIL),0)
	CFLAGS += -DRO_FAIL=0
	SUFFIX = _no_ro
else
	CFLAGS += -DRO_FAIL=1
endif

BINS  = $(BINDIR)/lb-ht_lazy$(BIN_SUFFIX)$(SUFFIX)
LLREP = lists
PROF = $(ROOT)/src

.PHONY:	all clean

all:	main

measurements.o:
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/measurements.o $(PROF)/measurements.c


intset.o:
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/intset.o $(LLREP)/intset.cpp

lazy.o: intset.o 
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/lazy.o $(LLREP)/lazy.cpp

linkedlist-lock.o: intset.o lazy.o 
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/linkedlist-lock.o $(LLREP)/linkedlist-lock.cpp

hashtable-lock.o: intset.o lazy.o linkedlist-lock.o
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/hashtable-lock.o hashtable-lock.cpp

test.o: intset.o lazy.o linkedlist-lock.o hashtable-lock.o
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/test.o $(TEST_FILE)

main: measurements.o intset.o lazy.o linkedlist-lock.o hashtable-lock.o test.o 
	$(CC) $(CFLAGS)  $(BUILDIR)/measurements.o  $(BUILDIR)/intset.o $(BUILDIR)/lazy.o $(BUILDIR)/linkedlist-lock.o $(BUILDIR)/hashtable-lock.o $(BUILDIR)/test.o -o $(BINS) $(LDFLAGS)

clean:
	rm -f $(BINS)


