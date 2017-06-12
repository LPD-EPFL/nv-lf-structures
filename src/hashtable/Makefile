ROOT = ../..

include $(ROOT)/common/Makefile.common

BINS = $(BINDIR)/lf-hashtable
LLREP = lists
PROF = $(ROOT)/src

.PHONY:	all clean

all:	main

measurements.o:
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/measurements.o $(PROF)/measurements.c

linkedlist.o:
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/linkedlist.o $(LLREP)/lf-linkedlist.cpp

hashtable.o: $(LLREP)/lf-linkedlist.h linkedlist.o
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/hashtable.o hashtable.cpp

intset.o: $(LLREP)/lf-linkedlist.h hashtable.o 
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/intset.o intset.cpp

test.o: linkedlist.o intset.o hashtable.o
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/test.o $(TEST_FILE)

main: measurements.o linkedlist.o intset.o hashtable.o test.o 
	$(CC) $(CFLAGS) $(BUILDIR)/measurements.o $(BUILDIR)/linkedlist.o $(BUILDIR)/hashtable.o $(BUILDIR)/intset.o $(BUILDIR)/test.o -o $(BINS) $(LDFLAGS)

clean:
	-rm -f $(BINS)
