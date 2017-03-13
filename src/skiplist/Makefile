ROOT = ../..

include $(ROOT)/common/Makefile.common

BINS = $(BINDIR)/lf-skiplist
PROF = $(ROOT)/src

.PHONY:	all clean

all:	main

measurements.o:
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/measurements.o $(PROF)/measurements.c

lf-skiplist.o: 
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/lf-skiplist.o lf-skiplist.cpp

test.o: lf-skiplist.h lf-skiplist.o
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/test.o $(TEST_FILE)

main: measurements.o lf-skiplist.o test.o 
	$(CC) $(CFLAGS) $(BUILDIR)/measurements.o $(BUILDIR)/lf-skiplist.o  $(BUILDIR)/test.o -o $(BINS) $(LDFLAGS)

clean:
	-rm -f $(BINS)
