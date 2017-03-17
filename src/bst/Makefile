ROOT = ../..

include $(ROOT)/common/Makefile.common

BINS = $(BINDIR)/lf-bst
PROF = $(ROOT)/src

.PHONY:	all clean

all:	main

measurements.o:
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/measurements.o $(PROF)/measurements.c

lf-bst.o: 
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/lf-bst.o lf-bst.cpp

test.o: lf-bst.h lf-bst.o
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/test.o $(TEST_FILE)

main: measurements.o lf-bst.o test.o 
	$(CC) $(CFLAGS) $(BUILDIR)/measurements.o $(BUILDIR)/lf-bst.o  $(BUILDIR)/test.o -o $(BINS) $(LDFLAGS)

clean:
	-rm -f $(BINS)