ROOT = ../..

include $(ROOT)/common/Makefile.common

BINS = $(BINDIR)/lf-linkedlist
PROF = $(ROOT)/src

.PHONY:	all clean

all:	main

measurements.o:
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/measurements.o $(PROF)/measurements.c

lf-linkedlist.o: 
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/lf-linkedlist.o lf-linkedlist.cpp

test.o: lf-linkedlist.h lf-linkedlist.o
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/test.o $(TEST_FILE)

main: measurements.o lf-linkedlist.o test.o 
	$(CC) $(CFLAGS) $(BUILDIR)/measurements.o $(BUILDIR)/lf-linkedlist.o  $(BUILDIR)/test.o -o $(BINS) $(LDFLAGS)

clean:
	-rm -f $(BINS)
