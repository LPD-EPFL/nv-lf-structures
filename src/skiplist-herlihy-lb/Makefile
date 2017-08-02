ROOT = ../..

include $(ROOT)/common/Makefile.common


ALGO_O = herlihy.o
ALGO_C = herlihy.c

BINS = "$(BINDIR)/lb-sl_herlihy$(BIN_SUFFIX)"
PROF = $(ROOT)/src

.PHONY:	all clean

all:	main


skiplist-lock.o:
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/skiplist-lock.o skiplist-lock.c

$(ALGO_O): skiplist-lock.h 
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/$(ALGO_O) $(ALGO_C)

intset.o: skiplist-lock.h optimistic.h 
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/intset.o intset.c

test.o: skiplist-lock.h optimistic.h intset.h
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/test.o $(TEST_FILE)

main: measurements.o skiplist-lock.o $(ALGO_O) intset.o test.o 
	$(CC) $(CFLAGS) $(BUILDIR)/measurements.o  $(BUILDIR)/skiplist-lock.o $(BUILDIR)/$(ALGO_O) $(BUILDIR)/intset.o $(BUILDIR)/test.o -o $(BINS) $(LDFLAGS)

clean:
	-rm -f $(BINS) *.o

