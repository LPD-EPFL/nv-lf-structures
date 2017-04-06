ROOT = ../..

include $(ROOT)/common/Makefile.common

BINS = $(BINDIR)/wfrbt
PROF = $(ROOT)/src

.PHONY:    all clean

all: wfrbt.o wfrbt


VERSION = TCMALLOC_NO_UPDATE

CPPC = g++ -std=gnu++0x

ifeq "$(VERSION)" "MALLOC_UPDATE"
CFLAGS += -lpthread -lrt -DUPDATEVAL
else
ifeq "$(VERSION)" "TCMALLOC_UPDATE"
CFLAGS += -lpthread -lrt /usr/lib64/libtcmalloc.so.4 -DUPDATEVAL
else
ifeq "$(VERSION)" "JEMALLOC_NO_UPDATE"
CFLAGS += -pthread -lrt /home/naravind/research/jemalloc-x64/lib/libjemalloc.so.1
else
CFLAGS += -pthread -lrt
endif
endif
endif

CFLAGS += -I/home/zablotch/local/include/ -I/home/zablotch/local/include/atomic_ops -I/home/zablotch/local/include/atomic_ops/sysdeps

measurements.o:
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/measurements.o $(PROF)/measurements.c

wfrbt.o: measurements.o wfrbt.h wfrbt.cpp
	$(CPPC) $(CFLAGS) -c -o $(BUILDIR)/wfrbt.o wfrbt.cpp

wfrbt: measurements.o wfrbt.o
	$(CPPC) $(CFLAGS) $(BUILDIR)/measurements.o $(BUILDIR)/wfrbt.o -o $(BINS) $(LDFLAGS)


clean:
	rm -f $(BINS)
	rm -f $(BUILDIR)/wfrbt.o
