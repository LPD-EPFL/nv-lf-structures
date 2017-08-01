#ifndef _H_LINKEDLIST_LOCK_
#define _H_LINKEDLIST_LOCK_

#include <assert.h>
#include <link-cache.h>
#include <active-page-table.h>
#include <nv_memory.h>
#include <nv_utils.h>
#include <epoch.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdint.h>

#include <atomic_ops.h>
#include "lock_if.h"

#include "common.h"
#include "utils.h"
//#include "measurements.h"

#define DEFAULT_LOCKTYPE	  	2
#define DEFAULT_ALTERNATE		0
#define DEFAULT_EFFECTIVE		1

#define DO_PAD 1
#define CACHE_LINES_PER_NV_NODE 1

//static volatile int stop;
//extern __thread ssmem_allocator_t* alloc;


typedef volatile struct node_l
{
  skey_t key;			/* 8 */
  svalue_t val;			/* 16 */
  volatile struct node_l *next;	/* 24 */
  volatile uint8_t marked;
#if !defined(LL_GLOBAL_LOCK)
  volatile ptlock_t lock;	/* 32 */
#endif
#if defined(DO_PAD)
  uint8_t padding[CACHE_LINE_SIZE - sizeof(skey_t) - sizeof(svalue_t) - sizeof(struct node*) - sizeof(uint8_t) - sizeof(ptlock_t)];
#endif
} node_l_t;

//STATIC_ASSERT(sizeof(node_l_t) == 32, "sizeof(node_l_t) == 32");

typedef ALIGNED(CACHE_LINE_SIZE) struct intset_l 
{
  node_l_t* head;
#if defined(LL_GLOBAL_LOCK)
  /* char padding[56]; */
  volatile ptlock_t* lock;
#endif
} intset_l_t;

#define LOG_STATUS_CLEAN 0
#define LOG_STATUS_PENDING 1
#define LOG_STATUS_COMMITTED 2

typedef struct thread_log_t {
  node_l_t val1;
  node_l_t val2;
  node_l_t* node1; 
  node_l_t* node2; 
  void* addr;
  int status;
} thread_log_t;


extern __thread thread_log_t* my_log;

node_l_t* new_node_l(skey_t key, svalue_t val, EpochThread epoch);
node_l_t* new_node_and_set_next_l(skey_t key, svalue_t val, node_l_t* next, int initializing, EpochThread epoch);
intset_l_t* set_new_l(EpochThread epoch);
void bucket_set_init(intset_l_t* set, ptlock_t* l, EpochThread epoch);
void set_delete_l(intset_l_t* set);
int set_size_l(intset_l_t* set);
void node_delete_l(node_l_t* node);

#endif	/* _H_LINKEDLIST_LOCK_ */
