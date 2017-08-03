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

#include "common.h"

#include <atomic_ops.h>
#include "lock_if.h"

#include "common.h"
#include "utils.h"

#define DEFAULT_ELASTICITY		4
#define DEFAULT_ALTERNATE               0
#define DEFAULT_EFFECTIVE 		1

#define ALGO_HERLIHY 1
#define ALGO_PUGH 2


#define DO_PAD 1
#define CACHE_LINES_PER_NV_NODE 2

extern unsigned int global_seed;

extern unsigned int levelmax, size_pad_32;

typedef volatile struct sl_node
{
  skey_t key;
  svalue_t val; 
  uint32_t toplevel;
  volatile uint32_t marked;
  volatile uint32_t fullylinked;
#if !defined(LL_GLOBAL_LOCK) && PTLOCK_SIZE < 64 /* fixing crash */
  volatile uint32_t padding;
#endif
#if !defined(LL_GLOBAL_LOCK)
  ptlock_t lock;
#endif
  volatile struct sl_node* next[1];
} sl_node_t;


typedef ALIGNED(CACHE_LINE_SIZE) struct sl_intset 
{
  sl_node_t *head;
#if defined(LL_GLOBAL_LOCK)
  volatile ptlock_t* lock;
#endif
} sl_intset_t;

int get_rand_level();
int floor_log_2(unsigned int n);

#define MAX_NUM_LEVELS 64

typedef union lognode {
    sl_node_t nd;
    char chars[sizeof(sl_node_t) + sizeof(void*) * MAX_NUM_LEVELS];
} lognode_t;
#define LOG_STATUS_CLEAN 0
#define LOG_STATUS_PENDING 1
#define LOG_STATUS_COMMITTED 2

typedef struct thread_log_t {
  lognode_t vals[MAX_NUM_LEVELS+1];
  sl_node_t* nodes[MAX_NUM_LEVELS+1];
  void* addr;
  int status;
} thread_log_t;

extern __thread thread_log_t* my_log;

/* 
 * Create a new node without setting its next fields. 
 */
sl_node_t* sl_new_simple_node(skey_t key, svalue_t val, int toplevel, int transactional, EpochThread epoch);
/* 
 * Create a new node with its next field. 
 * If next=NULL, then this create a tail node. 
 */
sl_node_t *sl_new_node(skey_t key, svalue_t val, sl_node_t *next, int toplevel, int transactional, EpochThread epoch);
void sl_delete_node(sl_node_t* n, EpochThread epoch);
sl_intset_t* sl_set_new(EpochThread epoch);
void sl_set_delete(sl_intset_t* set);
int sl_set_size(sl_intset_t* cset);
