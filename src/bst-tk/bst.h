#ifndef _H_BST_TK_
#define _H_BST_TK_

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

#define DO_PAD 1
#define CACHE_LINES_PER_NV_NODE 1

//static volatile int stop;

typedef union tl32
{
  struct
  {
    volatile uint16_t version;
    volatile uint16_t ticket;
  };
  volatile uint32_t to_uint32;
} tl32_t;


typedef union tl
{
  tl32_t lr[2];
  uint64_t to_uint64;
} tl_t;

static inline int
tl_trylock_version(volatile tl_t* tl, volatile tl_t* tl_old, int right)
{
  uint16_t version = tl_old->lr[right].version;
  if (unlikely(version != tl_old->lr[right].ticket))
    {
      return 0;
    }

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 6
  tl32_t tlo = { .version = version, .ticket = version };
  tl32_t tln = { .version = version, .ticket = (version + 1) };
  return CAS_U32(&tl->lr[right].to_uint32, tlo.to_uint32, tln.to_uint32) == tlo.to_uint32;
#else
  tl32_t tlo = { version, version };
  tl32_t tln = { version, (version + 1) };
#endif
  return CAS_U32(&tl->lr[right].to_uint32, tlo.to_uint32, tln.to_uint32) == tlo.to_uint32;
}

#define TLN_REMOVED  0x0000FFFF0000FFFF0000LL

static inline int
tl_trylock_version_both(volatile tl_t* tl, volatile tl_t* tl_old)
{
  uint16_t v0 = tl_old->lr[0].version;
  uint16_t v1 = tl_old->lr[1].version;
  if (unlikely(v0 != tl_old->lr[0].ticket || v1 != tl_old->lr[1].ticket))
    {
      return 0;
    }

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 6
  tl_t tlo = { .to_uint64 = tl_old->to_uint64 };
  return CAS_U64(&tl->to_uint64, tlo.to_uint64, TLN_REMOVED) == tlo.to_uint64;
#else
  /* tl_t tlo; */
  /* tlo.uint64_t = tl_old->to_uint64; */
  uint64_t tlo = *(uint64_t*) tl_old;

  return CAS_U64((uint64_t*) tl, tlo, TLN_REMOVED) == tlo;
#endif

}


static inline void
tl_unlock(volatile tl_t* tl, int right)
{
  /* PREFETCHW(tl); */
#ifdef __tile__
  MEM_BARRIER;
#endif
  COMPILER_NO_REORDER(tl->lr[right].version++);
}

static inline void
tl_revert(volatile tl_t* tl, int right)
{
  /* PREFETCHW(tl); */
  COMPILER_NO_REORDER(tl->lr[right].ticket--);
}


typedef struct node
{
  skey_t key;
  union
  {
    svalue_t val;
    volatile uint64_t leaf;
  };
  volatile struct node* left;
  volatile struct node* right;
  volatile tl_t lock;

  uint8_t padding[CACHE_LINE_SIZE - 40];
} node_t;

typedef ALIGNED(CACHE_LINE_SIZE) struct intset
{
  node_t* head;
} intset_t;

#define LOG_STATUS_CLEAN 0
#define LOG_STATUS_PENDING 1
#define LOG_STATUS_COMMITTED 2

typedef struct thread_log_t {
  node_t val1;
  node_t val2;
  node_t val3;
  node_t* node1; 
  node_t* node2; 
  node_t* node3; 
  void* addr1;
  void* addr2;
  int status;
} thread_log_t;


extern __thread thread_log_t* my_log;

node_t* new_node(skey_t key, svalue_t val, node_t* l, node_t* r, int initializing, EpochThread epoch);
node_t* new_node_no_init(EpochThread epoch);
intset_t* set_new(EpochThread epoch);
void set_delete(intset_t* set);
int set_size(intset_t* set);
void node_delete(node_t* node);

#endif	/* _H_BST_TK_ */
