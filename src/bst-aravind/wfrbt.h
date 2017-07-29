#include <algorithm>
#include <chrono>
#include <iostream>
#include <fstream>
#include <pthread.h>
#include <setjmp.h>
#include <stdint.h>
#include <unistd.h>
#include <vector>

#include <link-cache.h>
#include <active-page-table.h>
#include <nv_memory.h>
#include <nv_utils.h>
#include <epoch.h>
#include "common.h"
#include "random.h"
#include "lf-common.h"

#include "atomic_ops.h"
#include "standard_ao_double_t.h"

#define RECYCLED_VECTOR_RESERVE 5000000
#define DETAILED_STATS 1


#define MARK_BIT 1
#define FLAG_BIT 0

#define NODE_PADDING 1

#define CACHE_LINES_PER_NV_NODE 1


enum{INS,DEL};
enum {UNMARK,MARK};
enum {UNFLAG,FLAG};

typedef uintptr_t Word;

typedef struct node{
    AO_double_t volatile child;
    uint64_t key;
    #ifdef UPDATE_VAL
        uint64_t value;
        #ifdef NODE_PADDING
            BYTE padding[CACHE_LINE_SIZE - sizeof(uint64_t) - sizeof(AO_double_t) - sizeof(uint64_t)];
        #endif
    #else
        #ifdef NODE_PADDING
            BYTE padding[CACHE_LINE_SIZE - sizeof(uint64_t) - sizeof(AO_double_t)];
        #endif
    #endif
} node_t;

typedef struct seekRecord{
  // SeekRecord structure
  uint64_t leafKey;
  node_t * parent;
  AO_t pL;
  bool isLeftL; // is L the left child of P?
  node_t * lum;
  AO_t lumC;
  bool isLeftUM; // is  last unmarked node's child on access path the left child of  the last unmarked node?
} seekRecord_t;

// typedef struct barrier {
//   pthread_cond_t complete;
//   pthread_mutex_t mutex;
//   int count;
//   int crossing;
// } barrier_t;


typedef struct thread_data {
  int id;
  unsigned long numThreads;
  unsigned long numInsert;
  unsigned long numActualDelete;
  unsigned long ops;
  unsigned int seed;
  double search_frac;
  double insert_frac;
  double delete_frac;
  long keyspace1_size;
  node_t* rootOfTree;
  barrier_t *barrier;
  barrier_t *barrier2;
  // std::vector<node_t *> recycledNodes;
  seekRecord_t * sr; // seek record
  seekRecord_t * ssr; // secondary seek record
  linkcache_t* buffer;
  active_page_table_t* page_table;
  EpochThread epoch;

#ifdef DETAILED_STATS    
  double tot_read_time; 
  double tot_fastins_time;
  double tot_slowins_time;
  long tot_fastins_count;
  long tot_slowins_count;
  double tot_fastdel_time;
  double tot_slowdel_time;
  long tot_fastdel_count;
  long tot_slowdel_count;
  long tot_reads;
  long insertSeqNumber;
  long count;
#endif
  
} thread_data_t;


// Forward declaration of window transactions
int perform_one_delete_window_operation(thread_data_t* data, seekRecord_t * R, uint64_t key);

int perform_one_insert_window_operation(thread_data_t* data, seekRecord_t * R, uint64_t newKey);


/* ################################################################### *
 * Macro Definitions
 * ################################################################### */



inline bool SetBit(volatile unsigned long *array, int bit) {

     bool flag; 
     __asm__ __volatile__("lock bts %2,%1; setb %0" : "=q" (flag) : "m" (*array), "r" (bit)); return flag; 
   return flag;
}

bool mark_Node(volatile AO_t * word){
    return (SetBit(word, MARK_BIT));
}

#define atomic_cas_full(addr, old_val, new_val) __sync_bool_compare_and_swap(addr, old_val, new_val);


//-------------------------------------------------------------
#define create_child_word(addr, mark, flag) (((uintptr_t) addr & ~(uintptr_t)0x03) + (mark << 1) + (flag))
#define is_marked(x) ( ((x >> 1) & 1)  == 1 ? true:false)
#define is_flagged(x) ( (x & 1 )  == 1 ? true:false)

#define get_addr_for_reading(x) ((uintptr_t)x & ~(uintptr_t)0x07)
#define get_addr_for_comparing(x) ((uintptr_t)x & ~(uintptr_t)0x03)
#define add_mark_bit(x) (x + 4UL)
#define is_free(x) (((x) & 3) == 0? true:false)

//-------------------------------------------------------------

