#ifndef _LF_LINKEDLIST_H_
#define _LF_LINKEDLIST_H_
/*
 *  lock-free linked list algorithm
 */

#include <link-cache.h>
#include <active-page-table.h>
#include <nv_memory.h>
#include <nv_utils.h>
#include <epoch.h>
#include "common.h"
#include "random.h"
#include "lf-common.h"

inline void finalize_node(void * node, void * context, void* tls) {
    EpochFreeNode(node);
}

static inline UINT_PTR unmarked_ptr(UINT_PTR p) {
    return(p & ~(UINT_PTR)0x01);
}

#define UNMARKED_PTR(p) (node_t*)unmarked_ptr((UINT_PTR) p)

static inline UINT_PTR marked_ptr(UINT_PTR p) {
    return (p | (UINT_PTR)0x01);
}

#define MARKED_PTR(p) (node_t*)marked_ptr((UINT_PTR) p)

static inline int ptr_is_marked(UINT_PTR p) {
    return (int)(p & (UINT_PTR)0x01);
}

#define PTR_IS_MARKED(p) ptr_is_marked((UINT_PTR) p)

#define NODE_PADDING 1

#define CACHE_LINES_PER_NV_NODE 1 //TODO does nv-jemalloc need to be aware of this?

typedef struct node_t {
    skey_t key;
    svalue_t value;
    volatile node_t* next;
#ifdef NODE_PADDING
    BYTE padding[CACHE_LINE_SIZE - sizeof(skey_t) - sizeof(svalue_t) - sizeof(void*)];
#endif
} node_t;


typedef volatile node_t* linkedlist_t;
typedef linkedlist_t* plinkedlist_t;

svalue_t linkedlist_find(linkedlist_t * ll, skey_t key, EpochThread epoch, linkcache_t* buffer);
int linkedlist_insert(linkedlist_t * ll, skey_t key, svalue_t value, EpochThread epoch, linkcache_t* buffer);
svalue_t linkedlist_remove(linkedlist_t *ll, skey_t key, EpochThread epoch, linkcache_t* buffer);

linkedlist_t* new_linkedlist(EpochThread epoch);
void bucket_set_init(linkedlist_t* set, EpochThread epoch);
void delete_linkedlist(linkedlist_t * ll);

volatile node_t* new_node(skey_t key, svalue_t value, EpochThread epoch);
volatile node_t* new_node_and_set_next(skey_t key, svalue_t value, volatile node_t* next, EpochThread epoch);

int is_reachable(linkedlist_t* ll, void* address);
void recover(linkedlist_t* ll, linkcache_t* buffer, active_page_table_t** page_buffers, int num_page_buffers);

int linkedlist_size(linkedlist_t* ll);

#endif
