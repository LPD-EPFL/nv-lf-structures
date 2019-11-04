#ifndef _LF_SKIPLIST_H_
#define _LF_SKIPLIST_H_

/*
	lock-free skip-list algorithm
*/

#include <link-cache.h>
#include <assert.h>
#include <active-page-table.h>
#include <nv_memory.h>
#include <nv_utils.h>
#include <epoch.h>
#include "common.h"
#include "random.h"
#include "lf-common.h"

#define NODE_PADDING 1

#define CACHE_LINES_PER_NV_NODE 3 //TODO does nv-jemalloc need to be aware of this?

#define max_level ((CACHE_LINES_PER_NV_NODE * 8) - 3) //one cache-line node; use 13 for two cache-line nodes

typedef struct node_t {
	skey_t key;
	svalue_t value;

	volatile node_t* next[max_level]; //in our allocator, we will be working with chunks of the same size, so every node should have the same size
	UINT32 toplevel;
	BYTE node_flags;
#ifdef NODE_PADDING
	BYTE padding[3]; //TODO check padding size
#endif
} node_t;


typedef volatile node_t* skiplist_t;

const int skiplist_node_size = sizeof(node_t);

svalue_t skiplist_find(skiplist_t * sl, skey_t key, EpochThread epoch, linkcache_t* buffer);
int skiplist_insert(skiplist_t * sl, skey_t key, svalue_t value, EpochThread epoch, linkcache_t* buffer);
svalue_t skiplist_remove(skiplist_t *sl, skey_t key, EpochThread epoch, linkcache_t* buffer);

skiplist_t* new_skiplist(EpochThread epoch);
void delete_skiplist(skiplist_t * sl);

volatile node_t* new_node(skey_t key, svalue_t value, int height, EpochThread epoch);
volatile node_t* new_node_and_set_next(skey_t key, svalue_t value, int height, volatile node_t* next, EpochThread epoch);
void delete_node(node_t* node);

int is_reachable(skiplist_t* sl, void* address);
void recover(skiplist_t* sl, active_page_table_t** page_buffers, int num_page_buffers);

int skiplist_size(skiplist_t* sl);


#endif
