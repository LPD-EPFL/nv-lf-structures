#pragma once
/*
	lock-free skip-list algorithm
*/

//#include "lf-common.h"
//#include "FlushBuffer_one_cl.h"


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

svalue_t skiplist_find(skiplist_t * sl, skey_t key, EpochThread epoch, flushbuffer_t* buffer);
int skiplist_insert(skiplist_t * sl, skey_t key, svalue_t value, EpochThread epoch, flushbuffer_t* buffer);
svalue_t skiplist_remove(skiplist_t *sl, skey_t key, EpochThread epoch, flushbuffer_t* buffer);

skiplist_t* new_skiplist(EpochThread epoch);
void delete_skiplist(skiplist_t * sl);

volatile node_t* new_node(skey_t key, svalue_t value, int height, EpochThread epoch);
volatile node_t* new_node_and_set_next(skey_t key, svalue_t value, int height, volatile node_t* next, EpochThread epoch);
void delete_node(node_t* node);

int is_reachable(skiplist_t* sl, void* address);
void recover(skiplist_t* sl, page_buffer_t** page_buffers, int num_page_buffers);

int skiplist_size(skiplist_t* sl);

