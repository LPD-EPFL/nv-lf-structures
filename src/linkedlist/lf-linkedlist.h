#pragma once

/*
 *  lock-free linked list algorithm
 */

//#include "lf-common.h"
//#include "FlushBuffer_one_cl.h"

typedef struct ll_node_t {
    skey_t key;
    svalue_t value;
    volatile ll_node_t* next;
#ifdef NODE_PADDING
    BYTE padding[CACHE_LINE_SIZE - sizeof(skey_t) - sizeof(svalue_t) - sizeof(void*)];
#endif
} ll_node_t;


typedef volatile ll_node_t* linkedlist_t;

svalue_t linkedlist_find(linkedlist_t * ll, skey_t key, EpochThread epoch, flushbuffer_t* buffer);
int linkedlist_insert(linkedlist_t * ll, skey_t key, svalue_t value, EpochThread epoch, flushbuffer_t* buffer);
svalue_t linkedlist_remove(linkedlist_t *ll, skey_t key, EpochThread epoch, flushbuffer_t* buffer);

linkedlist_t* new_linkedlist(EpochThread epoch);
void delete_linkedlist(linkedlist_t * ll);

volatile ll_node_t* new_node(skey_t key, svalue_t value, EpochThread epoch);
volatile ll_node_t* new_node_and_set_next(skey_t key, svalue_t value, volatile ll_node_t* next, EpochThread epoch);

int is_reachable(linkedlist_t* ll, void* address);
void recover(linkedlist_t* ll, flushbuffer_t* buffer, page_buffer_t** page_buffers, int num_page_buffers);

int linkedlist_size(linkedlist_t* ll);
