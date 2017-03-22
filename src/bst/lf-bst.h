/*   
 *   File: bst-aravind.h
 *   Author: Tudor David <tudor.david@epfl.ch>
 *   Description: Aravind Natarajan and Neeraj Mittal. 
 *   Fast Concurrent Lock-free Binary Search Trees. PPoPP 2014
 *   bst-aravind.h is part of ASCYLIB
 *
 * Copyright (c) 2014 Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>,
 *                Tudor David <tudor.david@epfl.ch>
 *                Distributed Programming Lab (LPD), EPFL
 *
 * ASCYLIB is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 */

#ifndef _BST_ARAVIND_H_INCLUDED_
#define _BST_ARAVIND_H_INCLUDED_

#include <link-cache.h>
#include <active-page-table.h>
#include <nv_memory.h>
#include <nv_utils.h>
#include <epoch.h>
#include "common.h"
#include "random.h"
#include "lf-common.h"

#define CACHE_LINES_PER_NV_NODE 1

#define max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })



#define TRUE 1
#define FALSE 0

#define INF2 (KEY_MAX + 2)
#define INF1 (KEY_MAX + 1)
#define INF0 (KEY_MAX)

#define MAX_KEY KEY_MAX
#define MIN_KEY 0

typedef uint8_t bool_t;

typedef ALIGNED(64) struct node_t node_t;

struct node_t{
    skey_t key;
    svalue_t value;
    volatile node_t* right;
    volatile node_t* left;
  uint8_t padding[32];
};

#ifndef __tile__
#ifndef __sparc__

static inline void set_bit(volatile uintptr_t* *array, int bit) {
    asm("bts %1,%0" : "+m" (*array) : "r" (bit));
}
static inline bool_t set_bit2(volatile uintptr_t *array, int bit) {

   // asm("bts %1,%0" :  "+m" (*array): "r" (bit));
     bool_t flag; 
     __asm__ __volatile__("lock bts %2,%1; setb %0" : "=q" (flag) : "m" (*array), "r" (bit)); return flag; 
   return flag;
}
#endif
#endif


typedef ALIGNED(CACHE_LINE_SIZE) struct seek_record_t{
    node_t* ancestor;
    node_t* successor;
    node_t* parent;
    node_t* leaf;
  uint8_t padding[32];
} seek_record_t;

//extern __thread seek_record_t* seek_record;

node_t* initialize_tree(EpochThread epoch);
void bst_init_local();
node_t* create_node(skey_t k, svalue_t value, int initializing, EpochThread epoch);
seek_record_t * bst_seek(skey_t key, node_t* node_r, EpochThread epoch, linkcache_t* buffer);
svalue_t bst_search(skey_t key, node_t* node_r, EpochThread epoch, linkcache_t* buffer);
bool_t bst_insert(skey_t key, svalue_t val, node_t* node_r, EpochThread epoch, linkcache_t* buffer);
svalue_t bst_remove(skey_t key, node_t* node_r, EpochThread epoch, linkcache_t* buffer);
bool_t bst_cleanup(skey_t key, EpochThread epoch, linkcache_t* buffer);
uint32_t bst_size(volatile node_t* r);

static inline uint64_t GETFLAG(volatile node_t* ptr) {
    return ((uint64_t)ptr) & 1;
}

static inline uint64_t GETTAG(volatile node_t* ptr) {
    return ((uint64_t)ptr) & 4;
}

static inline uint64_t FLAG(node_t* ptr) {
    return (((uint64_t)ptr)) | 1;
}

static inline uint64_t TAG(node_t* ptr) {
    return (((uint64_t)ptr)) | 4;
}

static inline uint64_t UNTAG(node_t* ptr) {
    return (((uint64_t)ptr) & 0xfffffffffffffffb);
}

static inline uint64_t UNFLAG(node_t* ptr) {
    return (((uint64_t)ptr) & 0xfffffffffffffffe);
}

static inline node_t* ADDRESS(volatile node_t* ptr) {
    return (node_t*) (((uint64_t)ptr) & 0xfffffffffffffff8);
}

#endif
