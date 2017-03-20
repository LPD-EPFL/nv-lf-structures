/*   
 *   File: bst-aravind.c
 *   Author: Tudor David <tudor.david@epfl.ch>
 *   Description: Aravind Natarajan and Neeraj Mittal. 
 *   Fast Concurrent Lock-free Binary Search Trees. PPoPP 2014
 *   bst-aravind.c is part of ASCYLIB
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

#include "lf-bst.h"

RETRY_STATS_VARS;

__thread seek_record_t* seek_record;

node_t* initialize_tree(EpochThread epoch){
    node_t* r;
    node_t* s;
    node_t* inf0;
    node_t* inf1;
    node_t* inf2;
    r = create_node(INF2,0,1,epoch);
    s = create_node(INF1,0,1,epoch);
    inf0 = create_node(INF0,0,1,epoch);
    inf1 = create_node(INF1,0,1,epoch);
    inf2 = create_node(INF2,0,1,epoch);
    
    asm volatile("" ::: "memory");
    r->left = s;
    r->right = inf2;
    s->right = inf1;
    s->left= inf0;
    asm volatile("" ::: "memory");

    return r;
}

void bst_init_local() {
  seek_record = (seek_record_t*) memalign(CACHE_LINE_SIZE, sizeof(seek_record_t));
  assert(seek_record != NULL);
}

node_t* create_node(skey_t k, svalue_t value, int initializing, EpochThread epoch) {
    volatile node_t* new_node;

    new_node = (node_t*) EpochAllocNode(epoch, sizeof(node_t));

    new_node->left = NULL;
    new_node->right = NULL;
    new_node->key = k;
    new_node->value = value;
    // asm volatile("" ::: "memory");
    _mm_sfence();

    return (node_t*) new_node;
}

seek_record_t * bst_seek(skey_t key, node_t* node_r, EpochThread epoch, linkcache_t* buffer) { 
    PARSE_TRY();
    volatile seek_record_t seek_record_l;
    node_t* node_s = ADDRESS(node_r->left);
    seek_record_l.ancestor = node_r;
    seek_record_l.successor = node_s; 
    seek_record_l.parent = node_s;
    seek_record_l.leaf = ADDRESS(node_s->left);

    node_t* parent_field = (node_t*) seek_record_l.parent->left;
    node_t* current_field = (node_t*) seek_record_l.leaf->left;
    node_t* current = ADDRESS(current_field);


    while (current != NULL) {
        if (!GETTAG(parent_field)) {
            seek_record_l.ancestor = seek_record_l.parent;
            seek_record_l.successor = seek_record_l.leaf;
        }
        seek_record_l.parent = seek_record_l.leaf;
        seek_record_l.leaf = current;

        parent_field = current_field;
        if (key < current->key) {
            current_field= (node_t*) current->left;
        } else {
            current_field= (node_t*) current->right;
        }
        current=ADDRESS(current_field);
    }
    seek_record->ancestor=seek_record_l.ancestor;
    seek_record->successor=seek_record_l.successor;
    seek_record->parent=seek_record_l.parent;
    seek_record->leaf=seek_record_l.leaf;
    return seek_record;
}

svalue_t bst_search(skey_t key, node_t* node_r, EpochThread epoch, linkcache_t* buffer) {
   bst_seek(key, node_r, epoch, buffer);
   if (seek_record->leaf->key == key) {
        return seek_record->leaf->value;
   } else {
        return 0;
   }
}


bool_t bst_insert(skey_t key, svalue_t val, node_t* node_r, EpochThread epoch, linkcache_t* buffer) {
    node_t* new_internal = NULL;
    node_t* new_node = NULL;
    uint created = 0;
    while (1) {
      UPDATE_TRY();

        bst_seek(key, node_r, epoch, buffer);
        if (seek_record->leaf->key == key) {
#if GC == 1
            if (created) {
                ssmem_free(alloc, new_internal);
                ssmem_free(alloc, new_node);
            }
#endif
            return FALSE;
        }
        node_t* parent = seek_record->parent;
        node_t* leaf = seek_record->leaf;

        node_t** child_addr;
        if (key < parent->key) {
      child_addr= (node_t**) &(parent->left); 
        } else {
            child_addr= (node_t**) &(parent->right);
        }
        if (likely(created==0)) {
            new_internal=create_node(max(key,leaf->key),0,0,epoch);
            new_node = create_node(key,val,0,epoch);
            created=1;
        } else {
            new_internal->key=max(key,leaf->key);
        }
        if ( key < leaf->key) {
            new_internal->left = new_node;
            new_internal->right = leaf; 
        } else {
            new_internal->right = new_node;
            new_internal->left = leaf;
        }
 #ifdef __tile__
    MEM_BARRIER;
#endif
        node_t* result = CAS_PTR(child_addr, ADDRESS(leaf), ADDRESS(new_internal));
        if (result == ADDRESS(leaf)) {
            return TRUE;
        }
        node_t* chld = *child_addr; 
        if ( (ADDRESS(chld)==leaf) && (GETFLAG(chld) || GETTAG(chld)) ) {
            bst_cleanup(key, epoch, buffer); 
        }
    }
}

svalue_t bst_remove(skey_t key, node_t* node_r, EpochThread epoch, linkcache_t* buffer) {
    bool_t injecting = TRUE; 
    node_t* leaf;
    svalue_t val = 0;
    while (1) {
      UPDATE_TRY();

        bst_seek(key, node_r, epoch, buffer);
        val = seek_record->leaf->value;
        node_t* parent = seek_record->parent;

        node_t** child_addr;
        if (key < parent->key) {
            child_addr = (node_t**) &(parent->left);
        } else {
            child_addr = (node_t**) &(parent->right);
        }

        if (injecting == TRUE) {
            leaf = seek_record->leaf;
            if (leaf->key != key) {
                return 0;
            }
            node_t* lf = ADDRESS(leaf);
            node_t* result = CAS_PTR(child_addr, lf, FLAG(lf));
            if (result == ADDRESS(leaf)) {
                injecting = FALSE;
                bool_t done = bst_cleanup(key, epoch, buffer);
                if (done == TRUE) {
                    return val;
                }
            } else {
                node_t* chld = *child_addr;
                if ( (ADDRESS(chld) == leaf) && (GETFLAG(chld) || GETTAG(chld)) ) {
                    bst_cleanup(key, epoch, buffer);
                }
            }
        } else {
            if (seek_record->leaf != leaf) {
                return val; 
            } else {
                bool_t done = bst_cleanup(key, epoch, buffer);
                if (done == TRUE) {
                    return val;
                }
            }
        }
    }
}


bool_t bst_cleanup(skey_t key, EpochThread epoch, linkcache_t* buffer) {
    node_t* ancestor = seek_record->ancestor;
    node_t* successor = seek_record->successor;
    node_t* parent = seek_record->parent;
    //node_t* leaf = seek_record->leaf;

    node_t** succ_addr;
    if (key < ancestor->key) {
        succ_addr = (node_t**) &(ancestor->left);
    } else {
        succ_addr = (node_t**) &(ancestor->right);
    }

    node_t** child_addr;
    node_t** sibling_addr;
    if (key < parent->key) {
       child_addr = (node_t**) &(parent->left);
       sibling_addr = (node_t**) &(parent->right);
    } else {
       child_addr = (node_t**) &(parent->right);
       sibling_addr = (node_t**) &(parent->left);
    }

    node_t* chld = *(child_addr);
    if (!GETFLAG(chld)) {
        chld = *(sibling_addr);
        asm volatile("");
        sibling_addr = child_addr;
    }
//#if defined(__tile__) || defined(__sparc__)
    while (1) {
        node_t* untagged = *sibling_addr;
        node_t* tagged = (node_t*)TAG(untagged);
        node_t* res = CAS_PTR(sibling_addr,untagged, tagged);
        if (res == untagged) {
            break;
         }
    }
//#else
//    set_bit(sibling_addr,1);
//#endif

    node_t* sibl = *sibling_addr;
    if ( CAS_PTR(succ_addr, ADDRESS(successor), UNTAG(sibl)) == ADDRESS(successor)) {
#if GC == 1
    ssmem_free(alloc, ADDRESS(chld));
    ssmem_free(alloc, ADDRESS(successor));
#endif
        return TRUE;
    }
    return FALSE;
}

uint32_t bst_size(volatile node_t* node) {
    if (node == NULL) return 0; 
    if ((node->left == NULL) && (node->right == NULL)) {
       if (node->key < INF0 ) return 1;
    }
    uint32_t l = 0;
    uint32_t r = 0;
    if ( !GETFLAG(node->left) && !GETTAG(node->left)) {
        l = bst_size(node->left);
    }
    if ( !GETFLAG(node->right) && !GETTAG(node->right)) {
        r = bst_size(node->right);
    }
    return l+r;
}


