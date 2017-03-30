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

NODE_PTR initialize_tree(EpochThread epoch){
    NODE_PTR r;
    NODE_PTR s;
    NODE_PTR inf0;
    NODE_PTR inf1;
    NODE_PTR inf2;
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

NODE_PTR create_node(skey_t k, svalue_t value, int initializing, EpochThread epoch) {
    NODE_PTR new_node;

    new_node = (NODE_PTR) EpochAllocNode(epoch, sizeof(node_t));

    new_node->left = NULL;
    new_node->right = NULL;
    new_node->key = k;
    new_node->value = value;
    // asm volatile("" ::: "memory");
    _mm_sfence();

    return (NODE_PTR) new_node;
}

seek_record_t * bst_seek(skey_t key, NODE_PTR node_r, EpochThread epoch, linkcache_t* buffer) { 
    PARSE_TRY();
    volatile seek_record_t seek_record_l;
    NODE_PTR node_s = ADDRESS(node_r->left);
    seek_record_l.ancestor = node_r;
    seek_record_l.successor = node_s; 
    seek_record_l.parent = node_s;
    seek_record_l.leaf = ADDRESS_W_CACHE_MARK_BIT(node_s->left);

    NODE_PTR parent_field = (NODE_PTR) ADDRESS(seek_record_l.parent)->left;
    NODE_PTR current_field = (NODE_PTR) ADDRESS(seek_record_l.leaf)->left;
    NODE_PTR current = ADDRESS_W_CACHE_MARK_BIT(current_field);


    while (current != NULL) {
        if (!GETTAG(parent_field)) {
            seek_record_l.ancestor = seek_record_l.parent;
            seek_record_l.successor = seek_record_l.leaf;
        }
        seek_record_l.parent = seek_record_l.leaf;
        seek_record_l.leaf = current;

        parent_field = current_field;
        current = ADDRESS(current);
        if (key < current->key) {
            current_field= (NODE_PTR) current->left;
        } else {
            current_field= (NODE_PTR) current->right;
        }
        current=ADDRESS_W_CACHE_MARK_BIT(current_field);
    }
    seek_record->ancestor=seek_record_l.ancestor;
    seek_record->successor=seek_record_l.successor;
    seek_record->parent=seek_record_l.parent;
    seek_record->leaf=seek_record_l.leaf;

    if ((key > ADDRESS(seek_record_l.parent)->key && ADDRESS(seek_record_l.leaf)->key < ADDRESS(seek_record_l.parent)->key) 
            || (key < ADDRESS(seek_record_l.parent)->key && ADDRESS(seek_record_l.leaf)->key > ADDRESS(seek_record_l.parent)->key)) {
        printf("WEIRD STUFF!\n");
    }
    return seek_record;
}

svalue_t bst_search(skey_t key, NODE_PTR node_r, EpochThread epoch, linkcache_t* buffer) {
   bst_seek(key, node_r, epoch, buffer);
   if (ADDRESS(seek_record->leaf)->key == key) {
#ifdef BUFFERING_ON
        cache_scan(buffer, key);
#else
        flush_and_try_unflag((PVOID*)&(seek_record->parent->next));
#endif
        return ADDRESS(seek_record->leaf)->value;
   } else {
#ifdef BUFFERING_ON
        cache_scan(buffer, key);
#else
        flush_and_try_unflag((PVOID*)&(seek_record->parent->next));
#endif
        return 0;
   }
}


bool_t bst_insert(skey_t key, svalue_t val, NODE_PTR node_r, EpochThread epoch, linkcache_t* buffer) {
    NODE_PTR new_internal = NULL;
    NODE_PTR new_node = NULL;
    uint created = 0;
    while (1) {
        UPDATE_TRY();

        bst_seek(key, node_r, epoch, buffer);
        if (ADDRESS(seek_record->leaf)->key == key) {
// #if GC == 1
//             if (created) {
//                 ssmem_free(alloc, new_internal);
//                 ssmem_free(alloc, new_node);
//             }
// #endif
#ifdef BUFFERING_ON
            cache_scan(buffer, key);
#else
            flush_and_try_unflag((PVOID*)&(seek_record->parent->left)); 
            flush_and_try_unflag((PVOID*)&(seek_record->parent->right));
#endif
            return FALSE;
        }
        NODE_PTR parent = seek_record->parent;
        NODE_PTR leaf = seek_record->leaf;

        NODE_PTR* child_addr;
        parent = ADDRESS(parent);
        if (key < parent->key) {
            child_addr= (NODE_PTR*) &(parent->left); 
        } else {
            child_addr= (NODE_PTR*) &(parent->right);
        }
        if (likely(created==0)) {
            new_internal=create_node(max(key,ADDRESS(leaf)->key),0,0,epoch);
            new_node = create_node(key,val,0,epoch);
            write_data_nowait((void*) new_node, CACHE_LINES_PER_NV_NODE);
            created=1;
        } else {
            new_internal->key=max(key,ADDRESS(leaf)->key);
        }
        if ( key < ADDRESS(leaf)->key) {
            new_internal->left = new_node;
            new_internal->right = leaf;
            write_data_wait((void*) new_node, CACHE_LINES_PER_NV_NODE);
        } else {
            new_internal->right = new_node;
            new_internal->left = leaf;
            write_data_wait((void*) new_node, CACHE_LINES_PER_NV_NODE);
        }
 #ifdef __tile__
    MEM_BARRIER;
#endif
        // node_t* result = CAS_PTR(child_addr, ADDRESS(leaf), ADDRESS(new_internal));
        // if (result == ADDRESS(leaf)) {
        //     return TRUE;
        // }

        if (cache_try_link_and_add(buffer, key, (volatile void **) child_addr, ADDRESS_W_CACHE_MARK_BIT(leaf), ADDRESS_W_CACHE_MARK_BIT(new_internal))) {

            return TRUE;
        }

        NODE_PTR chld = *child_addr; 
        if ( (ADDRESS(chld)==leaf) && (GETFLAG(chld) || GETTAG(chld)) ) {
            bst_cleanup(key, epoch, buffer); 
        }
    }
}

svalue_t bst_remove(skey_t key, NODE_PTR node_r, EpochThread epoch, linkcache_t* buffer) {
    bool_t injecting = TRUE; 
    NODE_PTR leaf;
    svalue_t val = 0;
    while (1) {
      UPDATE_TRY();

        bst_seek(key, node_r, epoch, buffer);
        val = seek_record->leaf->value;
        NODE_PTR parent = seek_record->parent;

        NODE_PTR* child_addr;

        parent = ADDRESS(parent);

        if (key < parent->key) {
            child_addr = (NODE_PTR*) &(parent->left);
        } else {
            child_addr = (NODE_PTR*) &(parent->right);
        }

        if (injecting == TRUE) {
            leaf = seek_record->leaf;
            if (ADDRESS(leaf)->key != key) {
#ifdef BUFFERING_ON
                cache_scan(buffer, key);
#else
                flush_and_try_unflag((PVOID*)&(seek_record->parent->left)); 
                flush_and_try_unflag((PVOID*)&(seek_record->parent->right));
#endif
                return 0;
            }
            NODE_PTR lf = ADDRESS_W_CACHE_MARK_BIT(leaf);
            NODE_PTR result = CAS_PTR(child_addr, lf, FLAG(lf));
            if (result == ADDRESS_W_CACHE_MARK_BIT(leaf)) {
                injecting = FALSE;
                bool_t done = bst_cleanup(key, epoch, buffer);
                if (done == TRUE) {
                    return val;
                }
            } else {
                NODE_PTR chld = *child_addr;
                if ( (ADDRESS_W_CACHE_MARK_BIT(chld) == leaf) && (GETFLAG(chld) || GETTAG(chld)) ) {
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
    NODE_PTR ancestor = seek_record->ancestor;
    NODE_PTR successor = seek_record->successor;
    NODE_PTR parent = seek_record->parent;
    //node_t* leaf = seek_record->leaf;

    NODE_PTR* succ_addr;
    ancestor = ADDRESS(ancestor);
    if (key < ancestor->key) {
        succ_addr = (NODE_PTR*) &(ancestor->left);
    } else {
        succ_addr = (NODE_PTR*) &(ancestor->right);
    }

    NODE_PTR* child_addr;
    NODE_PTR* sibling_addr;
    parent = ADDRESS(parent);
    if (key < parent->key) {
       child_addr = (NODE_PTR*) &(parent->left);
       sibling_addr = (NODE_PTR*) &(parent->right);
    } else {
       child_addr = (NODE_PTR*) &(parent->right);
       sibling_addr = (NODE_PTR*) &(parent->left);
    }

    NODE_PTR chld = *(child_addr);
    if (!GETFLAG(chld)) {
        chld = *(sibling_addr);
        asm volatile("");
        sibling_addr = child_addr;
    }
//#if defined(__tile__) || defined(__sparc__)
    while (1) {
        NODE_PTR untagged = *sibling_addr;
        NODE_PTR tagged = (NODE_PTR)TAG(untagged);
        NODE_PTR res = CAS_PTR(sibling_addr,untagged, tagged);
        if (res == untagged) {
            break;
         }
    }
//#else
//    set_bit(sibling_addr,1);
//#endif

    NODE_PTR sibl = *sibling_addr;
    if (cache_try_link_and_add(buffer, key, (volatile void **)succ_addr, ADDRESS_W_CACHE_MARK_BIT(successor), (void*)UNTAG(sibl))) {
    // if ( CAS_PTR(succ_addr, ADDRESS_W_CACHE_MARK_BIT(successor), UNTAG(sibl)) == ADDRESS_W_CACHE_MARK_BIT(successor)) {

// #if GC == 1
//     ssmem_free(alloc, ADDRESS(chld));
//     ssmem_free(alloc, ADDRESS(successor));
// #endif
        return TRUE;
    }
    return FALSE;
}

uint32_t bst_size(NODE_PTR node) {
    node = ADDRESS(node);
    if (node == NULL) return 0;

    if ((node->left == NULL) && (node->right == NULL)) {
       if (node->key < INF0 ) return 1;
    }
    uint32_t l = 0;
    uint32_t r = 0;

    flush_and_try_unflag((PVOID*)&(node->left));
    flush_and_try_unflag((PVOID*)&(node->right));

    if ( !GETFLAG(node->left) && !GETTAG(node->left)) {
        l = bst_size(node->left);
    }
    if ( !GETFLAG(node->right) && !GETTAG(node->right)) {
        r = bst_size(node->right);
    }
    return l+r;
}


