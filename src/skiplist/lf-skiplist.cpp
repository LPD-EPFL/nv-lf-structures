#include "lf-skiplist.h"

static inline UINT_PTR unmarked_ptr(UINT_PTR p) {
	return(p & ~(UINT_PTR)0x01);
}

#define UNMARKED_PTR(p) (node_t*)unmarked_ptr((UINT_PTR) p)

static inline UINT_PTR unmarked_ptr_all(UINT_PTR p) {
	return(p & ~(UINT_PTR)0x03);
}

#define UNMARKED_PTR_ALL(p) (node_t*)unmarked_ptr_all((UINT_PTR) p)

static inline UINT_PTR marked_ptr(UINT_PTR p) {
	return (p | (UINT_PTR)0x01);
}

#define MARKED_PTR(p) (node_t*)marked_ptr((UINT_PTR) p)

static inline int ptr_is_marked(UINT_PTR p) {
	return (int)(p & (UINT_PTR)0x01);
}

#define PTR_IS_MARKED(p) ptr_is_marked((UINT_PTR) p)

int get_random_level() {
	int i;
	int level = 1;

	for (i = 0; i < max_level - 1; i++) {
		if (rand_range(101) < 50) { //TODO replace the mod with something else?
			level++;
		}
		else {
			break;
		}
	}
	return level;
}

void sl_finalize_node(void * node, void * context, void* tls) {
	EpochFreeNode(node);
}

skiplist_t* new_skiplist(EpochThread epoch) {
	//TODO: in persistent memory, this might have to be done differently;
	// this method shoulsd create a named entry which can be accessed from
	// persistent memory using only this names
	skiplist_t* sl = (skiplist_t*)malloc(sizeof(skiplist_t));
	*sl = NULL;

	volatile node_t* max = new_node_and_set_next(MAX_KEY, 0, max_level, NULL, epoch);
	volatile node_t* min = new_node_and_set_next(MIN_KEY, 0, max_level, max, epoch);
	(*sl) = min;
	return sl;
}
void delete_skiplist(skiplist_t* sl) {
	sl_finalize_node((void*)(*sl)->next[0], NULL, NULL);
	sl_finalize_node((void*)(*sl), NULL, NULL);
	free(sl);
}

volatile node_t* new_node(skey_t key, svalue_t value, int height, EpochThread epoch) {
	volatile node_t* the_node;
	the_node = (node_t*)EpochAllocNode(epoch, sizeof(node_t));
	the_node->key = key;
	the_node->value = value;
	the_node->toplevel = height;

	for (int i = 0; i < max_level; i++) {
		the_node->next[i] = NULL;
	}
    _mm_sfence();
	return the_node;
}

volatile node_t* new_node_and_set_next(skey_t key, svalue_t value, int height, volatile node_t* next, EpochThread epoch) {
	volatile node_t* the_node;
	the_node = (node_t*)EpochAllocNode(epoch, sizeof(node_t));
	the_node->key = key;
	the_node->value = value;
	the_node->toplevel = height;
	int i;

	for (i = 0; i < max_level; i++) {
		the_node->next[i] = next;
	}
	write_data_wait((void*)the_node, CACHE_LINES_PER_NV_NODE);
    _mm_sfence();
	return the_node;
}

void delete_node(node_t* node) {
	//TODO implement? - I don't need both this and the finalize method
}

int skiplist_size(skiplist_t* sl) {
	int size = 0;

	volatile node_t* node = UNMARKED_PTR((*sl)->next[0]);
	while (node->next[0] != NULL) {
		if (!PTR_IS_MARKED(node->next[0])) {
			size++;
		}
		node = UNMARKED_PTR(node->next[0]);
	}

	return size;
}

//search operation; retrieves predecessors and successors of the searched node; does cleanup if necessary - if the searched node is marked for removal, it is unlinked
//for the pourposes of non-volatile memory, it is the level 0 unlink that is critical
int sl_search(skiplist_t* sl, skey_t key, volatile node_t** left_nodes, volatile node_t** right_nodes, linkcache_t* buffer, EpochThread epoch) {
	int i;
	volatile node_t *left;
	volatile node_t* right = NULL;
	volatile node_t* left_next;
	volatile node_t* right_next;

retry:
	left = (*sl);
	for (i = max_level - 1; i >= 0; i--) {
		left_next = left->next[i];
		if (PTR_IS_MARKED(left_next)) {
			goto retry;
		}


		for (right = left_next; ; right = right_next) {
			right_next = right->next[i];
			while ((PTR_IS_MARKED(right_next))) {
				right = UNMARKED_PTR_ALL(right_next);
				right_next = right->next[i];
			}

			if (right->key >= key) {
				break;
			}
			left = right;
			left_next = right_next;
		}
		if ((i == 0) && (epoch!=NULL)) {
			EpochDeclareUnlinkNode(epoch, (void*)left_next, skiplist_node_size);
		}
#ifdef BUFFERING_ON
		if ((i == 0) && (buffer != NULL)) {
			if ((left_next != right) && (cache_try_link_and_add(buffer, key, (volatile void**) &(left->next[i]), left_next, right) == 0)) {
				goto retry;
			}
		}
		else {
			if ((left_next != right) &&
				(CAS_PTR((volatile PVOID*) &(left->next[i]), (PVOID)left_next, (PVOID)right)
					!= left_next)) {
				goto retry;
			}
			if ((left_next != right) && ((i == 0) || (buffer == NULL))) {
				write_data_wait((void*)&(left->next[i]), 1);
			}
		}
#else
		if (i != 0) {
#ifdef SIMULATE_NAIVE_IMPLEMENTATION
			write_data_wait(&(left_next), 1);
#endif
			if ((left_next != right) &&
				(CAS_PTR((volatile PVOID*) &(left->next[i]), (PVOID)left_next, (PVOID)right)
					!= left_next)) {
				goto retry;
			}
		}
		else {
			if ((left_next != right) &&
				((node_t*)link_and_persist((PVOID*) &(left->next[i]), (PVOID)left_next, (PVOID)right)
					!= left_next)) {
				goto retry;
			}
		}
#endif

		//recovery can handle out-of order links

/*#ifdef BUFFERING_ON
		if ((i == 0) && (left_next != right)) {
			cache_add(buffer, key, (void*)&(left->next[0]));
		}
#endif*/

		if (left_nodes != NULL) {
			left_nodes[i] = left;
		}

		if (right_nodes != NULL) {
			right_nodes[i] = right;
		}
	}

	return (right->key == key);
}

//search with no cleanup, retrieves both the predecessors and the successors of the searched node
int sl_search_no_cleanup(skiplist_t* sl, skey_t key, volatile node_t** left_nodes, volatile node_t** right_nodes) {
	int i;
	volatile node_t *left;
	volatile node_t* right = NULL;
	volatile node_t* left_next;

	left = (*sl);

	for (i = max_level - 1; i >= 0; i--) {
		left_next = UNMARKED_PTR_ALL(left->next[i]);
		right = left_next;
		while (1) {
			if (!PTR_IS_MARKED(right->next[i])) {
				if (right->key >= key) {
					break;
				}
				left = right;
			}
			right = UNMARKED_PTR_ALL(right->next[i]);
		}
		left_nodes[i] = left;
		right_nodes[i] = right;
	}
	return (right->key == key);
}

//simple search, does not do any cleanup, only sets the successors of the retrieved node
int sl_search_no_cleanup_succs(skiplist_t* sl, skey_t key, volatile node_t** right_nodes) {
	int i;
	volatile node_t *left;
	volatile node_t* right = NULL;
	volatile node_t* left_next;

	left = (*sl);
	for (i = max_level - 1; i >= 0; i--) {
		left_next = UNMARKED_PTR_ALL(left->next[i]);
		right = left_next;
		while (1) {
			if (!PTR_IS_MARKED(right->next[i])) {
				if (right->key >= key) {
					break;
				}
				left = right;
			}
			right = UNMARKED_PTR_ALL(right->next[i]);
		}
		right_nodes[i] = right;
	}
#ifndef BUFFERING_ON
	if (right->key != key) {
		flush_and_try_unflag((PVOID*)&(left->next[0]));
}
#endif // !BUFFERING_ON


	return (right->key == key);
}

inline int mark_node_pointers( volatile node_t* node, linkcache_t* buffer) {
	int i;
	int success = 0;
	//fprintf(stderr, "in mark node ptrs\n");

	volatile node_t* next_node;

	for (i = node->toplevel - 1; i >= 1; i--) {
		do {
			next_node = node->next[i];
			if (PTR_IS_MARKED(next_node)) {
				success = 0;
				break;
			}
			success = (CAS_PTR((PVOID*)&node->next[i], UNMARKED_PTR(next_node), MARKED_PTR(next_node)) == UNMARKED_PTR(next_node)) ? 1: 0;
#ifdef SIMULATE_NAIVE_IMPLEMENTATION
			write_data_wait((void*)(&node->next[i]), 1);
#endif
		} while (success == 0);
	}

	do {
		next_node = node->next[0];
		if (PTR_IS_MARKED(next_node)) {
			success = 0;
			break;
		}
#ifdef BUFFERING_ON
		if (buffer != NULL) {
			success = cache_try_link_and_add(buffer, node->key, (volatile void**)&node->next[0], UNMARKED_PTR(next_node), MARKED_PTR(next_node));
			//node logically deleted at this point (i.e., searches for key will retrun false; need to make sure if this is the case it will appear so after recovery too - hence the addition of the link to the buffer)
		}
		else {
			//buffer is null when recovering
			success = (CAS_PTR((PVOID*)&node->next[0],  UNMARKED_PTR(next_node), MARKED_PTR(next_node)) == UNMARKED_PTR(next_node)) ? 1 : 0;
			if (success) {
				write_data_wait((void*)&(node->next[0]), 1);
			}
		}
#else
		success = ((node_t*)link_and_persist((PVOID*)&node->next[0], UNMARKED_PTR(next_node), MARKED_PTR(next_node)) == UNMARKED_PTR(next_node)) ? 1 : 0;
#endif
	} while (success == 0);
	return success;
}

svalue_t skiplist_remove(skiplist_t* sl, skey_t key, EpochThread epoch, linkcache_t* buffer) {
	volatile node_t* successors[max_level];
	svalue_t result = 0;

	//fprintf(stderr, "in rmove\n");
	EpochStart(epoch);
	int found = sl_search_no_cleanup_succs(sl, key, successors);

	//fprintf(stderr, "in rmove 2\n");
	if (!found) {
#ifdef BUFFERING_ON
		cache_scan(buffer, key);
#endif
		EpochEnd(epoch);
		return 0;
	}

	volatile node_t* node_to_delete = successors[0];
	int delete_successful = mark_node_pointers(node_to_delete, buffer);

	if (delete_successful) {
		//fprintf(stderr, "delete successful\n");
		//make sure marks are persisted
		//write_data_wait((void*)node_to_delete, CACHE_LINES_PER_NV_NODE);
		//no need for the above; we either make sure the bottom unlink is already visible, or it is already in the flushbuffer, which will be emptied when the epoch change occurs at the latest
		result = node_to_delete->value;
		sl_search(sl, key, NULL, NULL, buffer, epoch);
#ifdef SIMULATE_NAIVE_IMPLEMENTATION
		write_data_wait(&(node_to_delete), 1);
#endif
		EpochReclaimObject(epoch, (node_t*) node_to_delete, NULL, NULL, sl_finalize_node);
	}
	EpochEnd(epoch);
	return result;

}

int skiplist_insert(skiplist_t* sl, skey_t key, svalue_t val, EpochThread epoch, linkcache_t* buffer) {
	volatile node_t* to_insert;
	volatile node_t* pred;
	volatile node_t* succ;

	volatile node_t* succs[max_level];
	volatile node_t* preds[max_level];

	UINT32 i;
	int found;

	EpochStart(epoch);
retry:
	found = sl_search_no_cleanup(sl, key, preds, succs);

	if (found) {
#ifdef BUFFERING_ON
		//need to make sure this is persisted
		cache_scan(buffer, key);
#else
		flush_and_try_unflag((PVOID*)&(preds[0]->next));
#endif
		EpochEnd(epoch);
		return 0;
	}

	to_insert = new_node(key, val, get_random_level(), epoch);

	for (i = 0; i < to_insert->toplevel; i++) {
		to_insert->next[i] = succs[i];
	}
	write_data_wait((void*)to_insert, CACHE_LINES_PER_NV_NODE); //we persist the newly allocated data in new_node; 

#ifdef BUFFERING_ON
	if (cache_try_link_and_add(buffer, key,(volatile void**) &preds[0]->next[0], UNMARKED_PTR(succs[0]), to_insert) == 0) {
		sl_finalize_node((void*)to_insert, NULL, NULL);
		goto retry;
	}
	//don't need to 100% make sure anything is persisted here
	//if a read will happen, the buffer will be flushed
	//and the recovery procedure can handle links to nodes missing
#else
	if ((node_t*)link_and_persist((PVOID*)&(preds[0]->next[0]), (PVOID)UNMARKED_PTR(succs[0]), (PVOID)to_insert) != UNMARKED_PTR(succs[0])) {
//if (CAS_PTR((PVOID*)&preds[0]->next[0], UNMARKED_PTR(succs[0]), (PVOID)to_insert) != UNMARKED_PTR(succs[0])) {
	//failed to insert the node, so I can actually free it if I want 
	//there's no chance anyone has a reference to it, right? so I can just call the finalize on it directly
	//EpochReclaimObject(epoch, (void*) to_insert, NULL, NULL, finalize_node);
	sl_finalize_node((void*)to_insert, NULL, NULL);
	goto retry;
}
//write_data_wait((void*)&(preds[0]->next[0]), 1);
#endif

volatile node_t* new_next;

for (i = 1; i < to_insert->toplevel; i++) {
	while (1) {
		pred = preds[i];
		succ = succs[i];

        new_next = to_insert->next[i];
		if (PTR_IS_MARKED(new_next)) {
			EpochEnd(epoch);
			return 1;
		}

        //tentative fix for problematic case in the original fraser algorithm
        if ((succ != new_next) && (CAS_PTR(&(to_insert->next[i]), new_next, succ)!= new_next)) {
            EpochEnd(epoch);
            return 1;
        }

		if (CAS_PTR((PVOID*)&pred->next[i], (PVOID) succ, (PVOID)to_insert) == succ) {
#ifdef SIMULATE_NAIVE_IMPLEMENTATION
			write_data_wait((void*)&(pred->next[i]), 1);
#endif
			break;
		}
		sl_search(sl, key, preds, succs, buffer, epoch);
	}
}
EpochEnd(epoch);
return 1;
}

//simple search, no cleanup;
static node_t* sl_left_search(skiplist_t* sl, skey_t key, linkcache_t* buffer) {
	node_t * left = NULL;
	volatile node_t* left_prev;

	left_prev = UNMARKED_PTR_ALL(*sl);

	int level;
	for (level = max_level - 1; level >= 0; level--) {
		left = UNMARKED_PTR_ALL(left_prev->next[level]);
		while (left->key < key || PTR_IS_MARKED(left->next[level])) {
			if (!PTR_IS_MARKED(left->next[level])) {
				left_prev = left;
			}
			left = UNMARKED_PTR_ALL(left->next[level]);
		}
		if (left->key == key) {
			break;
		}
	}
#ifdef BUFFERING_ON
	//even if key is not found, we still need to scan; a change deleting a node with the serached key might be buffered
	if (buffer != NULL) {
		cache_scan(buffer, key);
	}
#else
	flush_and_try_unflag((PVOID*)&(left->next[0]));
#endif
	return left;
}

svalue_t skiplist_find(skiplist_t* sl, skey_t key, EpochThread epoch, linkcache_t* buffer) {

	svalue_t result = 0;

	EpochStart(epoch);

	node_t* left = sl_left_search(sl, key, buffer);

	if (left->key == key) {
		result = left->value;
	}

	EpochEnd(epoch);

	return result;
}

int is_reachable(skiplist_t* sl, void* address) {
	//by the point we call is_reachable, we assume the skip-list has been brought to a consistent state
	//hence, we can do searches with logarithmic complexity instead of traversing the bottom layer 

	skey_t key = ((node_t*)address)->key;
	node_t* left = sl_left_search(sl, key, NULL);

	if ((left->key == key) && ((void*)left == address)) {
		return 1;
	}
	return 0;

	/*
	volatile node_t* node = UNMARKED_PTR((*sl)->next[0]);
	while (node->next[0] != NULL) {
		if ((void*)node == address) {
			return 1;
		}
		node = UNMARKED_PTR(node->next[0]);
	}
	return 0;
	*/

}


void recover(skiplist_t* sl, active_page_table_t** page_buffers, int num_page_buffers) {
	node_t ** unlinking_address = (node_t**)EpochCacheAlignedAlloc(sizeof(node_t*));

	volatile node_t* node = UNMARKED_PTR((*sl)->next[0]);
	volatile node_t* next;
	int i;
	int level;


	//since nodes may take multiple cache lines, it is possible that nodes that are unlinked from the bottom layer
	//are linked in higher layers; (either at insertion, or when doing unlinking);
	//I need to remove those nodes

	while (node->next[0] != NULL) {
		next = UNMARKED_PTR(node->next[0]);
		node->node_flags = 1;
		node = next;
	}
	node->node_flags = 1;


	//remove all the links to nodes not present in the first level
	//also, if nodes have missing links, adjust the level
	for (level = 1; level < max_level; level++) {
		node = *sl;
		while (node->next[level] != NULL) {
			next = UNMARKED_PTR(node->next[level]);
			if (next->node_flags != level) {
				//if a node linked at a level wasn't linked at all levels below, remove links
				if (next->toplevel > (next->node_flags)) {
					next->toplevel = next->node_flags ;
				}
				node->next[level] = next->next[level];
				//make sure that the nodes are correctly persisted
				write_data_nowait((void*)node, CACHE_LINES_PER_NV_NODE);
			}
			else {
				next->node_flags = level+1;
				node = next;
			}
		}
	}
	//these nodes will be in the pages that were recently touched and will be removed when testing for reachability



	// make sure all the nodes that are marked at the bottom layer are unlinked and freed from the data structure
	// if the pointers are marked this means I was in the process of deleting the node
	// but since the bottom layer is not unlinked, the delete did not finish
	// if I don't unlink now from every level, it will be unlinked at various levels of the skiplist
	// as they are traversed; but then I won't know when I can free the node; so I need to do this here.
	node = UNMARKED_PTR((*sl)->next[0]);

	while (node->next[0] != NULL) {
		//unset the marks we previously set
		node->node_flags = 0;


		next = UNMARKED_PTR(node->next[0]);

		if (PTR_IS_MARKED(node->next[0])) {
			// make sure I mark the pointers at every level
			// I don't impose any ordering on the persistence of marking of the pointers;
			// so some levels may be marked, while others are not
			// if the bottom level is not marked, I can continue even if
			// other levels are marked and will be unliked - those are needed just for routing;
			// but if the bottom level is marked, I need to make sure the higher ones are marked as well
			// such that I can remove all references to the node before I free it
			mark_node_pointers(node, NULL);
			//need to mark the page as containing a node about to be unlinked
			*unlinking_address = (node_t*) node;
			// need to make sure the node I am unlinking is persisted before I free it 
			// (there shouldn't be many ongoing deletes at any point, so there's no point in creating a page buffer just for this)
			write_data_wait(unlinking_address, 1);
			//unlink the node
			sl_search(sl, node->key, NULL, NULL, NULL, NULL);
			//make sure that the nodes are correctly persisted
			//write_data_nowait((void*)node, CACHE_LINES_PER_NV_NODE);
			//no concurrency, can just delete the node;
			//no need to go through the epoch-based reclamation scheme	
			if (!NodeMemoryIsFree((void*)node)) {
				sl_finalize_node((void*)node, NULL, NULL);
			}
		}
		node = next;
	}
	wait_writes();
	EpochCacheAlignedFree(unlinking_address);

	
	// now go over all the pages in the page buffers and check which of the nodes there are reachable;
	size_t j;
	size_t k;
	size_t page_size;
	size_t nodes_per_page;

	page_descriptor_t* crt;
    size_t num_pages;

    for (i = 0; i < num_page_buffers; i++) {
        page_size = page_buffers[i]->page_size; //TODO: now assuming all the pages in the buffer have one size; change this? (given that in the NV heap we basically just use one page size (except the bottom level), should be fine)
        num_pages = page_buffers[i]->last_in_use;
        crt = page_buffers[i]->pages;
        for (j = 0; j < num_pages; j++) {
            if (crt[j].page != NULL) {
                void * crt_address = crt[j].page;
                nodes_per_page = page_size / sizeof(node_t);
                for (k = 0; k < nodes_per_page; k++) {
                    void * node_address = (void*)((UINT_PTR)crt_address + (CACHE_LINES_PER_NV_NODE*CACHE_LINE_SIZE*k));
                    if (!NodeMemoryIsFree(node_address)) {
                        if (!is_reachable(sl, node_address)) {
                            //if (NodeMemoryIsFree(node_address)) {
                            //this should never happen in this structure - since we remove marked nodes just before
                            //	fprintf(stderr, "error: reachable node whose memory was free\n");
                            //MarkNodeMemoryAsAllocated(node_address); //if a node is reachable but its memory is marked as free, need to mark that memory as allocated
                            //}
                            //}
                            //else {
                            //	if (!NodeMemoryIsFree(node_address)) {
                            MarkNodeMemoryAsFree(node_address); //if a node is not reachable but its memory is marked as allocated, need to free the node
                        //	}
                    }
                    }
                }
            }
        }
        destroy_active_page_table(page_buffers[i]);
        }
}
