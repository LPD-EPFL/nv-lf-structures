#include "lf-linkedlist.h"

const int linkedlist_node_size = sizeof(node_t);

//#define DO_DEBUG 1

// void finalize_node(void * node, void * context, void* tls) {
// 	EpochFreeNode(node);
// }

// static inline UINT_PTR unmarked_ptr(UINT_PTR p) {
// 	return(p & ~(UINT_PTR)0x01);
// }

// #define UNMARKED_PTR(p) (node_t*)unmarked_ptr((UINT_PTR) p)

// static inline UINT_PTR marked_ptr(UINT_PTR p) {
// 	return (p | (UINT_PTR)0x01);
// }

// #define MARKED_PTR(p) (node_t*)marked_ptr((UINT_PTR) p)

// static inline int ptr_is_marked(UINT_PTR p) {
// 	return (int)(p & (UINT_PTR)0x01);
// }

// #define PTR_IS_MARKED(p) ptr_is_marked((UINT_PTR) p)

linkedlist_t* new_linkedlist(EpochThread epoch) {
	//TODO: in persistent memory, this might have to be done differently;
	// this method shoulsd create a named entry which can be accessed from
	// persistent memory using only this names
	linkedlist_t* ll = (linkedlist_t*)malloc(sizeof(linkedlist_t));
	*ll = NULL;

	volatile node_t* max = new_node_and_set_next(MAX_KEY, 0, NULL, epoch);
	volatile node_t* min = new_node_and_set_next(MIN_KEY, 0, max, epoch);
	(*ll) = min;
	return ll;
}

void
bucket_set_init(linkedlist_t* set, EpochThread epoch)
{
	EpochStart(epoch);
	volatile node_t* max = new_node_and_set_next(MAX_KEY, 0, NULL, epoch);
	write_data_wait((void*)max, CACHE_LINES_PER_NV_NODE);
	volatile node_t* min = new_node_and_set_next(MIN_KEY, 0, max, epoch);
	write_data_wait((void*)min, CACHE_LINES_PER_NV_NODE);
	(*set) = min;
	write_data_wait((void*)set, 1);
    EpochEnd(epoch);
}

void delete_linkedlist(linkedlist_t* ll) {
	finalize_node((void*)(*ll)->next, NULL, NULL);
	finalize_node((void*)(*ll), NULL, NULL);
	free(ll);
}


volatile node_t* new_node(skey_t key, svalue_t value, EpochThread epoch) {
	volatile node_t* the_node;
	the_node = (node_t*)EpochAllocNode(epoch, sizeof(node_t));
	the_node->key = key;
	the_node->value = value;
	the_node->next = NULL;
    _mm_sfence();
	return the_node;
}


volatile node_t* new_node_and_set_next(skey_t key, svalue_t value, volatile node_t* next, EpochThread epoch) {
	volatile node_t* the_node;
	the_node = (node_t*)EpochAllocNode(epoch, sizeof(node_t));
	the_node->key = key;
	the_node->value = value;
    next = (node_t*)unmark_ptr_cache((uintptr_t)(next));
	the_node->next = next;
	write_data_wait((void*)the_node, CACHE_LINES_PER_NV_NODE);
	_mm_sfence();
	return the_node;
}

static inline int delete_right(volatile node_t* left, volatile node_t* right, EpochThread epoch, linkcache_t* buffer) {
	volatile node_t* nnext = UNMARKED_PTR(right->next);
    nnext = (node_t*)unmark_ptr_cache((uintptr_t)(nnext));

	EpochDeclareUnlinkNode(epoch, (void*)right, linkedlist_node_size);
#ifdef BUFFERING_ON
	int success;
	if (buffer != NULL) {
		success = cache_try_link_and_add(buffer, right->key, (volatile void**) &(left->next), right, nnext);
	}
	else {
		node_t* res = (node_t*)CAS_PTR((PVOID*)&(left->next), (PVOID) right, (PVOID)nnext);
		success = (res == right);
		if (success) {
			write_data_wait((void*)&(left->next), 1); //has to be done before the epoch ends
		}
	}
#else

	node_t* res = (node_t*)link_and_persist((PVOID*)&(left->next), (PVOID)right, (PVOID)nnext);

	//node_t* res = (node_t*)CAS_PTR((PVOID*)&(left->next), (PVOID)right, (PVOID)nnext);
	int success = (res == right);
	//if (success) {
	//	write_data_wait((void*)&(left->next), 1); //has to be done before the epoch ends
	//}
#endif
	//no need to buffer anything here;
	//if it's unlinked but the unlink is not persisted
	// either (1) it's still not been reclaimed, in which case its page will be searched for reachability or
	// (2) it's been reclaimed, 

	if (success) {
		EpochReclaimObject(epoch, (node_t*)right, NULL, NULL, finalize_node);
	}
	return success;
}

static inline volatile node_t* search(linkedlist_t* ll, skey_t key, volatile node_t** left_ptr, EpochThread epoch, linkcache_t* buffer) {
	volatile node_t* left = *ll;
	volatile node_t* right = (node_t*)unmark_ptr_cache((uintptr_t)(*ll)->next);
	while (1) {
		if (!PTR_IS_MARKED(right->next)) {
			if (right->key >= key) {
				break;
			}
			left = right;
		}
		else {
			delete_right(left, right, epoch, buffer);
		}
		right = UNMARKED_PTR(right->next);
		right = (volatile node_t*) unmark_ptr_cache((UINT_PTR)right);
	}
	*left_ptr = left;
	return right;
}

int linkedlist_size(linkedlist_t* ll) {
	int size = 0;

    flush_and_try_unflag((PVOID*)&((*ll)->next));
	volatile node_t* node = (node_t*)unmark_ptr_cache((uintptr_t)(*ll)->next);
	UNMARKED_PTR(node);

	while (node->next != NULL) {
		if (!PTR_IS_MARKED(node->next)) {
			size++;
		}
        flush_and_try_unflag((PVOID*)&(node->next));
		node = UNMARKED_PTR(node->next);
	}

	return size;
}


svalue_t linkedlist_remove(linkedlist_t* ll, skey_t key, EpochThread epoch, linkcache_t* buffer) {
	node_t* res = NULL;
	node_t* unmarked;
	volatile node_t* left;
	volatile node_t* right;
	EpochStart(epoch);
	do {
		right = search(ll, key, &left, epoch, buffer);

		if (right->key != key) {
#ifdef BUFFERING_ON
            cache_scan(buffer, key);
#else
			flush_and_try_unflag((PVOID*)&(left->next));
#endif
			EpochEnd(epoch);
			return 0;
		}

		unmarked = UNMARKED_PTR(right->next);
		node_t* marked = MARKED_PTR(unmarked);
#ifdef BUFFERING_ON
		if (buffer != NULL) {
			int success = cache_try_link_and_add(buffer, right->key, (volatile void**)&(right->next), unmarked, marked);
			if (success) {
                res = unmarked;
				break;
			} else {
                res = marked;
            }
		} else {
			//this branch taken on recovery, no need to care about concurrency
			res = (node_t*)CAS_PTR((PVOID*)&(right->next), unmarked, marked);
			if (res == unmarked) {
				write_data_wait((void*)&(right->next), 1);
			}
		}
#else 
	
		res = (node_t*)link_and_persist((PVOID*)&(right->next), unmarked, marked);

		//res = (node_t*)CAS_PTR((PVOID*)&(right->next), unmarked, marked);
		//if (res == unmarked) {
		//	write_data_wait((void*)&(right->next), 1);
		//}

#endif
	} while (res != unmarked);

	svalue_t val = right->value;
	
	delete_right(left, right, epoch, buffer);

	EpochEnd(epoch);
	return val;
}




int linkedlist_insert(linkedlist_t* ll, skey_t key, svalue_t val, EpochThread epoch, linkcache_t* buffer) {
	EpochStart(epoch);
	do {
		volatile node_t* left;
		volatile node_t* right = search(ll,key,&left, epoch, buffer);

		if (right->key == key) {
#ifdef BUFFERING_ON
			cache_scan(buffer, key);
#else
			flush_and_try_unflag((PVOID*)&(left->next));
#endif
			EpochEnd(epoch);
			return 0;
		}

		volatile node_t* to_add = new_node_and_set_next(key, val, right, epoch);  //we persist the newly allocated data in new_node (done so in the call); 

#ifdef BUFFERING_ON
		if (cache_try_link_and_add(buffer, key, (volatile void**)&(left->next), right, to_add)) {
			EpochEnd(epoch);
			return 1;
		}
#else
		if ((node_t*)link_and_persist((PVOID*)&(left->next), (PVOID)right, (PVOID)to_add) == right) {
			EpochEnd(epoch);
			return 1;
		}
#endif

		finalize_node((void*)to_add, NULL, NULL);

	} while (1);
}



svalue_t linkedlist_find(linkedlist_t* ll, skey_t key, EpochThread epoch, linkcache_t* buffer) {

	EpochStart(epoch);
	volatile node_t* prev = (*ll);
	volatile node_t* node = (node_t*)unmark_ptr_cache((uintptr_t)(*ll)->next);
	
	while (node->key < key) {
		prev = node;
		node = UNMARKED_PTR(node->next);
		node= (volatile node_t*)unmark_ptr_cache((UINT_PTR)node);
	}

	if ((node->key == key) && (!PTR_IS_MARKED(node->next))) {
#ifdef BUFFERING_ON
        cache_scan(buffer, key);
#else
		flush_and_try_unflag((PVOID*)&(prev->next));
		flush_and_try_unflag((PVOID*)&(node->next));
#endif
		EpochEnd(epoch);
		return node->value;
	}
#ifdef BUFFERING_ON
    cache_scan(buffer, key);
#else
	flush_and_try_unflag((PVOID*)&(prev->next));
	flush_and_try_unflag((PVOID*)&(node->next));
#endif
	EpochEnd(epoch);

	return 0;
}


// int is_reachable(linkedlist_t* ll, void* address) {
// 	volatile node_t* node = UNMARKED_PTR((*ll)->next);
// 	while (node->next != NULL) {
// 		if ((void*)node == address) {
// 			return 1;
// 		}
// 		node = UNMARKED_PTR(node->next);
// 	}
// 	return 0;
// }


// void recover(linkedlist_t* ll, linkcache_t* buffer, active_page_table_t** page_buffers, int num_page_buffers) {

// 	node_t ** unlinking_address = (node_t**)EpochCacheAlignedAlloc(sizeof(node_t*));

// 	volatile node_t* prev = UNMARKED_PTR((*ll));
// 	volatile node_t* node = UNMARKED_PTR((*ll)->next);
// 	volatile node_t* next;
// 	int i;

// 	//remove the marked nodes
// 	while (node->next != NULL) {

// 		next = UNMARKED_PTR(node->next);

// 		if (PTR_IS_MARKED(node->next)) {
// 			*unlinking_address = (node_t*)node;
// 			write_data_wait(unlinking_address, 1);
// 			prev->next = next;
// 			write_data_wait((void*)prev, CACHE_LINES_PER_NV_NODE);	
// 			if (!NodeMemoryIsFree((void*)node)) {
// 				finalize_node((void*)node, NULL, NULL);
// 			}
// 			node = prev->next;
// 		}
// 		else {
// 			prev = node;
// 			node = next;
// 		}
// 	}
// 	wait_writes();
// 	EpochCacheAlignedFree(unlinking_address);

// 	// now go over all the pages in the page buffers and check which of the nodes there are reachable;

// 	size_t j;
// 	size_t k;
// 	size_t page_size;
// 	size_t nodes_per_page;

// 	page_descriptor_t* crt;
//     size_t num_pages;

// 	for (i = 0; i < num_page_buffers; i++) {
// 		page_size = page_buffers[i]->page_size; //TODO: now assuming all the pages in the buffer have one size; change this? (given that in the NV heap we basically just use one page size (except the bottom level), should be fine)
//         num_pages = page_buffers[i]->last_in_use;
//         crt = page_buffers[i]->pages;
//         for (j = 0; j < num_pages; j++) {
// 				if (crt[j].page != NULL) {
// 					void * crt_address = crt[j].page;
// 					nodes_per_page = page_size / sizeof(node_t);
// 					for (k = 0; k < nodes_per_page; k++) {
// 						void * node_address = (void*)((UINT_PTR)crt_address + (sizeof(node_t)*k));
// 						if (!NodeMemoryIsFree(node_address)) {
// 							if (!is_reachable(ll, node_address)) {
// 				//				if (NodeMemoryIsFree(node_address)) {
// 									//this should never happen in this structure - since we remove marked nodes
// 				//					fprintf(stderr, "error: reachable node whose memory was free\n");
// 									//MarkNodeMemoryAsAllocated(node_address); //if a node is reachable but its memory is marked as free, need to mark that memory as allocated
// 				//				}
// 			//				}
// 				//			else {
// 				//				if (!NodeMemoryIsFree(node_address)) {
// 									MarkNodeMemoryAsFree(node_address); //if a node is not reachable but its memory is marked as allocated, need to free the node
// 				//				}
// 							}
// 						}
// 					}
// 				}
// 			}
// 		destroy_active_page_table(page_buffers[i]);
// 	}
// }
