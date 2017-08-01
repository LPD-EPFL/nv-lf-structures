#ifndef _H_LAZY_
#define _H_LAZY_
#include "linkedlist-lock.h"


#define LAZY_RO_FAIL RO_FAIL


/* linked list accesses */
extern int parse_validate(node_l_t* pred, node_l_t* curr, EpochThread epoch);
svalue_t parse_find(intset_l_t* set, skey_t key, EpochThread epoch);
int parse_insert(intset_l_t* set, skey_t key, svalue_t val, EpochThread epoch);
svalue_t parse_delete(intset_l_t* set, skey_t key, EpochThread epoch);

#endif	/* _H_LAZY_ */
