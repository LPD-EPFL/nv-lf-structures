
#include "bst.h"

svalue_t bst_tk_delete(intset_t* set, skey_t key, EpochThread epoch);
svalue_t bst_tk_find(intset_t* set, skey_t key, EpochThread epoch);
int bst_tk_insert(intset_t* set, skey_t key, svalue_t val, EpochThread epoch);
