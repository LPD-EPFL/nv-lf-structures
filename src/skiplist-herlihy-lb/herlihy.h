#include "skiplist-lock.h"

svalue_t optimistic_find(sl_intset_t *set, skey_t key, EpochThread epoch);
int optimistic_insert(sl_intset_t *set, skey_t key, svalue_t val, EpochThread epoch);
svalue_t optimistic_delete(sl_intset_t *set, skey_t key, EpochThread epoch);
