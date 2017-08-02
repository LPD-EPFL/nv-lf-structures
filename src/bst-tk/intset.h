#include "bst.h"

svalue_t set_contains(intset_t *set, skey_t key, EpochThread epoch);
int set_add(intset_t *set, skey_t key, svalue_t val, EpochThread epoch);
svalue_t set_remove(intset_t *set, skey_t key, EpochThread epoch);
