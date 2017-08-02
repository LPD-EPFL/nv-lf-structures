#include "bst.h"

svalue_t set_contains(intset_t *set, skey_t key, EpochTHread epoch);
int set_add(intset_t *set, skey_t key, svalue_t val, EpochTHread epoch);
svalue_t set_remove(intset_t *set, skey_t key, EpochTHread epoch);
