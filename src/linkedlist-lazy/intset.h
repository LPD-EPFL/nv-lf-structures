
#include "lazy.h"

svalue_t set_contains_l(intset_l_t *set, skey_t key, EpochThread epoch);
int set_add_l(intset_l_t *set, skey_t key, svalue_t val, EpochThread epoch);
svalue_t set_remove_l(intset_l_t *set, skey_t key, EpochThread epoch);
