#include "herlihy.h"

svalue_t sl_contains(sl_intset_t *set, skey_t key, EpochThread epoch);
int sl_add(sl_intset_t *set, skey_t key, svalue_t val, EpochThread epoch);
svalue_t sl_remove(sl_intset_t *set, skey_t key, EpochThread epoch);
