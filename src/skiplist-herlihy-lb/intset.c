#include "intset.h"
#include "utils.h"

inline svalue_t
sl_contains(sl_intset_t *set, skey_t key, EpochThread epoch)
{
  EpochStart(epoch);
  return optimistic_find(set, key);
  EpochEnd(epoch);
}

inline int
sl_add(sl_intset_t *set, skey_t key, svalue_t val, EpochThread epoch)
{  
  EpochStart(epoch);
  return optimistic_insert(set, key, val);
  EpochEnd(epoch);

}

inline svalue_t
sl_remove(sl_intset_t *set, skey_t key, EpochThread epoch)
{
  EpochStart(epoch);
  return optimistic_delete(set, key);
  EpochEnd(epoch);
}
