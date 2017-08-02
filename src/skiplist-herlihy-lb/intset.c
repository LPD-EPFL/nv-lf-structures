#include "intset.h"
#include "utils.h"

svalue_t sl_contains(sl_intset_t *set, skey_t key, EpochThread epoch)
{
  EpochStart(epoch);
  svalue_t val = optimistic_find(set, key, epoch);
  EpochEnd(epoch);
  return val;
}

int sl_add(sl_intset_t *set, skey_t key, svalue_t val, EpochThread epoch)
{  
  EpochStart(epoch);
  int ret = optimistic_insert(set, key, val, epoch);
  EpochEnd(epoch);
  return ret;

}

svalue_t sl_remove(sl_intset_t *set, skey_t key, EpochThread epoch)
{
  EpochStart(epoch);
  svalue_t val =  optimistic_delete(set, key, epoch);
  EpochEnd(epoch);
  return val;
}
