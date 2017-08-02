#include "bst.h"
#include "bst_tk.h"
#include "utils.h"

svalue_t
set_contains(intset_t* set, skey_t key, EpochThread epoch)
{
  EpochStart(epoch);
  svalue_t val =  bst_tk_find(set, key, epoch);
  EpochEnd(epoch);
  return val;
}

int
set_add(intset_t* set, skey_t key, svalue_t val, EpochThread epoch)
{  
  EpochStart(epoch);
  int ret =  bst_tk_insert(set, key, val, epoch);
  EpochEnd(epoch);
  return ret;
}

svalue_t
set_remove(intset_t* set, skey_t key, EpochThread epoch)
{
  EpochStart(epoch);
  svalue_t val =  bst_tk_delete(set, key, epoch);
  EpochEnd(epoch);
  return val;
}
