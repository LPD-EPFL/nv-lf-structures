#include "bst.h"
#include "bst_tk.h"
#include "utils.h"

svalue_t
set_contains(intset_t* set, skey_t key)
{
  EpochStart(epoch);
  return bst_tk_find(set, key);
  EpochEnd(epoch);
}

int
set_add(intset_t* set, skey_t key, svalue_t val)
{  
  EpochStart(epoch);
  return bst_tk_insert(set, key, val);
  EpochEnd(epoch);
}

svalue_t
set_remove(intset_t* set, skey_t key)
{
  EpochStart(epoch);
  return bst_tk_delete(set, key);
  EpochEnd(epoch);
}
