
#include "lazy.h"
#include "utils.h"

svalue_t
set_contains_l(intset_l_t* set, skey_t key, EpochThread epoch)
{
    EpochStart(epoch);
    svalue_t val = parse_find(set, key, epoch);
    EpochEnd(epoch);
    return val;
}

int
set_add_l(intset_l_t* set, skey_t key, svalue_t val, EpochThread epoch)
{  
    int res;
    EpochStart(epoch);
    res =  parse_insert(set, key, val, epoch);
    EpochEnd(epoch);
    return res;
}

svalue_t
set_remove_l(intset_l_t* set, skey_t key, EpochThread epoch)
{
    svalue_t val;
    EpochStart(epoch);
    val = parse_delete(set, key, epoch);
    EpochEnd(epoch);
    return val;
    
}
