
#include "hashtable.h"

svalue_t ht_contains(ht_intset_t* set, skey_t key, EpochThread epoch, linkcache_t* buffer);
int ht_add(ht_intset_t* set, skey_t key, svalue_t val, EpochThread epoch, linkcache_t* buffer);
svalue_t ht_remove(ht_intset_t* set, skey_t key, EpochThread epoch, linkcache_t* buffer);

int is_reachable(ht_intset_t* ll, void* address);
void recover(ht_intset_t* ll, active_page_table_t** page_buffers, int num_page_buffers);
