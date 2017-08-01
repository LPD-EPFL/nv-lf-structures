#include <libpmemobj.h>
#include <pwd.h>

#include "./lists/intset.h"
//#include "./lists/linkedlist-lock.h"

#define DEFAULT_MOVE                    0
#define DEFAULT_SNAPSHOT                0
#define DEFAULT_LOAD                    1
#define DEFAULT_ELASTICITY              4
#define DEFAULT_ALTERNATE               0
#define DEFAULT_EFFECTIVE               1

#define MAXHTLENGTH                     65536

#define HT_POOL_SIZE    (1024 * 1024 * 1024)

/* Hashtable length (# of buckets) */
extern unsigned int maxhtlength;

/* Hashtable seed */
#ifdef TLS
extern __thread unsigned int *rng_seed;
#else /* ! TLS */
extern pthread_key_t rng_seed_key;
#endif /* ! TLS */

typedef struct ht_intset 
{
  size_t hash;
  intset_l_t* buckets;
  ptlock_t* locks;
  uint8_t padding[CACHE_LINE_SIZE - 24];
} ht_intset_t;

void ht_delete(ht_intset_t *set);
int ht_size(ht_intset_t *set);
int floor_log_2(unsigned int n);
ht_intset_t *ht_new(EpochThread epoch);

svalue_t ht_contains(ht_intset_t* set, skey_t key, EpochThread epoch);
int ht_add(ht_intset_t* set, skey_t key, svalue_t val, EpochThread epoch);
svalue_t ht_remove(ht_intset_t* set, skey_t key, EpochThread epoch);

POBJ_LAYOUT_BEGIN(ht);
POBJ_LAYOUT_ROOT(ht, ht_intset_t);
POBJ_LAYOUT_TOID(ht, intset_l_t);
POBJ_LAYOUT_END(ht);
