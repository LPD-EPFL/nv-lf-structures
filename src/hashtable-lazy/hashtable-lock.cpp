#include "hashtable-lock.h"

unsigned int maxhtlength;

void ht_delete(ht_intset_t *set) 
{
  //volatile node_l_t *node, *next;
  //int i;
  
  //for (i=0; i < maxhtlength; i++) 
    //{
      //node = set->buckets[i]->head;
      //while (node != NULL) 
	//{
	  //next = node->next;
	  //free((void*) node);
	  //node = next;
	//}
    //}
  //free(set->buckets);
  //free(set);
}

int
ht_size(ht_intset_t *set) 
{
  int size = 0;
  int i;
	
  for (i = 0; i < maxhtlength; i++) 
    {
      size += set_size_l(&set->buckets[i]);
    }
  return size;
}

int floor_log_2(unsigned int n) {
  int pos = 0;
  printf("n result = %d\n", n);
  if (n >= 1<<16) { n >>= 16; pos += 16; }
  if (n >= 1<< 8) { n >>=  8; pos +=  8; }
  if (n >= 1<< 4) { n >>=  4; pos +=  4; }
  if (n >= 1<< 2) { n >>=  2; pos +=  2; }
  if (n >= 1<< 1) {           pos +=  1; }
  printf("floor result = %d\n", pos);
  return ((n == 0) ? (-1) : pos);
}

// ht_intset_t* 
// ht_new(EpochThread epoch) 
// {
//   ht_intset_t *set;
//   int i;
	
//   if ((set = (ht_intset_t *)malloc(sizeof(ht_intset_t))) == NULL)
//     {
//       perror("malloc");
//       exit(1);
//     }  

//   set->hash = maxhtlength - 1;

//   size_t bs = (maxhtlength + 1) * sizeof(linkedlist_t);
//   bs += CACHE_LINE_SIZE - (bs & CACHE_LINE_SIZE);
//   if ((set->buckets = (linkedlist_t*)malloc(bs)) == NULL)
//     {
//       perror("malloc");
//       exit(1);
//     }  

//   for (i = 0; i < maxhtlength; i++) 
//     {
//       bucket_set_init(&set->buckets[i], epoch);
//     }
//   return set;
// }


ht_intset_t* 
ht_new(EpochThread epoch) 
{
  PMEMobjpool *pop = NULL;

  char path[32];  
  char *uname = cuserid(NULL);

#if defined(LL_GLOBAL_LOCK)
  size_t ls = (maxhtlength + 1) * sizeof(ptlock_t);
  ls += CACHE_LINE_SIZE - (ls & CACHE_LINE_SIZE);
  if ((set->locks = malloc(ls)) == NULL)
    {
      perror("malloc locks");
      exit(1);
    }  

#endif
  sprintf(path, "/tmp/ht_lb_pool_%s",uname); 

  //remove file if it exists
  //TODO might want to remove this instruction in the future
  remove(path);

  if (access(path, F_OK) != 0) {
        if ((pop = pmemobj_create(path, POBJ_LAYOUT_NAME(ht),
            HT_POOL_SIZE, S_IWUSR | S_IRUSR)) == NULL) {
            printf("failed to create pool1 wiht name %s\n", path);
            return NULL;
        }
  } else {
      if ((pop = pmemobj_open(path, POBJ_LAYOUT_NAME(ht))) == NULL) {
          printf("failed to open pool with name %s\n", path);
          return NULL;
      }
  }

  TOID(ht_intset_t) root = POBJ_ROOT(pop, ht_intset_t);

  size_t bs = (maxhtlength + 1) * sizeof(intset_l_t);
  bs += CACHE_LINE_SIZE - (bs & CACHE_LINE_SIZE);

  ht_intset_t* set = NULL;

  TX_BEGIN(pop) {
    TX_ADD(root);
    set = D_RW(root);
    set->hash = maxhtlength - 1;
    set->buckets = D_RW(TX_ALLOC(intset_l_t, bs));

  } TX_ONCOMMIT {
    int i;
    for (i = 0; i < maxhtlength; i++) {
#if defined(LL_GLOBAL_LOCK)
      ptlock_t* l = &set->locks[i];
#else
      ptlock_t* l = NULL;
#endif
      bucket_set_init(&set->buckets[i], l, epoch);
    }
    return set;

  } TX_ONABORT {
    return NULL;

  } TX_END

  return set;

}

svalue_t
ht_contains(ht_intset_t *set, skey_t key, EpochThread epoch) 
{
  int addr = key & set->hash;
  return set_contains_l(&set->buckets[addr], key, epoch);
}

int
ht_add(ht_intset_t *set, skey_t key, svalue_t val, EpochThread epoch) 
{
  int addr = key & set->hash;
  return set_add_l(&set->buckets[addr], key, val, epoch);
}

svalue_t
ht_remove(ht_intset_t *set, skey_t key, EpochThread epoch) 
{
  int addr = key & set->hash;
  return set_remove_l(&set->buckets[addr], key, epoch);
}
