#include "intset.h"
#include "utils.h"

__thread thread_log_t* my_log;

node_l_t* new_node_l(skey_t key, svalue_t val, EpochThread epoch){
  volatile node_l_t *the_node;
	the_node = (node_l_t*)EpochAllocNode(epoch, sizeof(node_l_t));
	the_node->key = key;
	the_node->val = val;
	the_node->next = NULL;
    the_node->marked = 0;
    _mm_sfence();
    return the_node;
}

node_l_t*
new_node_and_set_next_l(skey_t key, svalue_t val, node_l_t* next, int initializing, EpochThread epoch)
{
  volatile node_l_t *the_node;
  the_node = (node_l_t*)EpochAllocNode(epoch, sizeof(node_l_t));
  if (the_node == NULL) 
    {
      perror("malloc @ new_node");
      exit(1);
    }

  the_node->key = key;
  the_node->val = val;
  the_node->next = next;
  the_node->marked = 0;

  INIT_LOCK(ND_GET_LOCK(the_node));

 write_data_wait((void*)the_node, CACHE_LINES_PER_NV_NODE);
  return (node_l_t*) the_node;
}

intset_l_t *set_new_l(EpochThread epoch)
{
  intset_l_t *set;
  node_l_t *min, *max;

  if ((set = (intset_l_t *)malloc(sizeof(intset_l_t))) == NULL) 
    {
      perror("malloc");
      exit(1);
    }

  max = new_node_and_set_next_l(KEY_MAX, 0, NULL, 1, epoch);
  /* ssalloc_align_alloc(0); */
  min = new_node_and_set_next_l(KEY_MIN, 0, max, 1, epoch);
  set->head = min;

#if defined(LL_GLOBAL_LOCK)
  set->lock = (volatile ptlock_t*) malloc(sizeof(ptlock_t));
  if (set->lock == NULL)
    {
      perror("malloc");
      exit(1);
    }
  GL_INIT_LOCK(set->lock);
#endif

  MEM_BARRIER;
  return set;
}


void
bucket_set_init(intset_l_t* set, ptlock_t* lock, EpochThread epoch)
{
	EpochStart(epoch);
	volatile node_l_t* max = new_node_and_set_next_l(KEY_MAX, 0, NULL, 1,  epoch);
	write_data_wait((void*)max, CACHE_LINES_PER_NV_NODE);
	volatile node_l_t* min = new_node_and_set_next_l(KEY_MIN, 0, max, 1,  epoch);
	write_data_wait((void*)min, CACHE_LINES_PER_NV_NODE);

  set->head = min;

#if defined(LL_GLOBAL_LOCK)
  set->lock = lock;
  GL_INIT_LOCK(set->lock);
#endif

	write_data_wait((void*)set, 1);
  MEM_BARRIER;
    EpochEnd(epoch);
}

void
node_delete_l(node_l_t *node) 
{
  /*DESTROY_LOCK(&node->lock);*/
/*#if GC == 1*/
  /*ssfree((void*) node);*/
/*#endif*/
}

void set_delete_l(intset_l_t *set)
{
  /*node_l_t *node, *next;*/

  /*node = set->head;*/
  /*while (node != NULL) */
    /*{*/
      /*next = node->next;*/
      /*DESTROY_LOCK(&node->lock);*/
      /*[> free(node); <]*/
      /*ssfree((void*) node);		[> TODO : fix with ssmem <]*/
      /*node = next;*/
    /*}*/
  /*ssfree(set);*/
}

int set_size_l(intset_l_t *set)
{
  int size = 0;
  node_l_t *node;

  /* We have at least 2 elements */
  node = set->head->next;
  while (node->next != NULL) 
    {
      size++;
      node = node->next;
    }

  return size;
}



	
