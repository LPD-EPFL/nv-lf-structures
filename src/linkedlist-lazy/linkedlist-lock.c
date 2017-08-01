#include "intset.h"
#include "utils.h"

void finalize_node(void * node, void * context, void* tls) {
	EpochFreeNode(node);
}

node_l_t* new_node_l(skey_t key, svalue_t val, EpochThread epoch){
  volatile node_l_t *the_node;
	the_node = (node_t*)EpochAllocNode(epoch, sizeof(node_l_t));
	the_node->key = key;
	the_node->value = value;
	the_node->next = NULL;
    the_node->marked = 0;
    _mm_sfence();
    return the_node;
}

node_l_t*
new_node_l(skey_t key, sval_t val, node_l_t* next, int initializing, EpochThread epoch)
{
  volatile node_l_t *node;
  the_node = (node_t*)EpochAllocNode(epoch, sizeof(node_t));
  if (node == NULL) 
    {
      perror("malloc @ new_node");
      exit(1);
    }

  node->key = key;
  node->val = val;
  node->next = next;
  node->marked = 0;

  INIT_LOCK(ND_GET_LOCK(node));

 write_data_wait((void*)the_node, CACHE_LINES_PER_NV_NODE);
  return (node_l_t*) node;
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



	
