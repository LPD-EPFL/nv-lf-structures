#include "intset.h"
#include "utils.h"

__thread thread_log_t* my_log;

node_t*
new_node(skey_t key, svalue_t val, node_t* l, node_t* r, int initializing, EpochThread epoch)
{
  node_t* node;

	node = (node_t*)EpochAllocNode(epoch, sizeof(node_t));
  
  if (node == NULL) 
    {
      perror("malloc @ new_node");
      exit(1);
    }

  node->key = key;
  node->val = val;
  node->left = l;
  node->right = r;
  node->lock.to_uint64 = 0;

 write_data_wait((void*)node, CACHE_LINES_PER_NV_NODE);
  return (node_t*) node;
}

node_t*
new_node_no_init(EpochThread epoch)
{
  node_t* node;
  node = (node_t*)EpochAllocNode(epoch, sizeof(node_t));
  if (unlikely(node == NULL))
    {
      perror("malloc @ new_node");
      exit(1);
    }

  node->val = 0;
  node->lock.to_uint64 = 0;
    _mm_sfence();
  return (node_t*) node;
}



intset_t* set_new(EpochThread epoch)
{
  intset_t *set;

  if ((set = (intset_t *)malloc(sizeof(intset_t))) == NULL) 
    {
      perror("malloc");
      exit(1);
    }

 EpochStart(epoch);
  node_t* min = new_node(INT_MIN, 1, NULL, NULL, 1, epoch);
  node_t* max = new_node(INT_MAX, 1, NULL, NULL, 1, epoch);
  set->head = new_node(INT_MAX, 0, min, max, 1, epoch);
  MEM_BARRIER;
 EpochEnd(epoch);
  return set;
}

void
node_delete(node_t *node) 
{
/*#if GC == 1*/
  /*ssmem_free(alloc, node);*/
/*#else*/
  /*[> ssfree(node); <]*/
/*#endif*/
}

void
set_delete_l(intset_t *set)
{
  /* TODO: implement */
}

static int
node_size(node_t* n)
{
 if (n->leaf != 0)
    {
      return 1;
    }
  else
    {
      return node_size((node_t*) n->left) + node_size((node_t*) n->right);
    }
}

int 
set_size(intset_t* set)
{
  int size = node_size(set->head) - 2;
  return size;
}



	
