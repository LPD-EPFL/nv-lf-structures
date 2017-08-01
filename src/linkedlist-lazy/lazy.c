
#include "lazy.h"

extern __thread thread_log_t* log;

/*
 * Checking that both curr and pred are both unmarked and that pred's next pointer
 * points to curr to verify that the entries are adjacent and present in the list.
 */
inline int
parse_validate(node_l_t* pred, node_l_t* curr) 
{
  return (!pred->marked && !curr->marked && (pred->next == curr));
}

svalue_t
parse_find(intset_l_t *set, skey_t key, EpochThread epoch)
{
  node_l_t* curr = set->head;
  while (curr->key < key)
    {
      curr = curr->next;
    }

  svalue_t res = 0;
  
  if ((curr->key == key) && !curr->marked)
    {
      res = curr->val;
    }
  
  return res;
}

int
parse_insert(intset_l_t *set, skey_t key, svalue_t val, EpochThread epoch)
{
  node_l_t *curr, *pred, *newnode;
  int result = -1;
	
  do
    {
      pred = set->head;
      curr = pred->next;
      while (likely(curr->key < key))
	{
	  pred = curr;
	  curr = curr->next;
	}


#if LAZY_RO_FAIL == 1 
      if (curr->key == key)
	{
	  if (unlikely(curr->marked))
	    {
	      continue;
	    }
	  return false;
	}
#endif

      GL_LOCK(set->lock);		/* when GL_[UN]LOCK is defined the [UN]LOCK is not ;-) */
      LOCK(ND_GET_LOCK(pred));

      if (parse_validate(pred, curr))
	{
	  result = (curr->key != key);
	  if (result) 
	    {
	      newnode = new_node_l(key, val, curr, epoch);
	      pred->next = newnode;
	    } 
	}
      GL_UNLOCK(set->lock);
      UNLOCK(ND_GET_LOCK(pred));
    }
  while (result < 0);
  return result;
}

/*
 * Logically remove an element by setting a mark bit to 1 
 * before removing it physically.
 */
svalue_t
parse_delete(intset_l_t *set, skey_t key, EpochThread epoch)
{
  node_l_t *pred, *curr;
  svalue_t result = 0;
  int done = 0;
	
  do
    {
      pred = set->head;
      curr = pred->next;
      while (likely(curr->key < key))
	{
	  pred = curr;
	  curr = curr->next;
	}


#if LAZY_RO_FAIL == 1 
      if (curr->key != key)
	{
	  return false;
	}
#endif

    //init log
   log->status = 0;
   log->node1 = NULL;
   log->node2 = NULL;
   log->addr = NULL;
   write_data_wait(log, (sizeof(thread_log_t)+63)/64);

      GL_LOCK(set->lock);		/* when GL_[UN]LOCK is defined the [UN]LOCK is not ;-) */
      LOCK(ND_GET_LOCK(pred));
      LOCK(ND_GET_LOCK(curr));

      if (parse_validate(pred, curr))
	{
	  if (key == curr->key)
	    {
	      result = curr->val;

          log->node1 = pred;
          log->val1 = *pred;
          log->node2 = curr;
          log->val2 = *curr;
          log->addr = (void*) curr; 
          write_data_wait(log, (sizeof(thread_log_t)+63)/64);

	      node_l_t* c_nxt = curr->next;
	      curr->marked = 1;
	      pred->next = c_nxt;

		EpochReclaimObject(epoch, (node_t*)curr, NULL, NULL, finalize_node);

          write_data_no_wait(curr, 1);
          write_data_no_wait(add addr to allocator);
          write_data_wait(pred, 1);

          log->state = finsihed;
          write_data_wait(log, (sizeof(thread_log_t)+63)/64);

	    }
	  done = 1;
	}

      GL_UNLOCK(set->lock);
      UNLOCK(ND_GET_LOCK(curr));
      UNLOCK(ND_GET_LOCK(pred));
    }
  while (!done);
  return result;
}
