#include "optimistic.h"
#include "utils.h"

#include "latency.h"

void finalize_node(void * node, void * context, void* tls) {
	EpochFreeNode(node);
}

#if LATENCY_PARSING == 1
__thread size_t lat_parsing_get = 0;
__thread size_t lat_parsing_put = 0;
__thread size_t lat_parsing_rem = 0;
#endif	/* LATENCY_PARSING == 1 */

extern ALIGNED(CACHE_LINE_SIZE) unsigned int levelmax;

#define MAX_BACKOFF 131071
#define HERLIHY_MAX_MAX_LEVEL 64 /* covers up to 2^64 elements */

inline int 
ok_to_delete(sl_node_t *node, int found)
{
  return (node->fullylinked && ((node->toplevel-1) == found) && !node->marked);
}

/*
 * Function optimistic_search corresponds to the findNode method of the 
 * original paper. A fast parameter has been added to speed-up the search 
 * so that the function quits as soon as the searched element is found.
 */
inline int
optimistic_search(sl_intset_t *set, skey_t key, sl_node_t **preds, sl_node_t **succs, int fast)
{
 restart:
  PARSE_TRY();
  int found, i;
  sl_node_t *pred, *curr;
	
  found = -1;
  pred = set->head;
	
  for (i = (pred->toplevel - 1); i >= 0; i--)
    {
      curr = pred->next[i];
      while (key > curr->key)
	{
	  pred = curr;
	  curr = pred->next[i];
	}
      if (preds != NULL)
	{
	  preds[i] = pred;
	  if (unlikely(pred->marked))
	    {
	      goto restart;
	    }
	}
      succs[i] = curr;
      if (found == -1 && key == curr->key)
	{
	  found = i;
	}
    }
  return found;
}

inline sl_node_t*
optimistic_left_search(sl_intset_t *set, skey_t key)
{
  PARSE_TRY();
  int i;
  sl_node_t *pred, *curr, *nd = NULL;
	
  pred = set->head;
	
  for (i = (pred->toplevel - 1); i >= 0; i--)
    {
      curr = pred->next[i];
      while (key > curr->key)
	{
	  pred = curr;
	  curr = pred->next[i];
	}

      if (key == curr->key)
	{
	  nd = curr;
	  break;
	}
    }

  return nd;
}

/*
 * Function optimistic_find corresponds to the contains method of the original 
 * paper. In contrast with the original version, it allocates and frees the 
 * memory at right places to avoid the use of a stop-the-world garbage 
 * collector. 
 */
svalue_t
optimistic_find(sl_intset_t *set, skey_t key)
{ 
  svalue_t result = 0;

  PARSE_START_TS(0);
  sl_node_t* nd = optimistic_left_search(set, key);
  PARSE_END_TS(0, lat_parsing_get++);

  if (nd != NULL && !nd->marked && nd->fullylinked)
    {
      result = nd->val;
    }
  return result;
}

/*
 * Function unlock_levels is an helper function for the insert and delete 
 * functions.
 */ 
inline void
unlock_levels(sl_intset_t* set, sl_node_t **nodes, int highestlevel)
{

#if defined(LL_GLOBAL_LOCK)
  GL_UNLOCK(set->lock);
#else 
  int i;
  sl_node_t *old = NULL;
  for (i = 0; i <= highestlevel; i++)
    {
      if (old != nodes[i])
	{
	  UNLOCK(ND_GET_LOCK(nodes[i]));
	}
      old = nodes[i];
    }
#endif
}

/*
 * Function optimistic_insert stands for the add method of the original paper.
 * Unlocking and freeing the memory are done at the right places.
 */
int
optimistic_insert(sl_intset_t *set, skey_t key, svalue_t val)
{
  sl_node_t *succs[HERLIHY_MAX_MAX_LEVEL], *preds[HERLIHY_MAX_MAX_LEVEL];
  sl_node_t  *node_found, *prev_pred, *new_node;
  sl_node_t *pred, *succ;
  int toplevel, highest_locked, i, valid, found;
  unsigned int backoff;

  toplevel = get_rand_level();
  backoff = 1;
	
  PARSE_START_TS(1);
  while (1) 
    {
      UPDATE_TRY();
      found = optimistic_search(set, key, preds, succs, 1);
      PARSE_END_TS(1, lat_parsing_put);

      if (found != -1)
	{
	  node_found = succs[found];
	  if (!node_found->marked)
	    {
	      while (!node_found->fullylinked)
		{
		  PAUSE;
		}
	      PARSE_END_INC(lat_parsing_put);
	      return 0;
	    }
	  continue;
	}

      GL_LOCK(set->lock);		/* when GL_[UN]LOCK is defined the [UN]LOCK is not ;-) */
      highest_locked = -1;
      prev_pred = NULL;
      valid = 1;
      for (i = 0; valid && (i < toplevel); i++)
	{
	  pred = preds[i];
	  succ = succs[i];
	  if (pred != prev_pred)
	    {
	      LOCK(ND_GET_LOCK(pred));
	      highest_locked = i;
	      prev_pred = pred;
	    }	
			
	  valid = (!pred->marked && !succ->marked && 
		   ((volatile sl_node_t*) pred->next[i] == 
		    (volatile sl_node_t*) succ));
	}
	
      if (!valid) 
	{			 /* Unlock the predecessors before leaving */ 
	  unlock_levels(set, preds, highest_locked); /* unlocks the global-lock in the GL case */
	  if (backoff > 5000) 
	    {
	      nop_rep(backoff & MAX_BACKOFF);
	    }
	  backoff <<= 1;
	  continue;
	}

      /*my_log->status = LOG_STATUS_CLEAN;*/

      for (i = 0; i < levelmax; i++) {
          my_log->nodes[i] = NULL;
      }
      my_log->addr = NULL;
      /*write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);*/


         //write redo log
         my_log->nodes[0] = (sl_node_t*) GetNextNodeAddress(sizeof(sl_node_t));
         my_log->vals[0].key = key;
         my_log->vals[0].val = val;
         my_log->vals[0].fullylinked = 1;
         my_log->vals[0].marked = 0;
        for (i = 0; i < toplevel; i++)
      	{
	     my_log->vals[0].next[i] = succs[i];
    	}

        for (i = 0; i < toplevel; i++)
      	{
	     my_log->nodes[i+1] = preds[i];
         memcpy((void*)&(my_log->vals[i+1]), (void*) preds[i], sizeof(sl_node_t));
         my_log->nodes[i+1]->next[i] = new_node;
    	}
         my_log->addr = (void*)my_log->nodes[0];
         write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);
		
        my_log->status = LOG_STATUS_PENDING;
        write_data_wait(&my_log->status,1);
      //START OF UPDATES
      new_node = sl_new_simple_node(key, val, toplevel, 0);

      for (i = 0; i < toplevel; i++)
	{
	  new_node->next[i] = succs[i];
	}

      for (i = 0; i < toplevel; i++)
	{
	  preds[i]->next[i] = new_node;
      write_data_nowait((void*)preds[i], CACHE_LINES_PER_NV_NODE);
	}
		
      new_node->fullylinked = 1;

      write_data_wait((void*)new_node, 1);
      //END OF UPDATES

       my_log->status = LOG_STATUS_COMMITTED;
       write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);

      unlock_levels(set, preds, highest_locked);
      PARSE_END_INC(lat_parsing_put);
      return 1;
    }
}

/*
 * Function optimistic_delete is similar to the method remove of the paper.
 * Here we avoid the fast search parameter as the comparison is faster in C 
 * than calling the Java compareTo method of the Comparable interface 
 * (cf. p132 of SIROCCO'07 proceedings).
 */
svalue_t
optimistic_delete(sl_intset_t *set, skey_t key)
{
  sl_node_t *succs[HERLIHY_MAX_MAX_LEVEL], *preds[HERLIHY_MAX_MAX_LEVEL];
  sl_node_t *node_todel, *prev_pred; 
  sl_node_t *pred, *succ;
  int is_marked, toplevel, highest_locked, i, valid, found;	
  unsigned int backoff;

  node_todel = NULL;
  is_marked = 0;
  toplevel = -1;
  backoff = 1;
	
  PARSE_START_TS(2);

      /*my_log->status = LOG_STATUS_CLEAN;*/

      for (i = 0; i < levelmax; i++) {
          my_log->nodes[i] = NULL;
      }
      my_log->addr = NULL;
      /*write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);*/

  while(1)
    {
      UPDATE_TRY();
      found = optimistic_search(set, key, preds, succs, 1);
      PARSE_END_TS(2, lat_parsing_rem);

      /* If not marked and ok to delete, then mark it */
      if (is_marked || (found != -1 && ok_to_delete(succs[found], found)))
	{	
	  GL_LOCK(set->lock); /* when GL_[UN]LOCK is defined the [UN]LOCK is not ;-) */
	  if (!is_marked)
	    {
	      node_todel = succs[found];

	      LOCK(ND_GET_LOCK(node_todel));
	      toplevel = node_todel->toplevel;
	      /* Unless it has been marked meanwhile */

	      if (node_todel->marked)
		{
		  GL_UNLOCK(set->lock);
		  UNLOCK(ND_GET_LOCK(node_todel));
		  PARSE_END_INC(lat_parsing_rem);
		  return 0;
		}

          
          my_log->nodes[0] = node_todel;
         memcpy((void*)&(my_log->vals[0]), (void*) node_todel, sizeof(sl_node_t));
         my_log->vals[1]->marked = 1;
         write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);
        my_log->status = LOG_STATUS_PENDING;
        write_data_wait(&my_log->status,1);
	      node_todel->marked = 1;
	      is_marked = 1;
          write_data_wait((void*)node_todel, 1);
          my_log->status = LOG_STATUS_COMMITTED;
          write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);
	    }

	  /* Physical deletion */
	  highest_locked = -1;
	  prev_pred = NULL;
	  valid = 1;
	  for (i = 0; valid && (i < toplevel); i++)
	    {
	      pred = preds[i];
	      succ = succs[i];
	      if (pred != prev_pred)
		{
		  LOCK(ND_GET_LOCK(pred));
		  highest_locked = i;
		  prev_pred = pred;
		}

	      valid = (!pred->marked && ((volatile sl_node_t*) pred->next[i] == 
					 (volatile sl_node_t*) succ));
	    }

	  if (!valid)
	    {	
	      unlock_levels(set, preds, highest_locked);
	      if (backoff > 5000) 
		{
		  nop_rep(backoff & MAX_BACKOFF);
		}
	      backoff <<= 1;
	      continue;
	    }

      /*my_log->status = LOG_STATUS_CLEAN;*/
      /*write_data_wait(&my_log->status, 1);*/

        for (i = 0; i < toplevel; i++)
      	{
	     my_log->nodes[i+1] = preds[i];
         memcpy((void*)&(my_log->vals[i+1]), (void*) preds[i], sizeof(sl_node_t));
         my_log->nodes[i+1]->next[i] = node_todel->next[i];
    	}
         my_log->addr = (void*)my_log->nodes[0];
         write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);

        my_log->status = LOG_STATUS_PENDING;
        write_data_wait(&my_log->status,1);
       //PHYSICAL DELETION START
			
	  for (i = (toplevel-1); i >= 0; i--)
	    {
	      preds[i]->next[i] = node_todel->next[i];
	    }

	  svalue_t val = node_todel->val;

	 EpochReclaimObject(epoch, (void*)node_todel, NULL, NULL, finalize_node);

      for (i = 0; i < toplevel; i++)
	{
	  preds[i]->next[i] = new_node;
      write_data_nowait((void*)preds[i], CACHE_LINES_PER_NV_NODE);
	}

          write_data_wait((void*)node_todel, CACHE_LINES_PER_NV_NODE);

          my_log->status = LOG_STATUS_COMMITTED;
          write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);
     //PHYSICAL DELETION END

	  UNLOCK(ND_GET_LOCK(node_todel));
	  unlock_levels(set, preds, highest_locked);

	  PARSE_END_INC(lat_parsing_rem);
	  return val;
	}
      else
	{
	  PARSE_END_INC(lat_parsing_rem);
	  return 0;
	}
    }
}
