
#include "bst_tk.h"

void finalize_node(void * node, void * context, void* tls) {
	EpochFreeNode(node);
}

svalue_t
bst_tk_delete(intset_t* set, skey_t key, EpochThread epoch)
{
  node_t* curr;
  node_t* pred = NULL;
  node_t* ppred = NULL;
  volatile uint64_t curr_ver = 0;
  uint64_t pred_ver = 0, ppred_ver = 0, right = 0, pright = 0;

 retry:

  curr = set->head;

  do
    {
      curr_ver = curr->lock.to_uint64;

      ppred = pred;
      ppred_ver = pred_ver;
      pright = right;

      pred = curr;
      pred_ver = curr_ver;

      if (key < curr->key)
	{
	  right = 0;
	  curr = (node_t*) curr->left;
	}
      else
	{
	  right = 1;
	  curr = (node_t*) curr->right;
	}
    }
  while(likely(!curr->leaf));


  if (curr->key != key)
    {
      return 0;
    }

  if ((!tl_trylock_version(&ppred->lock, (volatile tl_t*) &ppred_ver, pright)))
    {
      goto retry;
    }

  if ((!tl_trylock_version_both(&pred->lock, (volatile tl_t*) &pred_ver)))
    {
      tl_revert(&ppred->lock, pright);
      goto retry;
    }

    //AT THIS POINT I HAVE THE LOCKS AND WILL BE DOING THE CHANGES

   /*my_log->status = LOG_STATUS_CLEAN;*/
   my_log->node1 = NULL;
   my_log->node2 = NULL;
   my_log->node3 = NULL;
  my_log->addr1 = NULL;
  my_log->addr2 = NULL;
   /*write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);*/

   //write redo log
   my_log->node1 = curr; 
   my_log->node2 = pred; 
   my_log->node3 = ppred; 
   my_log->addr1 = curr; 
   my_log->addr2 = curr; 
   memcpy((void*)&(my_log->val3), (void*) ppred, sizeof(node_t));


  if (pright)
    {
      if (right)
	{
	  my_log->val3.right = pred->left;
	}
      else
	{
	  my_log->val3.right = pred->right;
	}
    }
  else
    {
      if (right)
	{
	  my_log->val3.left = pred->left;
	}
      else
	{
	  my_log->val3.left = pred->right;
	}
    }

   write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);
   //do changes

   my_log->status = LOG_STATUS_PENDING;
   write_data_wait(&my_log->status,1);



  if (pright)
    {
      if (right)
	{
	  ppred->right = pred->left;
	}
      else
	{
	  ppred->right = pred->right;
	}
    }
  else
    {
      if (right)
	{
	  ppred->left = pred->left;
	}
      else
	{
	  ppred->left = pred->right;
	}
    }

  EpochReclaimObject(epoch, (void*)curr, NULL, NULL, finalize_node);
  EpochReclaimObject(epoch, (void*)pred, NULL, NULL, finalize_node);

  write_data_wait((void*)ppred, 1);
  my_log->status = LOG_STATUS_COMMITTED;
  write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);
  tl_unlock(&ppred->lock, pright);


  return curr->val;
} 

svalue_t
bst_tk_find(intset_t* set, skey_t key, EpochThread epoch) 
{

  node_t* curr = set->head;

  while (likely(!curr->leaf))
    {
      if (key < curr->key)
	{
	  curr = (node_t*) curr->left;
	}
      else
	{
	  curr = (node_t*) curr->right;
	}
    }

  if (curr->key == key)
    {
      return curr->val;
    }  

  return 0;
}

int
bst_tk_insert(intset_t* set, skey_t key, svalue_t val, EpochThread epoch) 
{
  node_t* curr;
  node_t* pred = NULL;
  volatile uint64_t curr_ver = 0;
  uint64_t pred_ver = 0, right = 0;

 retry:

  curr = set->head;

  do
    {
      curr_ver = curr->lock.to_uint64;

      pred = curr;
      pred_ver = curr_ver;

      if (key < curr->key)
	{
	  right = 0;
	  curr = (node_t*) curr->left;
	}
      else
	{
	  right = 1;
	  curr = (node_t*) curr->right;
	}
    }
  while(likely(!curr->leaf));


  if (curr->key == key)
    {
      return 0;
    }


  if ((!tl_trylock_version(&pred->lock, (volatile tl_t*) &pred_ver, right)))
    {
      goto retry;
    }


    //AT THIS POINT I CAN DO THE INSERT
   /*my_log->status = LOG_STATUS_CLEAN;*/
   my_log->node1 = NULL;
   my_log->node2 = NULL;
   my_log->node3 = NULL;
  my_log->addr1 = NULL;
  my_log->addr2 = NULL;
   /*write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);*/

   //write redo log
   my_log->node2 = (node_t*) GetNextNodeAddress(sizeof(node_t));
   my_log->node1 = (node_t*) GetNextNodeAddress(sizeof(node_t));

   my_log->val2.key = key;
   my_log->val2.val = val;
   my_log->val2.left = NULL;
   my_log->val2.right = NULL;
   my_log->val2.lock.to_uint64 = 0;

   my_log->val1.val = 0;
   my_log->val1.lock.to_uint64 = 0;

   if (key < curr->key)
   {
       my_log->val1.key = curr->key;
       my_log->val1.left = my_log->node2;
       my_log->val2.right = curr;
   }
   else
   {
       my_log->val1.key = key;
       my_log->val1.left = curr;
       my_log->val1.right = my_log->node2;
   }
   
   my_log->node3 = pred;
   memcpy((void*)&(my_log->val3), (void*) pred, sizeof(node_t));

  if (right)
    {
      my_log->val3.right = my_log->node1;
    }
  else
    {
       my_log->val3.left =  my_log->node1;
    }

         my_log->addr1 = (void*)my_log->node1;
         my_log->addr2 = (void*)my_log->node2;
         write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);
         //do changes

   my_log->status = LOG_STATUS_PENDING;
   write_data_wait(&my_log->status,1);

  node_t* nn = new_node(key, val, NULL, NULL, 0, epoch);
  node_t* nr = new_node_no_init(epoch);

  if (key < curr->key)
    {
      nr->key = curr->key;
      nr->left = nn;
      nr->right = curr;
    }
  else
    {
      nr->key = key;
      nr->left = curr;
      nr->right = nn;
    }

  if (right)
    {
      pred->right = nr;
    }
  else
    {
      pred->left = nr;
    }

   write_data_nowait((void*)nr, 1);
   write_data_wait((void*)pred, 1);

     //commit
      my_log->status = LOG_STATUS_COMMITTED;
      write_data_wait(my_log, (sizeof(thread_log_t)+63)/64);
  tl_unlock(&pred->lock, right);

  return 1;
}
