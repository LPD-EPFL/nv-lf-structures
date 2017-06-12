/*   
 *   File: intset.h
 *   Author: Vincent Gramoli <vincent.gramoli@sydney.edu.au>, 
 *  	     Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *   Description: 
 *   intset.h is part of ASCYLIB
 *
 * Copyright (c) 2014 Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>,
 * 	     	      Tudor David <tudor.david@epfl.ch>
 *	      	      Distributed Programming Lab (LPD), EPFL
 *
 * ASCYLIB is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 */



#include "hashtable.h"

svalue_t ht_contains(ht_intset_t* set, skey_t key, EpochThread epoch, linkcache_t* buffer);
int ht_add(ht_intset_t* set, skey_t key, svalue_t val, EpochThread epoch, linkcache_t* buffer);
svalue_t ht_remove(ht_intset_t* set, skey_t key, EpochThread epoch, linkcache_t* buffer);

int is_reachable(linkedlist_t* ll, void* address);
void recover(linkedlist_t* ll, linkcache_t* buffer, active_page_table_t** page_buffers, int num_page_buffers);