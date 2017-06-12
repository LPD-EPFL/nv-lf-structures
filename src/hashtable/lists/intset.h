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

/*
 *  linkedlist.h
 *  
 *
 *  Copyright 2009 __MyCompanyName__. All rights reserved.
 *
 */

#include "harris.h"

svalue_t set_contains(intset_t *set, skey_t key);
int set_add(intset_t *set, skey_t key, skey_t val);
svalue_t set_remove(intset_t *set, skey_t key);

