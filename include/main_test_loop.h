/*   
 *   File: main_test_loop.h
 *   Author: Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *   Description: 
 *   main_test_loop.h is part of ASCYLIB
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

#ifndef _MAIN_TEST_LOOP_H_
#define _MAIN_TEST_LOOP_H_

#if OPS_PER_THREAD == 1
#  define PRINT_OPS_PER_THREAD()					\
  printf("%-3lu  %-8zu %-8zu %-8zu\n",					\
	 t, getting_count[t], putting_count[t], removing_count[t]);
#else
#  define PRINT_OPS_PER_THREAD()					
#endif

#if WORKLOAD == 2 //skewed workload
#  define THREAD_INIT(id)						\
  if (!ID) { printf("- Creating zipf random numbers array\n"); }	\
  __zipf_arr = zipf_get_rand_array(ZIPF_ALPHA, 0, rand_max + 1, id); \
  barrier_cross(&barrier);						\
  if (!ID) { printf("- Done\n"); }

#  define THREAD_END(id)					\
  ZIPF_STATS_DO(						\
		if (id == 0)					\
		  {						\
		    zipf_print_stats(__zipf_arr);		\
		  }						\
		free(__zipf_arr->stats);			\
							);	\
  free(__zipf_arr);

#else
#  define THREAD_INIT(id)
#  define THREAD_END()
#endif


#define TEST_VARS_GLOBAL						\
  volatile int phase_put = 0;						\
  volatile uint32_t phase_put_threshold_start = 0.99999 * UINT_MAX;	\
  volatile uint32_t phase_put_threshold_stop  = 0.9999999 * UINT_MAX;	\
  __thread volatile ticks phase_start, phase_stop;			\
  ZIPF_RAND_DECLARATIONS();

#ifndef WORKLOAD
#  define WORKLOAD 0		/* normal workload */
#endif 

#if WORKLOAD == 1		/* with phases */
#  define TEST_LOOP(algo_type)						\
  c = (uint32_t)(my_random(&(seeds[0]),&(seeds[1]),&(seeds[2])));	\
  key = (c & rand_max) + rand_min;					\
  if (!phase_put && c > phase_put_threshold_start)			\
    {									\
      phase_start = getticks();						\
      phase_put = 1;							\
      MEM_BARRIER;							\
    }									\
  else if (phase_put && c > phase_put_threshold_stop)			\
    {									\
      phase_stop = getticks();						\
      if (!ID) printf("[%2u]phase dur = %f\n", ID, (phase_stop-phase_start)/2.8e9); \
      phase_put = 0;							\
    }									\
									\
  if (phase_put || unlikely(c <= scale_put))				\
    {									\
      int res;								\
      START_TS(1);							\
      res = DS_ADD(set, key, key, epoch, lc);				\
      if(res)								\
	{								\
	  END_TS(1, my_putting_count_succ);				\
	  ADD_DUR(my_putting_succ);					\
	  my_putting_count_succ++;					\
	}								\
      END_TS_ELSE(4, my_putting_count - my_putting_count_succ,		\
		  my_putting_fail);					\
      my_putting_count++;						\
    }									\
  else if(unlikely(c <= scale_rem))					\
    {									\
      int removed;							\
      START_TS(2);							\
      removed = DS_REMOVE(set, key, epoch, lc);				\
      if(removed != 0)							\
	{								\
	  END_TS(2, my_removing_count_succ);				\
	  ADD_DUR(my_removing_succ);					\
	  my_removing_count_succ++;					\
	}								\
      END_TS_ELSE(5, my_removing_count - my_removing_count_succ,	\
		  my_removing_fail);					\
      my_removing_count++;						\
    }									\
  else									\
    {									\
      int res;								\
      START_TS(0);							\
      res = (sval_t) DS_CONTAINS(set, key, epoch, lc);			\
      if(res != 0)							\
	{								\
	  END_TS(0, my_getting_count_succ);				\
	  ADD_DUR(my_getting_succ);					\
	  my_getting_count_succ++;					\
	}								\
      END_TS_ELSE(3, my_getting_count - my_getting_count_succ,		\
		  my_getting_fail);					\
      my_getting_count++;						\
    }

#elif WORKLOAD == 0	/* normal uniform workload */

#  define TEST_LOOP(algo_type)						\
  c = (uint32_t)(my_random(&(seeds[0]),&(seeds[1]),&(seeds[2])));	\
  key = (c & rand_max) + rand_min;					\
									\
  if (unlikely(c <= scale_put))						\
    {									\
      int res;								\
      START_TS(1);							\
      res = DS_ADD(set, key, key, epoch, lc);				\
      if(res)								\
	{								\
	  END_TS(1, my_putting_count_succ);				\
	  ADD_DUR(my_putting_succ);					\
	  my_putting_count_succ++;					\
	}								\
      END_TS_ELSE(4, my_putting_count - my_putting_count_succ,		\
		  my_putting_fail);					\
      my_putting_count++;						\
    }									\
  else if(unlikely(c <= scale_rem))					\
    {									\
      int removed;							\
      START_TS(2);							\
      removed = DS_REMOVE(set, key, epoch, lc);				\
      if(removed != 0)							\
	{								\
	  END_TS(2, my_removing_count_succ);				\
	  ADD_DUR(my_removing_succ);					\
	  my_removing_count_succ++;					\
	}								\
      END_TS_ELSE(5, my_removing_count - my_removing_count_succ,	\
		  my_removing_fail);					\
      my_removing_count++;						\
    }									\
  else									\
    {									\
      int res;								\
      START_TS(0);							\
      res = (sval_t) DS_CONTAINS(set, key, epoch, lc);			\
      if(res != 0)							\
	{								\
	  END_TS(0, my_getting_count_succ);				\
	  ADD_DUR(my_getting_succ);					\
	  my_getting_count_succ++;					\
	}								\
      END_TS_ELSE(3, my_getting_count - my_getting_count_succ,		\
		  my_getting_fail);					\
      my_getting_count++;						\
    }

#  define TEST_LOOP_ONLY_UPDATES()					\
  c = (uint32_t)(my_random(&(seeds[0]),&(seeds[1]),&(seeds[2])));	\
  if (unlikely(c < scale_put))						\
    {									\
      key = (c & rand_max) + rand_min;					\
      int res;								\
      START_TS(1);							\
      res = DS_ADD(set, key, key, epoch, lc);					\
      if(res)								\
	{								\
	  END_TS(1, my_putting_count_succ);				\
	  ADD_DUR(my_putting_succ);					\
	  my_putting_count_succ++;					\
	}								\
      END_TS_ELSE(4, my_putting_count - my_putting_count_succ,		\
		  my_putting_fail);					\
      my_putting_count++;						\
    }									\
  else if(unlikely(c <= scale_rem))					\
    {									\
      int removed;							\
      START_TS(2);							\
      removed = DS_REMOVE(set, key, epoch, lc);						\
      if(removed != 0)							\
	{								\
	  END_TS(2, my_removing_count_succ);				\
	  ADD_DUR(my_removing_succ);					\
	  my_removing_count_succ++;					\
	}								\
      END_TS_ELSE(5, my_removing_count - my_removing_count_succ,	\
		  my_removing_fail);					\
      my_removing_count++;						\
    }									\
  cpause((num_threads-1)*32);

/* cdelay(1); */
/* cpause(0); */
/* cdelay((num_threads-1)*128); */


#elif WORKLOAD == 2	/* zipf workload */

#  define TEST_LOOP(algo_type)						\
  c = (uint32_t)(my_random(&(seeds[0]),&(seeds[1]),&(seeds[2])));	\
  key = rand_max - zipf_get_next(__zipf_arr) + rand_min;		\
					     ZIPF_STATS_DO(__zipf_arr->stats[key]++); \
				  					\
					     if (unlikely(c <= scale_put)) \
					       {			\
						 int res;		\
						 START_TS(1);		\
						 res = DS_ADD(set, key, key, epoch, lc); \
						 if(res)		\
						   {			\
						     END_TS(1, my_putting_count_succ); \
						     ADD_DUR(my_putting_succ); \
						     my_putting_count_succ++; \
						   }			\
						 END_TS_ELSE(4, my_putting_count - my_putting_count_succ, \
							     my_putting_fail); \
						 my_putting_count++;	\
					       }			\
					     else if(unlikely(c <= scale_rem)) \
					       {			\
						 int removed;		\
						 START_TS(2);		\
						 removed = DS_REMOVE(set, key, epoch, lc); \
						 if(removed != 0)	\
						   {			\
						     END_TS(2, my_removing_count_succ);	\
						     ADD_DUR(my_removing_succ);	\
						     my_removing_count_succ++; \
						   }			\
						 END_TS_ELSE(5, my_removing_count - my_removing_count_succ, \
							     my_removing_fail);	\
						 my_removing_count++;	\
					       }			\
					     else			\
					       {			\
						 int res;		\
						 START_TS(0);		\
						 res = (sval_t) DS_CONTAINS(set, key, epoch, lc); \
						 if(res != 0)		\
						   {			\
						     END_TS(0, my_getting_count_succ); \
						     ADD_DUR(my_getting_succ); \
						     my_getting_count_succ++; \
						   }			\
						 END_TS_ELSE(3, my_getting_count - my_getting_count_succ, \
							     my_getting_fail); \
						 my_getting_count++;	\
					       }

#endif	/* WORKLOAD */

#define POW_CORRECTED 0


#endif	/* _MAIN_TEST_LOOP_H_ */
