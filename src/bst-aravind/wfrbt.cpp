   /*A Lock Free Binary Search Tree
 
 * File:
 *   wfrbt.cpp
 * Author(s):
 *   Aravind Natarajan <natarajan.aravind@gmail.com>
 * Description:
 *   A Lock Free Binary Search Tree
 *
 * Copyright (c) 20013-2014.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

Please cite our PPoPP 2014 paper - Fast Concurrent Lock-Free Binary Search Trees by Aravind Natarajan and Neeraj Mittal if you use our code in your experiments    

 Features:
1. Insert operations directly install their window without injecting the operation into the tree. They help any conflicting operation at the injection point, before executing their window txn.
2. Delete operations are the same as that of the original algorithm.
 
 */

#include <getopt.h>
#include <signal.h>
#include <sys/time.h>
#include <vector>
#include <set>

#include "wfrbt.h"
#include "operations.h" 


#ifdef DEBUG
#define IO_FLUSH                        fflush(NULL)
/* Note: stdio is thread-safe */
#endif

/*
 * Useful macros to work with transactions. Note that, to use nested
 * transactions, one should check the environment returned by
 * stm_get_env() and only call sigsetjmp() if it is not null.
 */

#define PREPROCESSING 0
//#define ESTIMATE_RECOVERY 1
#define FLUSH_CACHES 1

#define RO                              1
#define RW                              0

#define DEFAULT_SEED                    0
#define DEFAULT_SEARCH_FRAC             0.0
#define DEFAULT_INSERT_FRAC             0.5
#define DEFAULT_DELETE_FRAC             0.5
#define DEFAULT_READER_THREADS             0
#define DEFAULT_KEYSPACE1_SIZE          100
#define KEYSPACE1_PROB                  1.0
#define LOWERHALF                           0.99

#define XSTR(s)                         STR(s)
#define STR(s)                          #s

/* ################################################################### *
 * GLOBALS
 * ################################################################### */

static volatile AO_t stop = 0;
static volatile AO_t stop2 = 0;
long total_insert = 0;
timespec t;
long leafNodes = 0;


/* ################################################################### *
 * Correctness Checking
 * ################################################################### */
 
uint64_t in_order_visit(node_t * rootNode){
    uint64_t key = rootNode->key;
    
    if((node_t *)get_addr_for_reading(rootNode->child.AO_val1) == NULL){
        leafNodes++;
        return (key);
    }
    
    node_t * lChild = (node_t *)get_addr_for_reading(rootNode->child.AO_val1);
    node_t * rChild = (node_t *)get_addr_for_reading(rootNode->child.AO_val2);
    
    if((lChild) != NULL){
        uint64_t lKey = in_order_visit(lChild);
        if(lKey >= key){
            std::cout << "Lkey is larger!!__" << lKey << "__ " << key << std::endl;
            std::cout << "Sanity Check Failed!!" << std::endl;
        }
    }
    
    if((rChild) != NULL){
        uint64_t rKey = in_order_visit(rChild);
        if(rKey < key){
            std::cout << "Rkey is smaller!!__" << rKey << "__ " << key <<  std::endl;
            std::cout << "Sanity Check Failed!!" << std::endl;
        }
    }
    return (key);
}




/*************************************************************************************************/
int perform_one_insert_window_operation(thread_data_t* data, seekRecord_t * R, uint64_t newKey){
  
  node_t *newInt ;
  node_t *newLeaf;
  // if(data->recycledNodes.empty()){
  //     node_t * allocedNodeArr =(node_t *)malloc(2*sizeof(node_t));// new pointerNode_t[2];
  //   newInt = &allocedNodeArr[0];
  //   newLeaf = &allocedNodeArr[1]; 
  // }
  // else{ 
  //   // reuse memory of previously allocated nodes.
  //   newInt = data->recycledNodes.back();
  //   data->recycledNodes.pop_back();
  //   newLeaf = data->recycledNodes.back();
  //   data->recycledNodes.pop_back();
  // }
  newInt = (node_t*)EpochAllocNode(data->epoch, sizeof(node_t));
  newLeaf = (node_t*)EpochAllocNode(data->epoch, sizeof(node_t));
        
  newLeaf->child.AO_val1 = 0;
  newLeaf->child.AO_val2 = 0;
  newLeaf->key = newKey;

  write_data_wait((void*) newInt, CACHE_LINES_PER_NV_NODE);      
  write_data_nowait((void*) newLeaf, CACHE_LINES_PER_NV_NODE);
  
  _mm_sfence();
    
  node_t * existLeaf = (node_t *)get_addr_for_reading(R->pL);
  uint64_t existKey = R->leafKey;

        
  if(newKey < existKey){
    // key is to be inserted on lchild
    newInt->key = existKey;
    newInt->child.AO_val1 = create_child_word(newLeaf,0,0);            
    newInt->child.AO_val2 = create_child_word(existLeaf,0,0);
  }
  else{
    // key is to be inserted on rchild
    newInt->key = newKey;
    newInt->child.AO_val2 = create_child_word(newLeaf,0,0);            
    newInt->child.AO_val1 = create_child_word(existLeaf,0,0);
  }
        
  // cas to replace window
  AO_t newCasField;
  newCasField = create_child_word(newInt,UNMARK,UNFLAG);
  int result;
        
  if(R->isLeftL){
    // result = atomic_cas_full(&R->parent->child.AO_val1, R->pL, newCasField);
    result = cache_try_link_and_add(data->buffer, newKey, (volatile void**)&R->parent->child.AO_val1, (volatile void*)R->pL, (volatile void*)newCasField);
  }
  else{
    // result = atomic_cas_full(&R->parent->child.AO_val2, R->pL, newCasField);
    result = cache_try_link_and_add(data->buffer, newKey, (volatile void**)&R->parent->child.AO_val2, (volatile void*)R->pL, (volatile void*)newCasField);
  }
        
  if(result == 1){
    // successfully inserted.
    data->numInsert++;
    return 1;
  }
  else{
    // reuse data and pointer nodes
    // data->recycledNodes.push_back(newInt);
    // data->recycledNodes.push_back(newLeaf);

    // TODO: free nodes
    return 0; 
  }
}

/*************************************************************************************************/

int perform_one_delete_window_operation(thread_data_t* data, seekRecord_t * R, uint64_t key){
  
  AO_t pS;
    
  // mark sibling.
  if(R->isLeftL){
    // L is the left child of P
    mark_Node(&R->parent->child.AO_val2);
    pS = R->parent->child.AO_val2;
  }
  else{
    mark_Node(&R->parent->child.AO_val1);
    pS = R->parent->child.AO_val1;
  }
         
  AO_t newWord;
        
  if(is_flagged(pS)){
    newWord = create_child_word((node_t *)get_addr_for_comparing(pS), UNMARK, FLAG);    
  }
  else{
    newWord = create_child_word((node_t *)get_addr_for_comparing(pS), UNMARK, UNFLAG);
  }
        
  int result;
  
  EpochDeclareUnlinkNode(data->epoch, (void*)R->lumC, sizeof(node_t));
  if(R->isLeftUM){
    // result = atomic_cas_full(&R->lum->child.AO_val1, R->lumC, newWord);
    result = cache_try_link_and_add(data->buffer, key, (volatile void**)&R->lum->child.AO_val1, (volatile void*)R->lumC, (volatile void*)newWord);
  }
  else{
    // result = atomic_cas_full(&R->lum->child.AO_val2, R->lumC, newWord);
    result = cache_try_link_and_add(data->buffer, key, (volatile void**)&R->lum->child.AO_val2, (volatile void*)R->lumC, (volatile void*)newWord);
  }

  return result;    
}


/* ################################################################### *
 * BARRIER
 * ################################################################### */

// void barrier_init(barrier_t *b, int n)
// {
//   pthread_cond_init(&b->complete, NULL);
//   pthread_mutex_init(&b->mutex, NULL);
//   b->count = n;
//   b->crossing = 0;
// }

// void barrier_cross(barrier_t *b)
// {
//   pthread_mutex_lock(&b->mutex);
//   /* One more thread through */
//   b->crossing++;
//   /* If not all here, wait */
//   if (b->crossing < b->count) {
//     pthread_cond_wait(&b->complete, &b->mutex);
//   } else {
//     pthread_cond_broadcast(&b->complete);
//     /* Reset for next time */
//     b->crossing = 0;
//   }
//   pthread_mutex_unlock(&b->mutex);
// }

// #ifdef PREPROCESSING
// bool barrier_peek(barrier_t *b)
// {
//   pthread_mutex_lock(&b->mutex);
//   if (b->crossing == (b->count - 1)) {
//     // all threads except the main thread have reached it.
//  pthread_mutex_unlock(&b->mutex);
//     return false;
//   } else { 
//  pthread_mutex_unlock(&b->mutex);
//     return true;
//   }
// }
// #endif

/* ################################################################### *
 * TEST
 * ################################################################### */


#ifdef ESTIMATE_RECOVERY
#define MULTITHREADED_RECOVERY 1
#define RECOVERY_THREADS 1
typedef struct rec_thread_info_t{
    uint32_t id;
    size_t size;
    unsigned int seed;
    size_t rec_threads;
    size_t page_size;
    node_t* root; 
    ticks time;
} rec_thread_info_t;
std::set<void*> unique_pages;
std::vector<void*> page_vector;
barrier_t barrier_local, barrier_global;

void*
rec(void* thread) 
{
  //pthread_exit(NULL);
  rec_thread_info_t* td = (rec_thread_info_t*) thread;
  uint32_t ID = td->id;
  size_t rec_threads = td->rec_threads;
  size_t size = td->size;
  unsigned int seed = td->seed;
  size_t page_size = td->page_size;
  ticks duration;
  node_t* root = td->root;
  size_t i = ID;
  size_t k;
  size_t nodes_per_page;

  //fprintf(stderr, "thread %d, total size %lu\n", ID, size);
#ifdef FLUSH_CACHES
#define NUM_EL 8388608
    volatile uint64_t* elms = (volatile uint64_t*) malloc (NUM_EL*sizeof(uint64_t));

    size_t el;
    for (k = 0; k < NUM_EL; k++) {
      el = rand_r(&seed) % (NUM_EL);
      //el = (my_random(&(seeds[0]), &(seeds[1]), &(seeds[2])) % (NUM_EL - 1));
       elms[el] = el+2;
    }
#endif

  barrier_cross(&barrier_global);

  volatile ticks corr = getticks_correction_calc();
  ticks startCycles = getticks();
  size_t all = CACHE_LINES_PER_NV_NODE * CACHE_LINE_SIZE;
  assert(all >= sizeof(node_t));

  while (i < size) {
  void * crt_address = page_vector[i]; 
  nodes_per_page = page_size / all; 
  for (k = 0; k < nodes_per_page; k++) {
        void * node_address = (void*)((UINT_PTR)crt_address + (all*k));
    if (!DSNodeMemoryIsFree(node_address, all)) {
      if (!is_reachable(root, node_address)) {
                MarkNodeMemoryAsFree(node_address); 
            }
        }
    }
    
    i+= rec_threads;
  }
  //DO RECOVERY
  ticks endCycles = getticks();

  duration = endCycles - startCycles + corr;
  td->time = duration;

  barrier_cross(&barrier_local);
  //fprintf(stderr, "recovery took %llu\n", duration);
  pthread_exit(NULL);

}
#endif

void *testRW(void *data)
{
  
  double action;
  Word key;
  thread_data_t *d = (thread_data_t *)data;

#if PREPROCESSING  
   bool prepop = false;
#endif

  d->epoch = EpochThreadInit(d->id);
  
  d->page_table = (active_page_table_t*)GetOpaquePageBuffer(d->epoch);

  /* Wait on barrier */
restart:  
  barrier_cross(d->barrier);

  while (stop == 0) {
    // determine what we're going to do
    action = (double)rand_r(&d->seed) / (double) RAND_MAX;
      key = rand_r(&d->seed) % (d->keyspace1_size);
    while(key == 0){
        key = rand_r(&d->seed) % (d->keyspace1_size);
      }
 
    if (action <= d->search_frac){
      // Search Operation
#ifdef DETAILED_STATS
      auto search_start = std::chrono::high_resolution_clock::now();
#endif
        
      search(d, key);

#ifdef DETAILED_STATS
        auto search_end = std::chrono::high_resolution_clock::now();
      auto search_dur = std::chrono::duration_cast<std::chrono::microseconds>(search_end - search_start);
      d->tot_read_time += search_dur.count();
        d->tot_reads++;
#endif
    }
    else if (action > d->search_frac && action <= (d->search_frac + d->insert_frac)){
      // Insert Operation
        
      bool slowOpn = false;
        
#ifdef DETAILED_STATS
    auto ins_start = std::chrono::high_resolution_clock::now();
#endif    
        
         slowOpn = insert(d,key);
        
#ifdef DETAILED_STATS
          auto ins_end = std::chrono::high_resolution_clock::now();
        auto ins_dur = std::chrono::duration_cast<std::chrono::microseconds>(ins_end - ins_start);
          if(slowOpn){
              d->tot_slowins_time += ins_dur.count();
              d->tot_slowins_count++;
          }
          else{
              d->tot_fastins_time += ins_dur.count();
              d->tot_fastins_count++;
          }
#endif    
    }
    else{
      // Delete Operation
      bool slowOpn = false;

#ifdef DETAILED_STATS
      auto del_start = std::chrono::high_resolution_clock::now();
#endif
        
      slowOpn = delete_node(d,key);
#ifdef DETAILED_STATS
        auto del_end = std::chrono::high_resolution_clock::now();
        auto del_dur = std::chrono::duration_cast<std::chrono::microseconds>(del_end - del_start);
          if(slowOpn){
              d->tot_slowdel_time += del_dur.count();
              d->tot_slowdel_count++;
          }
          else{
              d->tot_fastdel_time += del_dur.count();
              d->tot_fastdel_count++;
          }    
#endif
    }
    d->ops++;
  }

#if PREPROCESSING    
    if(!prepop){
    
        barrier_cross(d->barrier2);
    }
    
    
    if(stop2 == 1){
        if(prepop){
            //Done.
            return NULL;
        }
        else{
            prepop = true;
            d->ops = 0;
        }
    
    }
    goto restart; 
#endif

#ifndef ESTIMATE_RECOVERY
  EpochThreadShutdown(d->epoch);
#endif
  FlushThread();
  return NULL;
}

void catcher(int sig)
{
  printf("CAUGHT SIGNAL %d\n", sig);
}

int main(int argc, char **argv)
{
  struct option long_options[] = {
    // These options don't set a flag
    {"help",                      no_argument,       NULL, 'h'},
    {"table-size",                required_argument, NULL, 't'},
    {"duration",                  required_argument, NULL, 'd'},
    {"num-threads",               required_argument, NULL, 'n'},
    {"seed",                      required_argument, NULL, 's'},
    {"search-fraction",           required_argument, NULL, 'r'},
    {"insert-update-fraction",    required_argument, NULL, 'i'},
    {"delete-fraction",           required_argument, NULL, 'x'},
    {"keyspace1-size",             required_argument, NULL, 'k'},
    {NULL, 0, NULL, 0}
  };



  int i, c;
  char *s;
  unsigned long inserts, deletes, actInsert, actDelete;
  thread_data_t *data;
  pthread_t *threads;
  pthread_attr_t attr;
  barrier_t * barrier = new barrier_t;
  barrier_t * barrier2 = new barrier_t;
  
  struct timeval start, end, end1;
  struct timespec timeout;
  
  int duration = DEFAULT_DURATION;
  
  int nb_threads = DEFAULT_NB_THREADS;
  double search_frac = DEFAULT_SEARCH_FRAC;
  double insert_frac = DEFAULT_INSERT_FRAC;
  double delete_frac = DEFAULT_DELETE_FRAC;
  long keyspace1_size = DEFAULT_KEYSPACE1_SIZE;
  int seed = DEFAULT_SEED;
  sigset_t block_set;
  

  while(1) {
    i = 0;
    c = getopt_long(argc, argv, "hd:n:s:r:i:x:k:", long_options, &i);

    if(c == -1)
      break;

    if(c == 0 && long_options[i].flag == 0)
      c = long_options[i].val;

    switch(c) {
     case 0:
       /* Flag is automatically set */
       break;
     case 'h':
       printf("Balanced Search Tree\n"
              "\n"
              "Usage:\n"
              "  BST [options...]\n"
              "\n"
              "Options:\n"
              "  -h, --help\n"
              "        Print this message\n"
              "  -d, --duration <int>\n"
              "        Test duration in milliseconds (0=infinite, default=" XSTR(DEFAULT_DURATION) ")\n"
              "  -n, --num-threads <int>\n"
              "        Number of threads (default=" XSTR(DEFAULT_NB_THREADS) ")\n"
              "  -s, --seed <int>\n"
              "        RNG seed (0=time-based, default=" XSTR(DEFAULT_SEED) ")\n"
                "  -r, --search-fraction <int>\n"
              "        Number of search Threads (default=" XSTR(DEFAULT_SEARCH_FRAC) ")\n"
              "  -i, --insert-update-fraction <int>\n"
              "        Number of insert/update Threads (default=" XSTR(DEFAULT_INSERT_FRAC) ")\n"
              "  -x, --delete-fraction <double>\n"
              "        Fraction of delete operations (default=" XSTR(DEFAULT_DELETE_FRAC) ")\n"
              "  -k, --keyspace-size <int>\n"
               "       Number of possible keys (default=" XSTR(DEFAULT_KEYSPACE1_SIZE) ")\n"
         );
       exit(0);
     case 'd':
       duration = atoi(optarg);
       break;
     case 'n':
       nb_threads = atoi(optarg);
       break;
     case 's':
       seed = atoi(optarg);
       break;
     case 'r':
       search_frac = atof(optarg);
       break;
     case 'i':
       insert_frac = atof(optarg);
       break;
     case 'x':
       delete_frac = atof(optarg);
       break;
    case 'k':
       keyspace1_size = atoi(optarg);
       break;
     case '?':
       printf("Use -h or --help for help\n");
       exit(0);
     default:
       exit(1);
    }
  }

  assert(duration >= 0);
  assert(nb_threads > 0);
 
  printf("Duration       : %d\n", duration);
  printf("Nb threads     : %d\n", nb_threads);
  printf("Seed           : %d\n", seed);
  printf("Search frac    : %f\n", search_frac);     
  printf("Insert frac    : %f\n", insert_frac);
  printf("Delete frac    : %f\n", delete_frac);
  printf("Keyspace1 size : %ld\n", keyspace1_size);


  timeout.tv_sec = duration / 1000;
  timeout.tv_nsec = (duration % 1000) * 1000000;
  
  
  data = new thread_data_t[nb_threads+1];
  linkcache_t* lc = cache_create();
  EpochGlobalInit(lc);
  EpochThread epoch = EpochThreadInit(0);

  if ((threads = (pthread_t *)malloc(nb_threads * sizeof(pthread_t))) == NULL) {
    perror("malloc");
    exit(1);
  }

  if (seed == 0)
    srand((int)time(0));
  else
    srand(seed);


 

node_t * newRT = new node_t;
 node_t * newLC = new node_t;
 node_t * newRC = new node_t;
 
 newRT->key = keyspace1_size+2;
 newLC->key = keyspace1_size+1;
 newRC->key = keyspace1_size+2;
 
 
 newRT->child.AO_val1 = create_child_word(newLC,UNMARK, UNFLAG);
 newRT->child.AO_val2 = create_child_word(newRC,UNMARK, UNFLAG);
 
  
  stop = 0;

  //Pre-populate Tree-------------------------------------------------
  int pre_inserts = 2;
  int i1 = nb_threads;
  data[i1].id = i1+1;
  data[i1].numThreads = nb_threads;
  data[i1].numInsert = 0;
  data[i1].numActualDelete = 0;
  data[i1].seed = rand();
  data[i1].search_frac = 0.0;
  data[i1].insert_frac = 1.0;
  data[i1].delete_frac = 0.0;
  data[i1].keyspace1_size = keyspace1_size;
  data[i1].ops = 0;
  data[i1].rootOfTree = newRT;
  data[i1].barrier = barrier;
  data[i1].buffer = lc;
  data[i1].epoch = epoch;
    
#ifdef DETAILED_STATS
   data[i1].tot_reads = 0;
   data[i1].tot_fastins_time = 0;
   data[i1].tot_slowins_time = 0;
   data[i1].tot_fastins_count = 0;
   data[i1].tot_slowins_count = 0;
   data[i1].tot_fastdel_time = 0;
   data[i1].tot_slowdel_time = 0;
   data[i1].tot_fastdel_count = 0;
   data[i1].tot_slowdel_count = 0;
   data[i1].count = 0;
#endif
  
    
    // data[i1].recycledNodes.reserve(RECYCLED_VECTOR_RESERVE);
  data[i1].sr = new seekRecord_t;
  data[i1].ssr = new seekRecord_t;
        
  Word key;
  
  while(data[i1].numInsert < (keyspace1_size/2)){
      key = rand_r(&data[0].seed) % (keyspace1_size);
      while(key == 0){
      // Dont allow a key of 0 in the tree
        key = rand_r(&data[0].seed) % (keyspace1_size);
      } 
        insert(&data[i1],key);
  } 
  pre_inserts+= data[i1].numInsert; 
            
  //------------------------------------------------------------------- 
  std::cout << "pre_inserts = " << pre_inserts << std::endl;     
  
  barrier_init(barrier, nb_threads + 1);
  barrier_init(barrier2, nb_threads + 1);
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  
  for (i = 0; i < nb_threads; i++) {
    data[i].id = i+1;
    data[i].numThreads = nb_threads;
    data[i].numInsert = 0;
    data[i].numActualDelete = 0;
    data[i].seed = rand();
    data[i].search_frac = search_frac;
    data[i].insert_frac = insert_frac;
    data[i].delete_frac = delete_frac;
    data[i].keyspace1_size = keyspace1_size;
    data[i].ops = 0;
    data[i].rootOfTree = newRT;
    data[i].barrier = barrier;
    data[i].barrier2 = barrier2;
    // data[i].recycledNodes.reserve(RECYCLED_VECTOR_RESERVE);
    data[i].sr = new seekRecord_t;
    data[i].ssr = new seekRecord_t;
    data[i].buffer = lc;
   
#ifdef DETAILED_STATS     
    data[i].tot_reads = 0;
    data[i].tot_read_time = 0;
    data[i].tot_reads = 0;
    data[i].tot_fastins_time = 0;
    data[i].tot_slowins_time = 0;
    data[i].tot_fastins_count = 0;
    data[i].tot_slowins_count = 0;
    data[i].tot_fastdel_time = 0;
    data[i].tot_slowdel_time = 0;
    data[i].tot_fastdel_count = 0;
    data[i].tot_slowdel_count = 0;
    data[i].insertSeqNumber = 0;
    data[i].count = 0;
#endif
      
    if (pthread_create(&threads[i], &attr, testRW, (void *)(&data[i])) != 0) {
      fprintf(stderr, "Error creating thread\n");
        exit(1);
    }
  }
  
  pthread_attr_destroy(&attr); 

  /* Catch some signals */
 
  if (signal(SIGHUP, catcher) == SIG_ERR ||
      signal(SIGINT, catcher) == SIG_ERR ||
      signal(SIGTERM, catcher) == SIG_ERR) {
    perror("signal");
    exit(1);
  }

#if PREPROCESSING
  bool startsim = false;
  
  while(!startsim){  
     leafNodes = 0;
    barrier_cross(barrier);
    // let the threads run for a while.
    sleep(2);
    AO_store_full(&stop, 1);
    // check the size of the tree. Within 5% of steady state size
    in_order_visit(newRT);
    if((leafNodes > ((1.05)*(insert_frac*keyspace1_size)/(insert_frac+delete_frac))) || 
        (leafNodes < ((0.95)*(insert_frac*keyspace1_size)/(insert_frac+delete_frac)))){
        //  Not yet within steady state limits. Let it run again.
        // Wait until all worker threads reach barrier2
        while(barrier_peek(barrier2)){
          pthread_yield();
        }
        AO_store_full(&stop, 0);
        barrier_cross(barrier2);
    }
    else{
        // prepopulation done
        
        AO_store_full(&stop2, 1);
        // Wait until all worker threads reach barrier2
        while(barrier_peek(barrier2)){
          pthread_yield();
        }
        AO_store_full(&stop, 0);        
        barrier_cross(barrier2);
        startsim = true;
    }    
 } 
 #endif  
    
  barrier_cross(barrier);
  gettimeofday(&start, NULL);
  if (duration > 0) {
      nanosleep(&timeout, NULL);
  } 
  else {
    sigemptyset(&block_set);
    sigsuspend(&block_set);
  }
  
  AO_store_full(&stop, 1);    
  gettimeofday(&end, NULL);
  
  /* Wait for thread completion */
  for (i = 0; i < nb_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Error waiting for thread completion\n");
      exit(1);
    }
  } 
  
  // CONSISTENCY CHECK
  leafNodes = 0;    
  uint64_t rootkey =  in_order_visit((newRT));
  unsigned long tot_inserts = 0;
  unsigned long tot_deletes = 0;
  unsigned long tot_ops = 0;
   
#ifdef DETAILED_STATS
  unsigned long tot_reads = 0;
  double tot_read_time = 0;
  double tot_fastins_time = 0;
  double tot_slowins_time = 0;
  double tot_fastdel_time = 0;
  double tot_slowdel_time = 0;
  unsigned long tot_fastins_count = 0;
  unsigned long tot_slowins_count = 0;
  unsigned long tot_fastdel_count = 0;
  unsigned long tot_slowdel_count = 0;
  uint64_t inscases = 0;
    uint64_t inswin = 0;
#endif

  for(int i = 0; i < nb_threads; i++){
       tot_inserts += data[i].numInsert;
       tot_deletes += data[i].numActualDelete;
       
       tot_ops += data[i].ops;
       
#ifdef DETAILED_STATS
       tot_reads += data[i].tot_reads;
       tot_read_time += (data[i].tot_read_time);
       
       tot_fastins_time += (data[i].tot_fastins_time);
       tot_slowins_time += (data[i].tot_slowins_time);
       
    tot_fastdel_time += (data[i].tot_fastdel_time);
       tot_slowdel_time += (data[i].tot_slowdel_time);
       
       tot_fastins_count += data[i].tot_fastins_count; 
       tot_slowins_count += data[i].tot_slowins_count;
        
       tot_fastdel_count += data[i].tot_fastdel_count; 
       tot_slowdel_count += data[i].tot_slowdel_count;
#endif
    
  }
   
  tot_inserts+= pre_inserts;
  unsigned long tot_updates = 0;
  for(int i = 0; i < nb_threads; i++){
       tot_updates += data[i].ops;
  }
  std::cout << "Total Number of Nodes ( " << tot_inserts << ", " << tot_deletes << " ) = " << tot_inserts - tot_deletes << std::endl;
  std::cout << "Total Number of leaf nodes (sanity check) = " << leafNodes << std::endl;
   
  if((tot_inserts - tot_deletes) != leafNodes){
    std::cout << "ERROR. MISMATCH" << std::endl;
       exit(0);
   } 
   
  duration = (end.tv_sec * 1000 + end.tv_usec / 1000) - (start.tv_sec * 1000 + start.tv_usec / 1000);
  
  std::cout << "*--PERFORMANCE STATISTICS--------*" << std::endl;
  std::cout << "Total Operations per sec = " << (tot_ops * 1000)/duration << std::endl; 

#ifdef DETAILED_STATS
  std::cout << "Total reads = " << tot_reads << std::endl;
  std::cout << "Total updates = " << tot_updates << std::endl;
  std::cout << "AVG read time(usec) = " << tot_read_time/tot_reads << std::endl; 
  std::cout << "*--------------------------------------*" << std::endl;
 
  std::cout << "Number of Slow Insert opns = " << tot_slowins_count<< std::endl;
  std::cout << "Avg. Slow Insert Time(usec) = " << tot_slowins_time/tot_slowins_count<< std::endl;

  std::cout << "\n";
  std::cout << "Number of Fast Insert opns = " << tot_fastins_count<< std::endl;
  std::cout << "Avg. Fast Insert Time(usec) = " << tot_fastins_time/tot_fastins_count<< std::endl;
  std::cout << "\n";
  std::cout << "Number of Slow Delete opns = " << tot_slowdel_count<< std::endl;
  std::cout << "Avg. Slow Delete Time(usec) = " << tot_slowdel_time/tot_slowdel_count<< std::endl;
  std::cout << "\n";

  std::cout << "Number of Fast Delete opns = " << tot_fastdel_count<< std::endl;
  std::cout << "Avg. Fast Delete Time(usec) = " << tot_fastdel_time/tot_fastdel_count<< std::endl;
#endif
  
  ticks recovery_cycles = 0;

#ifdef ESTIMATE_RECOVERY

  FlushThread();
  active_page_table_t** page_tables = (active_page_table_t**)malloc(sizeof(active_page_table_t*) * (nb_threads));
  for (ULONG i = 0; i < nb_threads; i++) {
    page_tables[i] = data[i].page_table;
    fprintf(stderr, "page table %d has %u pages\n", i, page_tables[i]->current_size);
#ifdef DO_STATS
    fprintf(stderr, "marks %u, hits %u\n", page_tables[i]->num_marks, page_tables[i]->hits);
#endif
#ifdef MULTITHREADED_RECOVERY
        size_t num_pages = page_tables[i]->last_in_use;
        for (size_t j = 0;  j<num_pages; j++) {
            unique_pages.insert(page_tables[i]->pages[j].page);
        }
#endif
  }
  //page_tables[nb_threads] = (active_page_table_t*)GetOpaquePageBuffer(epoch);
  //fprintf(stderr, "page table %d has %u pages\n", args->threadCount, page_buffers[args->threadCount]->current_size);
//#ifdef DO_STATS
  //fprintf(stderr, "marks %u, hits %u\n", page_buffers[args->threadCount]->num_marks, page_buffers[args->threadCount]->hits);
//#endif
#ifdef MULTITHREADED_RECOVERY
    page_vector = std::vector<void*>(unique_pages.begin(), unique_pages.end());
    size_t pg_size= page_tables[0]->page_size; //assuming one size pages
    size_t total_pages = page_vector.size();

    int num_rec_threads=RECOVERY_THREADS;
    if (total_pages < num_rec_threads) {
        num_rec_threads = total_pages;
    }


  barrier_init(&barrier_global, num_rec_threads + 1);
  barrier_init(&barrier_local, num_rec_threads);

  pthread_t rec_threads[num_rec_threads];
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    
  rec_thread_info_t* rtds = (rec_thread_info_t*) malloc(num_rec_threads * sizeof(rec_thread_info_t));

  int rc;
  void *status;
  size_t thr;
  for(thr = 0; thr < num_rec_threads; thr++)
    {
      rtds[thr].id = thr;
      rtds[thr].size = total_pages;
      rtds[thr].seed = rand();
      rtds[thr].rec_threads = num_rec_threads;
      rtds[thr].root = newRT; 
      rtds[thr].page_size = pg_size;
      rc = pthread_create(&rec_threads[thr], &attr, rec, rtds + thr);
      if (rc)
  {
    printf("ERROR; return code from pthread_create() is %d\n", rc);
    exit(-1);
  }
        
    }
   MEM_BARRIER;
  barrier_cross(&barrier_global);

  for(thr = 0; thr < num_rec_threads; thr++) 
    {
      rc = pthread_join(rec_threads[thr], &status);
      if (rc) 
  {
    printf("ERROR; return code from pthread_join() is %d\n", rc);
    exit(-1);
  }
    }

    ticks max_duration = 0;
    for(thr = 0; thr < num_rec_threads; thr++)
    {
        if (rtds[thr].time > max_duration) { 
             max_duration = rtds[thr].time;
        }
    }

  printf("    Recovery takes (cycles): %llu\n", max_duration);
#else 

#ifdef FLUSH_CACHES
#define NUM_EL 8388608
    volatile uint64_t* elms = (volatile uint64_t*) malloc (NUM_EL*sizeof(uint64_t));

    size_t el;
    unsigned int useed = (unsigned int) seed;
    for (size_t k = 0; k < NUM_EL; k++) {
      el = rand_r(&useed) % (NUM_EL);
      //el = (my_random(&(seeds[0]), &(seeds[1]), &(seeds[2])) % (NUM_EL - 1));
       elms[el] = el+2;
    }
#endif

  volatile ticks corr = getticks_correction_calc();
  ticks startCycles = getticks();
  recover(&data[nb_threads], page_tables, nb_threads);
  ticks endCycles = getticks();

  recovery_cycles = endCycles - startCycles + corr;
  free(page_tables);
  active_page_table_t* pb = create_active_page_table(nb_threads+1);

  SetOpaquePageBuffer(epoch, pb);
#endif
#else
  EpochThreadShutdown(epoch);
#endif

#define LLU long long unsigned int
#ifndef MULTITHREADED_RECOVERY
  printf("    Recovery takes (cycles): %llu\n", (LLU)recovery_cycles);
#endif


#ifdef ESTIMATE_RECOVERY
  destroy_active_page_table((active_page_table_t*)GetOpaquePageBuffer(epoch));

#ifdef FLUSH_CACHES
#ifndef MULTITHREADED_RECOVERY
    free((void*)elms);
#endif
#endif
#endif

  free(threads);
  delete [] data;
  return 0;
}
