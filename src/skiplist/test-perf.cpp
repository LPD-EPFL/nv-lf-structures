
#include <assert.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sched.h>
#include <inttypes.h>
#include <sys/time.h>
#include <unistd.h>
#include <malloc.h>
#include <vector>
#include <set>

using namespace std;

#include <nv_utils.h>

#include "lf-skiplist.h"

#define FLUSH_CACHES 1

/* ################################################################### *
 * Definition of macros: per data structure
 * ################################################################### */

#define DS_CONTAINS(s,k,e,c)  skiplist_find(s,k,e,c)
#define DS_ADD(s,k,v,e,c)     skiplist_insert(s, k,(k+4),e,c)
#define DS_REMOVE(s, k, e, c)    skiplist_remove(s, k, e, c)
#define DS_SIZE(s)          skiplist_size(s)
#define DS_NEW(e)            new_skiplist(e)
#define DS_DELETE(s)         delete_skiplist(s)

#define DS_IS_REACHABLE(s,a) is_reachable(s,a)
#define DS_RECOVER(s,p,n) recover(s,p,n)

#define DS_TYPE             skiplist_t
#define DS_NODE             node_t
#define DS_KEY              skey_t


/* ################################################################### *
 * GLOBALS
 * ################################################################### */

barrier_t barrier, barrier_global;
RETRY_STATS_VARS_GLOBAL;

size_t initial = DEFAULT_INITIAL;
size_t range = DEFAULT_RANGE; 
size_t update = DEFAULT_UPDATE;
size_t num_threads = DEFAULT_NB_THREADS; 
size_t duration = DEFAULT_DURATION;

size_t print_vals_num = 100; 
size_t pf_vals_num = 1023;
size_t put, put_explicit = false;
double update_rate, put_rate, get_rate;

size_t size_after = 0;
int seed = 0;
__thread unsigned long * seeds;
uint32_t rand_max;
#define rand_min 1

static volatile int stop;
TEST_VARS_GLOBAL;

volatile ticks *putting_succ;
volatile ticks *putting_fail;
volatile ticks *getting_succ;
volatile ticks *getting_fail;
volatile ticks *removing_succ;
volatile ticks *removing_fail;
volatile ticks *putting_count;
volatile ticks *putting_count_succ;
volatile ticks *getting_count;
volatile ticks *getting_count_succ;
volatile ticks *removing_count;
volatile ticks *removing_count_succ;
volatile ticks *total;

#ifdef ESTIMATE_RECOVERY
#define MULTITHREADED_RECOVERY 1
#define RECOVERY_THREADS 48
typedef struct rec_thread_info_t{
    uint32_t id;
    size_t size;
    size_t rec_threads;
    size_t page_size;
    DS_TYPE* set; 
    ticks time;
} rec_thread_info_t;
std::set<void*> unique_pages;
std::vector<void*> page_vector;

void*
rec(void* thread) 
{
  //pthread_exit(NULL);
  rec_thread_info_t* td = (rec_thread_info_t*) thread;
  uint32_t ID = td->id;
  size_t rec_threads = td->rec_threads;
  size_t size = td->size;
  size_t page_size = td->page_size;
  ticks duration;
  DS_TYPE* set = td->set;
  size_t i = ID;
  size_t k;
  size_t nodes_per_page;

  //fprintf(stderr, "thread %d, total size %lu\n", ID, size);
#ifdef FLUSH_CACHES
#define NUM_EL 8388608
    volatile uint64_t* elms = (volatile uint64_t*) malloc (NUM_EL*sizeof(uint64_t));

  seeds = seed_rand();
    size_t el;
    for (k = 0; k < NUM_EL; k++) {
      el = (my_random(&(seeds[0]), &(seeds[1]), &(seeds[2])) % (NUM_EL - 1));
      //el = (k * (NUM_EL - k)) % NUM_EL;
       elms[el] = el+2;
    }
#endif

  barrier_cross(&barrier_global);

  volatile ticks corr = getticks_correction_calc();
  ticks startCycles = getticks();

  while (i < size) {
	void * crt_address = page_vector[i]; 
	nodes_per_page = page_size / sizeof(DS_NODE);
	for (k = 0; k < nodes_per_page; k++) {
        void * node_address = (void*)((UINT_PTR)crt_address + (CACHE_LINES_PER_NV_NODE*CACHE_LINE_SIZE*k));
		if (!NodeMemoryIsFree(node_address)) {
			if (!is_reachable(set, node_address)) {
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

  barrier_cross(&barrier);
  //fprintf(stderr, "recovery took %llu\n", duration);
  pthread_exit(NULL);

}
#endif

/* ################################################################### *
 * LOCALS
 * ################################################################### */


typedef struct thread_data
{
  uint32_t id;
  DS_TYPE* set;
  active_page_table_t* page_table;
  linkcache_t* lc;
} thread_data_t;

void*
test(void* thread) 
{
  thread_data_t* td = (thread_data_t*) thread;
  uint32_t ID = td->id;
  linkcache_t * lc = td->lc;
  set_cpu(ID);
  //DS_LOCAL();

  DS_TYPE* set = td->set;

  THREAD_INIT(ID);
  PF_INIT(3, SSPFD_NUM_ENTRIES, ID);

#if defined(COMPUTE_LATENCY)
  volatile ticks my_putting_succ = 0;
  volatile ticks my_putting_fail = 0;
  volatile ticks my_getting_succ = 0;
  volatile ticks my_getting_fail = 0;
  volatile ticks my_removing_succ = 0;
  volatile ticks my_removing_fail = 0;
#endif
  uint64_t my_putting_count = 0;
  uint64_t my_getting_count = 0;
  uint64_t my_removing_count = 0;

  uint64_t my_putting_count_succ = 0;
  uint64_t my_getting_count_succ = 0;
  uint64_t my_removing_count_succ = 0;
    
#if defined(COMPUTE_LATENCY) && PFD_TYPE == 0
  volatile ticks start_acq, end_acq;
  volatile ticks correction = getticks_correction_calc();
#endif
    
  seeds = seed_rand();
    
    EpochThread epoch = EpochThreadInit(ID);
	td->page_table = (active_page_table_t*)GetOpaquePageBuffer(epoch);

  barrier_cross(&barrier);

  DS_KEY key;
  uint32_t c = 0;
  uint32_t scale_rem = (uint32_t) (update_rate * UINT_MAX);
  uint32_t scale_put = (uint32_t) (put_rate * UINT_MAX);

  int i;
  uint32_t num_elems_thread = (uint32_t) (initial / num_threads);
  uint32_t missing = (uint32_t) initial - (num_elems_thread * num_threads);
  if (ID < missing)
    {
      num_elems_thread++;
    }

#if INITIALIZE_FROM_ONE == 1
  num_elems_thread = (ID == 0) * initial;
#endif    

  for(i = 0; i < (int64_t) num_elems_thread; i++) 
    {
      key = (my_random(&(seeds[0]), &(seeds[1]), &(seeds[2])) % (rand_max + 1)) + rand_min;
      
      if(DS_ADD(set,key,key,epoch,lc) == 0)
	{
	  i--;
	}
    }
  MEM_BARRIER;

  barrier_cross(&barrier);

  if (!ID)
    {
      printf("#BEFORE size is: %zu\n", (size_t) DS_SIZE(set));
    }

  RETRY_STATS_ZERO();

  barrier_cross(&barrier_global);


  while (stop == 0) 
    {
      TEST_LOOP(NULL);
    }

  barrier_cross(&barrier);

  if (!ID)
    {
      size_after = DS_SIZE(set);
      printf("#AFTER  size is: %zu\n", size_after);
    }

  barrier_cross(&barrier);

#if defined(COMPUTE_LATENCY)
  putting_succ[ID] += my_putting_succ;
  putting_fail[ID] += my_putting_fail;
  getting_succ[ID] += my_getting_succ;
  getting_fail[ID] += my_getting_fail;
  removing_succ[ID] += my_removing_succ;
  removing_fail[ID] += my_removing_fail;
#endif
  putting_count[ID] += my_putting_count;
  getting_count[ID] += my_getting_count;
  removing_count[ID]+= my_removing_count;

  putting_count_succ[ID] += my_putting_count_succ;
  getting_count_succ[ID] += my_getting_count_succ;
  removing_count_succ[ID]+= my_removing_count_succ;

  EXEC_IN_DEC_ID_ORDER(ID, num_threads)
    {
      print_latency_stats(ID, SSPFD_NUM_ENTRIES, print_vals_num);
      RETRY_STATS_SHARE();
    }
  EXEC_IN_DEC_ID_ORDER_END(&barrier);

#ifndef ESTIMATE_RECOVERY
  EpochThreadShutdown(epoch);
#endif
    FlushThread();

  SSPFDTERM();
  THREAD_END();
  pthread_exit(NULL);
}

int
main(int argc, char **argv) 
{
  set_cpu(0);
  seeds = seed_rand();

  struct option long_options[] = {
    // These options don't set a flag
    {"help",                      no_argument,       NULL, 'h'},
    {"duration",                  required_argument, NULL, 'd'},
    {"initial-size",              required_argument, NULL, 'i'},
    {"num-threads",               required_argument, NULL, 'n'},
    {"range",                     required_argument, NULL, 'r'},
    {"update-rate",               required_argument, NULL, 'u'},
    {"num-buckets",               required_argument, NULL, 'b'},
    {"print-vals",                required_argument, NULL, 'v'},
    {"vals-pf",                   required_argument, NULL, 'f'},
    {NULL, 0, NULL, 0}
  };

  int i, c;
  while(1) 
    {
      i = 0;
      c = getopt_long(argc, argv, "hAf:d:i:n:r:s:u:m:a:p:b:v:f:", long_options, &i);
		
      if(c == -1)
	break;
		
      if(c == 0 && long_options[i].flag == 0)
	c = long_options[i].val;
		
      switch(c) 
	{
	case 0:
	  /* Flag is automatically set */
	  break;
	case 'h':
	  printf("nv-lf-structures -- stress test "
		 "\n"
		 "\n"
		 "Usage:\n"
		 "  %s [options...]\n"
		 "\n"
		 "Options:\n"
		 "  -h, --help\n"
		 "        Print this message\n"
		 "  -d, --duration <int>\n"
		 "        Test duration in milliseconds\n"
		 "  -i, --initial-size <int>\n"
		 "        Number of elements to insert before test\n"
		 "  -n, --num-threads <int>\n"
		 "        Number of threads\n"
		 "  -r, --range <int>\n"
		 "        Range of integer values inserted in set\n"
		 "  -u, --update-rate <int>\n"
		 "        Percentage of update transactions\n"
		 "  -p, --put-rate <int>\n"
		 "        Percentage of put update transactions (should be less than percentage of updates)\n"
		 "  -b, --num-buckets <int>\n"
		 "        Number of initial buckets (stronger than -l)\n"
		 "  -v, --print-vals <int>\n"
		 "        When using detailed profiling, how many values to print.\n"
		 "  -f, --val-pf <int>\n"
		 "        When using detailed profiling, how many values to keep track of.\n"
		 , argv[0]);
	  exit(0);
	case 'd':
	  duration = atoi(optarg);
	  break;
	case 'i':
	  initial = atoi(optarg);
	  break;
	case 'n':
	  num_threads = atoi(optarg);
	  break;
	case 'r':
	  range = atol(optarg);
	  break;
	case 'u':
	  update = atoi(optarg);
	  break;
	case 'p':
	  put_explicit = 1;
	  put = atoi(optarg);
	  break;
	case 'v':
	  print_vals_num = atoi(optarg);
	  break;
	case 'f':
	  pf_vals_num = pow2roundup(atoi(optarg)) - 1;
	  break;
	case '?':
	default:
	  printf("Use -h or --help for help\n");
	  exit(1);
	}
    }


  if (!is_power_of_two(initial))
    {
      size_t initial_pow2 = pow2roundup(initial);
      printf("** rounding up initial (to make it power of 2): old: %zu / new: %zu\n", initial, initial_pow2);
      initial = initial_pow2;
    }

  if (range < initial)
    {
      range = 2 * initial;
    }

  printf("## Initial: %zu / Range: %zu /\n", initial, range);

  double kb = initial * sizeof(DS_NODE) / 1024.0;
  double mb = kb / 1024.0;
  printf("Sizeof initial: %.2f KB = %.2f MB\n", kb, mb);

  if (!is_power_of_two(range))
    {
      size_t range_pow2 = pow2roundup(range);
      printf("** rounding up range (to make it power of 2): old: %zu / new: %zu\n", range, range_pow2);
      range = range_pow2;
    }

  if (put > update)
    {
      put = update;
    }

  update_rate = update / 100.0;

  if (put_explicit)
    {
      put_rate = put / 100.0;
    }
  else
    {
      put_rate = update_rate / 2;
    }



  get_rate = 1 - update_rate;

  /* printf("num_threads = %u\n", num_threads); */
  /* printf("cap: = %u\n", num_buckets); */
  /* printf("num elem = %u\n", num_elements); */
  /* printf("filing rate= %f\n", filling_rate); */
  /* printf("update = %f (putting = %f)\n", update_rate, put_rate); */


  rand_max = range - 1;
    
  struct timeval start, end;
  struct timespec timeout;
  timeout.tv_sec = duration / 1000;
  timeout.tv_nsec = (duration % 1000) * 1000000;
    
  stop = 0;


	linkcache_t* lc = cache_create();
	EpochGlobalInit(lc);

    EpochThread epoch = EpochThreadInit(num_threads);
  DS_TYPE* set = DS_NEW(epoch);
  assert(set != NULL);

  /* Initializes the local data */
  putting_succ = (ticks *) calloc(num_threads , sizeof(ticks));
  putting_fail = (ticks *) calloc(num_threads , sizeof(ticks));
  getting_succ = (ticks *) calloc(num_threads , sizeof(ticks));
  getting_fail = (ticks *) calloc(num_threads , sizeof(ticks));
  removing_succ = (ticks *) calloc(num_threads , sizeof(ticks));
  removing_fail = (ticks *) calloc(num_threads , sizeof(ticks));
  putting_count = (ticks *) calloc(num_threads , sizeof(ticks));
  putting_count_succ = (ticks *) calloc(num_threads , sizeof(ticks));
  getting_count = (ticks *) calloc(num_threads , sizeof(ticks));
  getting_count_succ = (ticks *) calloc(num_threads , sizeof(ticks));
  removing_count = (ticks *) calloc(num_threads , sizeof(ticks));
  removing_count_succ = (ticks *) calloc(num_threads , sizeof(ticks));
    
  pthread_t threads[num_threads];
  pthread_attr_t attr;
  int rc;
  void *status;
    
  barrier_init(&barrier_global, num_threads + 1);
  barrier_init(&barrier, num_threads);
    
  /* Initialize and set thread detached attribute */
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    
  thread_data_t* tds = (thread_data_t*) malloc(num_threads * sizeof(thread_data_t));
#ifdef ESTIMATE_RECOVERY
	active_page_table_t** page_tables = (active_page_table_t**)malloc(sizeof(active_page_table_t*) * (num_threads));
#endif

  size_t t;
  for(t = 0; t < num_threads; t++)
    {
      tds[t].id = t;
      tds[t].set = set;
      tds[t].page_table = NULL;
      tds[t].lc = lc;
      rc = pthread_create(&threads[t], &attr, test, tds + t);
      if (rc)
	{
	  printf("ERROR; return code from pthread_create() is %d\n", rc);
	  exit(-1);
	}
        
    }
    
  /* Free attribute and wait for the other threads */
  pthread_attr_destroy(&attr);
    
  barrier_cross(&barrier_global);
  gettimeofday(&start, NULL);
  nanosleep(&timeout, NULL);

  stop = 1;
  gettimeofday(&end, NULL);
  duration = (end.tv_sec * 1000 + end.tv_usec / 1000) - (start.tv_sec * 1000 + start.tv_usec / 1000);
    
  for(t = 0; t < num_threads; t++) 
    {
      rc = pthread_join(threads[t], &status);
      if (rc) 
	{
	  printf("ERROR; return code from pthread_join() is %d\n", rc);
	  exit(-1);
	}
    }

    
  volatile ticks putting_suc_total = 0;
  volatile ticks putting_fal_total = 0;
  volatile ticks getting_suc_total = 0;
  volatile ticks getting_fal_total = 0;
  volatile ticks removing_suc_total = 0;
  volatile ticks removing_fal_total = 0;
  volatile uint64_t putting_count_total = 0;
  volatile uint64_t putting_count_total_succ = 0;
  volatile uint64_t getting_count_total = 0;
  volatile uint64_t getting_count_total_succ = 0;
  volatile uint64_t removing_count_total = 0;
  volatile uint64_t removing_count_total_succ = 0;
    
  for(t=0; t < num_threads; t++) 
    {
      PRINT_OPS_PER_THREAD();
      putting_suc_total += putting_succ[t];
      putting_fal_total += putting_fail[t];
      getting_suc_total += getting_succ[t];
      getting_fal_total += getting_fail[t];
      removing_suc_total += removing_succ[t];
      removing_fal_total += removing_fail[t];
      putting_count_total += putting_count[t];
      putting_count_total_succ += putting_count_succ[t];
      getting_count_total += getting_count[t];
      getting_count_total_succ += getting_count_succ[t];
      removing_count_total += removing_count[t];
      removing_count_total_succ += removing_count_succ[t];
    }


    ticks recovery_cycles = 0;

#ifdef ESTIMATE_RECOVERY
    FlushThread();
  nanosleep(&timeout, NULL);
	for (ULONG i = 0; i < num_threads; i++) {
		page_tables[i] = tds[i].page_table;
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

	//page_tables[num_threads] = (active_page_table_t*)GetOpaquePageBuffer(epoch);
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
  barrier_init(&barrier, num_rec_threads);

  pthread_t rec_threads[num_rec_threads];
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    
  rec_thread_info_t* rtds = (rec_thread_info_t*) malloc(num_rec_threads * sizeof(rec_thread_info_t));


  //size_t t;
  for(t = 0; t < num_rec_threads; t++)
    {
      rtds[t].id = t;
      rtds[t].size = total_pages;
      rtds[t].rec_threads = num_rec_threads;
      rtds[t].set = set; 
      rtds[t].page_size = pg_size;
      rc = pthread_create(&rec_threads[t], &attr, rec, rtds + t);
      if (rc)
	{
	  printf("ERROR; return code from pthread_create() is %d\n", rc);
	  exit(-1);
	}
        
    }
   MEM_BARRIER;
  barrier_cross(&barrier_global);

  for(t = 0; t < num_rec_threads; t++) 
    {
      rc = pthread_join(rec_threads[t], &status);
      if (rc) 
	{
	  printf("ERROR; return code from pthread_join() is %d\n", rc);
	  exit(-1);
	}
    }

    ticks max_duration = 0;
    for(t = 0; t < num_rec_threads; t++)
    {
        if (rtds[t].time > max_duration) { 
             max_duration = rtds[t].time;
        }
    }

	printf("    Recovery takes (cycles): %llu\n", max_duration);
#else 

#ifdef FLUSH_CACHES
#define NUM_EL 8388608
    volatile uint64_t* elms = (volatile uint64_t*) malloc (NUM_EL*sizeof(uint64_t));

  seeds = seed_rand();
    size_t el;
    for (k = 0; k < NUM_EL; k++) {
      el = (my_random(&(seeds[0]), &(seeds[1]), &(seeds[2])) % (NUM_EL - 1));
       elms[el] = el+2;
    }
#endif
	//page_tables[num_threads] = (active_page_table_t*)GetOpaquePageBuffer(epoch);
	//fprintf(stderr, "page table %d has %u pages\n", args->threadCount, page_buffers[args->threadCount]->current_size);
//#ifdef DO_STATS
	//fprintf(stderr, "marks %u, hits %u\n", page_buffers[args->threadCount]->num_marks, page_buffers[args->threadCount]->hits);
//#endif
    volatile ticks corr = getticks_correction_calc();
	ticks startCycles = getticks();
	DS_RECOVER(set, page_tables, num_threads);
	ticks endCycles = getticks();

	recovery_cycles = endCycles - startCycles + corr;

    
	for (ULONG i = 0; i < num_threads; i++) {
         //fprintf(stderr, "destroying %d\n",i);
        //destroy_active_page_table(page_tables[i]);
	}

    MEM_BARRIER;
	free(page_tables);
	active_page_table_t* pb = create_active_page_table(num_threads);

	SetOpaquePageBuffer(epoch, pb);
#endif
#else
  EpochThreadShutdown(epoch);
#endif

  nanosleep(&timeout, NULL);

#if defined(COMPUTE_LATENCY)
  printf("#thread srch_suc srch_fal insr_suc insr_fal remv_suc remv_fal   ## latency (in cycles) \n"); fflush(stdout);
  long unsigned get_suc = (getting_count_total_succ) ? getting_suc_total / getting_count_total_succ : 0;
  long unsigned get_fal = (getting_count_total - getting_count_total_succ) ? getting_fal_total / (getting_count_total - getting_count_total_succ) : 0;
  long unsigned put_suc = putting_count_total_succ ? putting_suc_total / putting_count_total_succ : 0;
  long unsigned put_fal = (putting_count_total - putting_count_total_succ) ? putting_fal_total / (putting_count_total - putting_count_total_succ) : 0;
  long unsigned rem_suc = removing_count_total_succ ? removing_suc_total / removing_count_total_succ : 0;
  long unsigned rem_fal = (removing_count_total - removing_count_total_succ) ? removing_fal_total / (removing_count_total - removing_count_total_succ) : 0;
  printf("%-7zu %-8lu %-8lu %-8lu %-8lu %-8lu %-8lu\n", num_threads, get_suc, get_fal, put_suc, put_fal, rem_suc, rem_fal);
#endif
    
#define LLU long long unsigned int

  int UNUSED pr = (int) (putting_count_total_succ - removing_count_total_succ);
  if (size_after != (initial + pr))
    {
      printf("// WRONG size. %zu + %d != %zu\n", initial, pr, size_after);
      assert(size_after == (initial + pr));
    }

  printf("    : %-10s | %-10s | %-11s | %-11s | %s\n", "total", "success", "succ %", "total %", "effective %");
  uint64_t total = putting_count_total + getting_count_total + removing_count_total;
  double putting_perc = 100.0 * (1 - ((double)(total - putting_count_total) / total));
  double putting_perc_succ = (1 - (double) (putting_count_total - putting_count_total_succ) / putting_count_total) * 100;
  double getting_perc = 100.0 * (1 - ((double)(total - getting_count_total) / total));
  double getting_perc_succ = (1 - (double) (getting_count_total - getting_count_total_succ) / getting_count_total) * 100;
  double removing_perc = 100.0 * (1 - ((double)(total - removing_count_total) / total));
  double removing_perc_succ = (1 - (double) (removing_count_total - removing_count_total_succ) / removing_count_total) * 100;
  printf("srch: %-10llu | %-10llu | %10.1f%% | %10.1f%% | \n", (LLU) getting_count_total, 
	 (LLU) getting_count_total_succ,  getting_perc_succ, getting_perc);
  printf("insr: %-10llu | %-10llu | %10.1f%% | %10.1f%% | %10.1f%%\n", (LLU) putting_count_total, 
	 (LLU) putting_count_total_succ, putting_perc_succ, putting_perc, (putting_perc * putting_perc_succ) / 100);
  printf("rems: %-10llu | %-10llu | %10.1f%% | %10.1f%% | %10.1f%%\n", (LLU) removing_count_total, 
	 (LLU) removing_count_total_succ, removing_perc_succ, removing_perc, (removing_perc * removing_perc_succ) / 100);

  double throughput = (putting_count_total + getting_count_total + removing_count_total) * 1000.0 / duration;
  printf("#txs %zu\t(%-10.0f\n", num_threads, throughput);
  printf("#Mops %.3f\n", throughput / 1e6);

  RETRY_STATS_PRINT(total, putting_count_total, removing_count_total, putting_count_total_succ + removing_count_total_succ);    
    
#ifndef MULTITHREADED_RECOVERY
	printf("    Recovery takes (cycles): %llu\n", (LLU)recovery_cycles);
#endif

    DS_DELETE(set);
#ifdef ESTIMATE_RECOVERY
	destroy_active_page_table((active_page_table_t*)GetOpaquePageBuffer(epoch));

#ifdef FLUSH_CACHES
#ifndef MULTITHREADED_RECOVERY
    free((void*)elms);
#endif
#endif
#endif

  cache_destroy(lc);
  free(tds);
  pthread_exit(NULL);
    
  return 0;
}
