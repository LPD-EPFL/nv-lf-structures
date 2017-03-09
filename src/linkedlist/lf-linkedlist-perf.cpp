//*********************************************************************
// Copyright (c) Microsoft Corporation.
//
// @File: PmemLinkedList_perf.cpp
// @Owner: t-alekd
//
// Purpose:
//
//  Measure performance of several linkedlist implementations based
//  on Stm and StmSpec interfaces.
//  
// Notes:
//
// @EndHeader@
//*********************************************************************

#include <testutil\precomp.h>

static const ULONG MAX_THREADS = 512;

//----------------------------------------------------------------------------
// Parameters passed from command line
//
struct PmemLinkedListArgs {
	// ratio of insert and remove operations
	// the ratio of inserts and removes is equal to have roughly constant
	// set size
	ULONG insertRemoveRatio;

	// maximum value inserted into the set
	ULONG maxValue;

	// how long to run the experiment
	ULONG durationMs;

	// how many threads to run
	ULONG threadCount;

	// default values
	static const ULONG DEFAULT_INSERT_REMOVE_RATIO = 5;
	static const ULONG DEFAULT_MAX_VALUE = 65535;
	static const ULONG DEFAULT_DURATION_MS = 10000;
	static const ULONG DEFAULT_THREAD_COUNT = 1;

	// value limits
	static const ULONG MAX_INSERT_REMOVE_RATIO = 50;

	// command line keys
	static const char *KEY_INSERT_REMOVE_RATIO;
	static const char *KEY_MAX_VALUE;
	static const char *KEY_DURATION_MS;
	static const char *KEY_THREAD_COUNT;
};

const char *PmemLinkedListArgs::KEY_INSERT_REMOVE_RATIO = "insremrat";
const char *PmemLinkedListArgs::KEY_MAX_VALUE = "maxva";
const char *PmemLinkedListArgs::KEY_DURATION_MS = "time";
const char *PmemLinkedListArgs::KEY_THREAD_COUNT = "threads";


//----------------------------------------------------------------------------
// Data structure passed to the thread
//
struct OperationStats {
	// total operations
	UINT64 operationCount;

	// total lookups
	UINT64 lookupCountTotal;
	// successful lookups
	UINT64 lookupCountSucc;
	// failed lookups
	UINT64 lookupCountFail;

	// total inserts
	UINT64 insertCountTotal;
	// successful inserts
	UINT64 insertCountSucc;
	// failed inserts
	UINT64 insertCountFail;

	// total removes
	UINT64 removeCountTotal;
	// successful removes
	UINT64 removeCountSucc;
	// failed removes
	UINT64 removeCountFail;

	// total sum of elements inserted into the list
	LONG sum;
};

struct ThreadInfo
{
	BYTE pad1[CACHE_LINE_SIZE];

	// shared linkedlist
	linkedlist_t *linkedlist;

	// threshold for insert operation
	ULONG insertThreshold;

	// threshold for remove operation
	ULONG removeThreshold;

	// max value to insert into linkedlist
	ULONG maxValue;

	// stats about executed operations
	OperationStats opStats;

	// JoinAllStart - Used to synchronize test startup.
	JOIN_ALL* JoinAllStart;

	// JoinAllEnd - Used to synchronize worker finishing.
	JOIN_ALL* JoinAllEnd;

	// system assigned thread id.
	DWORD ThreadId;

	// test-assigned value for the thread.
	DWORD ThreadNdx;

	// handle associated with this thread.
	HANDLE Thread;

	page_buffer_t* page_buffer;

	BYTE pad2[CACHE_LINE_SIZE];
};

//--------------------------------------------------------------
// some global data
//

// flag to stop worker threads
static PMEM_CACHE_ALIGNED union
{
	volatile bool done;
	BYTE pad[CACHE_LINE_SIZE];
} doneFlag;

// shared linkedlist
struct SharedLinkedList {
	static linkedlist_t* linkedlist;
};

PMEM_CACHE_ALIGNED linkedlist_t* SharedLinkedList::linkedlist;

PMEM_CACHE_ALIGNED flushbuffer_t* buffer;


// Main thread function
//
__checkReturn DWORD WINAPI __callback  
llWorkerThread(__in void* lpParam)
{
	HRESULT hr = S_OK;
	ThreadInfo *info = (ThreadInfo *)lpParam;
	linkedlist_t* ll = info->linkedlist;
	UtRandomGenerator rndgen;

	// initiailize Epochs
	EpochThread epoch = EpochThreadInit();

	info->page_buffer = (page_buffer_t*) GetOpaquePageBuffer(epoch);
	// wait for all threads to initialize.
	hr = JoinAllWait(info->JoinAllStart);

	if (FAILED(hr))
	{
		goto exit;
	}

	// maximum
	static const ULONG MAX_OPERATION_RND = 100;
	int opSucc;


	while (!doneFlag.done)
	{
		// execute next operation
		ULONG rndop = rndgen.Generate() % MAX_OPERATION_RND;
		skey_t rndkey = rndgen.Generate() % (info->maxValue-1) + 1; //0 used as special value in this implementation
		svalue_t rndval = rndkey;
		if(rndop < info->insertThreshold) {
			// perform insert
			opSucc = linkedlist_insert(ll, rndkey, rndval, epoch, buffer);
			if(opSucc) {
				info->opStats.insertCountSucc++;
				info->opStats.sum += rndkey;
			} else {
				info->opStats.insertCountFail++;
			}

			info->opStats.insertCountTotal++;
		} else if(rndop < info->removeThreshold) {
			// perform remove
			opSucc = (linkedlist_remove(ll, rndkey, epoch, buffer)!= 0);

			if(opSucc) {
				info->opStats.removeCountSucc++;
				info->opStats.sum -= rndkey;
			} else {
				info->opStats.removeCountFail++;
			}

			info->opStats.removeCountTotal++;
		} else {
			// perform lookup
			opSucc = (linkedlist_find(ll, rndkey, epoch, buffer) != 0);

			if(opSucc) {
				info->opStats.lookupCountSucc++;
			} else {
				info->opStats.lookupCountFail++;
			}

			info->opStats.lookupCountTotal++;
		}

		info->opStats.operationCount++;
	}

	
	// wait for all threads to finish
	hr = JoinAllWait(info->JoinAllEnd);

	if (FAILED(hr))
	{
		goto exit;
	}

exit:
	EpochThreadShutdown(epoch);

	return hr;
}

// This is the main function for the test.
__checkReturn HRESULT runTest(
	__in const HkStmUnitTest* test,
	__in PmemLinkedListArgs *args)
{
    HRESULT hr = S_OK;
	bool joinStartInit = false;
	bool joinEndInit = false;
	bool hkThreadInit = false;
	// needed to initialize the linkedlist
	UtRandomGenerator rndgen;


	// Create the synchronization point for worker thread startup/shutdown.
	//
	JOIN_ALL joinStart;
	EpochThread epoch = EpochThreadInit();

	// initialize shared linkedlist
	SharedLinkedList::linkedlist = new_linkedlist(epoch);
	
	hr = JoinAllInit(&joinStart, args->threadCount + 1);

	if(FAILED(hr)) goto exit;

	joinStartInit = true;

	JOIN_ALL joinEnd;
	
	hr = JoinAllInit(&joinEnd, args->threadCount);

	if(FAILED(hr)) goto exit;

	joinEndInit = true;

	// Crate and initialize CPU group information.
	//
	HkStmTestCpuGroups cpuGroups;
	HkStmTestCpuGroupsInit(&cpuGroups);

	//
	// Create all threads.
	//
	ThreadInfo info[MAX_THREADS];
	memset(&info, 0, sizeof(info));

	for (ULONG i = 0; i < args->threadCount; i++)
	{
		info[i].ThreadNdx = i + 1;
		info[i].JoinAllStart = &joinStart;
		info[i].JoinAllEnd = &joinEnd;

		info[i].linkedlist = SharedLinkedList::linkedlist;
		info[i].insertThreshold = args->insertRemoveRatio;
		info[i].removeThreshold = args->insertRemoveRatio * 2;
		info[i].maxValue = args->maxValue;

		info[i].Thread = CreateThread(
			NULL, 
			0, 
			llWorkerThread, 
			&info[i], 
			0, 
			&info[i].ThreadId);

		if (NULL == info[i].Thread)
		{
			hr = HRESULT_FROM_WIN32(GetLastError());
			if(FAILED(hr)) goto exit;
		}

		// set affinity
		HkStmTestCpuGroupsSetAffinity(&cpuGroups, info[i].Thread, i);
	}

	// Initialize the linkedlist by inserting half of the maxValue
	// elements into it.

	hkThreadInit = true;

	ULONG inserted = 0;
	LONG insertedSum = 0;

	linkedlist_t* sl = SharedLinkedList::linkedlist;

	while(inserted < args->maxValue / 2) {
		skey_t id = rndgen.Generate() % (args->maxValue-1) + 1;

		int opSucc;

		opSucc = linkedlist_insert(sl, id, id, epoch, buffer);

		if(opSucc) {
			inserted++;
			insertedSum += id;
		}
	}

	// Wait to complete initalization.
	//
	hr = JoinAllWait(&joinStart);

	if(FAILED(hr)) goto exit;

	// Let the worker threads run.
	//
	Sleep(args->durationMs);

	// Notify workers of shutdown.
	//
	doneFlag.done = true;

	// Wait for worker thread completion.
	//
	for (ULONG ndx = 0; ndx < args->threadCount; ++ndx)
	{
		if (WAIT_OBJECT_0 != WaitForSingleObject(info[ndx].Thread, INFINITE))
		{
			hr = HRESULT_FROM_WIN32(GetLastError());

			if(FAILED(hr)) goto exit;
		}

		BOOL closeSuccess = CloseHandle(info[ndx].Thread);
		assert(closeSuccess);
	}

	ULONG64 recovery_cycles = 0;
#ifdef ESTIMATE_RECOVERY
	page_buffer_t** page_buffers = (page_buffer_t**)malloc(sizeof(page_buffer_t*) * (1 + args->threadCount));
	for (ULONG i = 0; i < args->threadCount; i++) {
		page_buffers[i] = info[i].page_buffer;
		fprintf(stderr, "page buffer %d has %u pages\n", i, page_buffers[i]->current_size);
#ifdef DO_STATS
		fprintf(stderr, "marks %u, hits %u\n", page_buffers[i]->num_marks, page_buffers[i]->hits);
#endif
	}
	page_buffers[args->threadCount] = (page_buffer_t*)GetOpaquePageBuffer(epoch);
	fprintf(stderr, "page buffer %d has %u pages\n", args->threadCount, page_buffers[args->threadCount]->current_size);
#ifdef DO_STATS
	fprintf(stderr, "marks %u, hits %u\n", page_buffers[args->threadCount]->num_marks, page_buffers[args->threadCount]->hits);
#endif
	ULONG64 startCycles = __rdtsc();
	recover(sl, NULL, page_buffers, args->threadCount + 1);
	ULONG64 endCycles = __rdtsc();

	recovery_cycles = endCycles - startCycles;
	free(page_buffers);

	page_buffer_t* pb = create_page_buffer();

	SetOpaquePageBuffer(epoch, pb);
#endif

	// cleanup all memory to avoid memory leaks (and complaints)
	//
	ULONG removed = 0;
	LONG removedSum = 0;

	for(skey_t id = 1;id < args->maxValue;id++) {
		bool opsucc = (linkedlist_remove(sl, id, epoch, buffer) != 0);

		if(opsucc) {
			removed++;
			removedSum += id;
		}
	}

	// See if there are any elements left in the linkedlist.
	ULONG elementsInEmptyList = 0;

	for(skey_t id = 1;id < args->maxValue;id++) {
		if(linkedlist_find(sl, id, epoch, buffer) != 0) {
			elementsInEmptyList++;
		}
	}
#ifdef ESTIMATE_RECOVERY
	destroy_page_buffer((page_buffer_t*)GetOpaquePageBuffer(epoch));
#endif
	//
	// calculate all statistics and print them
	//
	OperationStats totalOpStats;
	memset((void *)&totalOpStats, 0, sizeof(OperationStats));

	for(ULONG i = 0;i < args->threadCount;i++) {
		totalOpStats.operationCount += info[i].opStats.operationCount;

		totalOpStats.lookupCountTotal += info[i].opStats.lookupCountTotal;
		totalOpStats.lookupCountSucc += info[i].opStats.lookupCountSucc;
		totalOpStats.lookupCountFail += info[i].opStats.lookupCountFail;

		totalOpStats.insertCountTotal += info[i].opStats.insertCountTotal;
		totalOpStats.insertCountSucc += info[i].opStats.insertCountSucc;
		totalOpStats.insertCountFail += info[i].opStats.insertCountFail;

		totalOpStats.removeCountTotal += info[i].opStats.removeCountTotal;
		totalOpStats.removeCountSucc += info[i].opStats.removeCountSucc;
		totalOpStats.removeCountFail += info[i].opStats.removeCountFail;

		totalOpStats.sum += info[i].opStats.sum;
	}

	static const DOUBLE MS_IN_S = 1000;

	DOUBLE opsPerSec = (DOUBLE)totalOpStats.operationCount /
		                      (args->durationMs / MS_IN_S);
	DOUBLE lookupRatio = (DOUBLE)totalOpStats.lookupCountTotal /
		                         totalOpStats.operationCount;
	DOUBLE lookupSuccess = (DOUBLE)totalOpStats.lookupCountSucc /
								   totalOpStats.lookupCountTotal;
	DOUBLE insertRatio = (DOUBLE)totalOpStats.insertCountTotal /
		                          totalOpStats.operationCount;
	DOUBLE insertSuccess = (DOUBLE)totalOpStats.insertCountSucc /
								   totalOpStats.insertCountTotal;
	DOUBLE removeRatio = (DOUBLE)totalOpStats.removeCountTotal /
		                          totalOpStats.operationCount;
	DOUBLE removeSuccess = (DOUBLE)totalOpStats.removeCountSucc /
								   totalOpStats.removeCountTotal;

	// what should be the linkedlist size based on the number of successful
	// operations reported
	ULONG projectedLinkedListSize =
		inserted +
		(ULONG)totalOpStats.insertCountSucc -
		(ULONG)totalOpStats.removeCountSucc;
	LONG projectedLinkedListSum = insertedSum + totalOpStats.sum;

	// total number of elements is the number of elements removed
	// during cleanup
	ULONG totalLinkedListSize = removed;
	LONG totalLinkedListSum = removedSum;

	delete_linkedlist(sl);

	printf("\nResults:\n");
	printf("    Lookup operation ratio : %.2f\n", lookupRatio);
	printf("        Successful lookups : %.2f\n", lookupSuccess);
	printf("    Insert operation ratio : %.2f\n", insertRatio);
	printf("        Successful inserts : %.2f\n", insertSuccess);
	printf("    Remove operation ratio : %.2f\n", removeRatio);
	printf("        Successful removes : %.2f\n", removeSuccess);
	printf("    Total linkedlist size    : %d\n", totalLinkedListSize);
	printf("    Projected linkedlist size: %d\n", projectedLinkedListSize);
	printf("    Total linkedlist sum     : %d\n", totalLinkedListSum);
	printf("    Projected linkedlist sum : %d\n", projectedLinkedListSum);

	if(totalLinkedListSize == projectedLinkedListSize) {
		printf("Projected and actual linkedlist size match.\n");
	} else {
		printf("Error: Projected and actual linkedlist size do not match.\n");
		hr = E_FAIL;
	}

	if(totalLinkedListSum == projectedLinkedListSum) {
		printf("Projected and actual linkedlist sums match.\n");
	} else {
		printf("Error: Projected and actual linkedlist sums do not match.\n");
		hr = E_FAIL;
	}

	if(elementsInEmptyList == 0) {
		printf("Linkedlist emptied correctly.\n");
	} else {
		printf("Error: Emptied linkedlist is not empty anymore.\n");
		hr = E_FAIL;
	}

	printf("    Total operations: %d\n", totalOpStats.operationCount);
	printf("    Throughput (ops/s): %.2f\n", opsPerSec);
	printf("    Recovery takes (cycles): %u\n", recovery_cycles);
	printf("\n");

exit:
	if (buffer != NULL) {
		buffer_destroy(buffer);
	}
	if(hkThreadInit)
	{
		EpochThreadShutdown(epoch);
	}

	if (joinStartInit)
	{
		JoinAllCleanup(&joinStart);
	}

	if (joinEndInit)
	{
		JoinAllCleanup(&joinEnd);
	}

	return hr;
}

//----------------------------------------------------------------------------
// Function: PmemLinkedListPerfTestPrintUsage
//
// Description:
//
//  This routine displays usage format for the LfLinkedListPerf test.
//
// Returns:
//
//  None.
//
// Notes:
//
static void PrintUsage(char *name)
{
	printf("------------------------\n");
	printf("/test name=lf-linkedlist (%s)\n", name);
	printf("[/param =<%s>] - Controls ratio of insert and update "
		"operations. Default is %d.\n",
		PmemLinkedListArgs::KEY_INSERT_REMOVE_RATIO, PmemLinkedListArgs::DEFAULT_INSERT_REMOVE_RATIO);
	printf("[/param =<%s>] - Controls max value to insert. "
		"Default is %d.\n", PmemLinkedListArgs::KEY_MAX_VALUE, PmemLinkedListArgs::DEFAULT_MAX_VALUE);
	printf("[/param =<%s>] - Controls duration of test (ms). "
		"Default is %d.\n",
		PmemLinkedListArgs::KEY_DURATION_MS, PmemLinkedListArgs::DEFAULT_DURATION_MS);
	printf("[/param =<%s>] - Controls the number of threads used. "
		"Default is %d.\n\n",
		PmemLinkedListArgs::KEY_THREAD_COUNT, PmemLinkedListArgs::DEFAULT_THREAD_COUNT);
}

static __checkReturn HRESULT
readParams(
	__in const HkStmUnitTest* test,
	__in PmemLinkedListArgs *args)
{
	HRESULT hr = S_OK;

	// Read insremrat.
	hr = UtReadUlongValue(test, PmemLinkedListArgs::KEY_INSERT_REMOVE_RATIO,
		&args->insertRemoveRatio);

	if (HK_E_NOTFOUND == hr)
	{
		args->insertRemoveRatio = PmemLinkedListArgs::DEFAULT_INSERT_REMOVE_RATIO;
		hr = S_OK;
	}
	else if (FAILED(hr))
	{
		goto exit;
	}

	if(args->insertRemoveRatio > PmemLinkedListArgs::MAX_INSERT_REMOVE_RATIO)
	{
		hr = E_INVALIDARG;
		goto exit;
	}

	// Read maxval.
	hr = UtReadUlongValue(test, PmemLinkedListArgs::KEY_MAX_VALUE, &args->maxValue);

	if (HK_E_NOTFOUND == hr)
	{
		args->maxValue = PmemLinkedListArgs::DEFAULT_MAX_VALUE;
		hr = S_OK;
	}
	else if (FAILED(hr))
	{
		goto exit;
	}

	// Read time.
	hr = UtReadUlongValue(test, PmemLinkedListArgs::KEY_DURATION_MS,
		&args->durationMs);

	if (HK_E_NOTFOUND == hr)
	{
		args->durationMs = PmemLinkedListArgs::DEFAULT_DURATION_MS;
		hr = S_OK;
	}
	else if (FAILED(hr))
	{
		goto exit;
	}

	// Read threads.
	hr = UtReadUlongValue(test, PmemLinkedListArgs::KEY_THREAD_COUNT,
		&args->threadCount);

	if (HK_E_NOTFOUND == hr)
	{
		args->threadCount = PmemLinkedListArgs::DEFAULT_THREAD_COUNT;
		hr = S_OK;
	}
	else if (FAILED(hr))
	{
		goto exit;
	}

exit:
	return hr;
}

//----------------------------------------------------------------------------
// Function: PmemLinkedListPerfTestFormatUsage
//
// Description:
//
//  This routine uses the tokenized cmd line and maps it to test specific values.
//	The routine prints the tests' interpretation of the command line.
//
// Returns:
//
//  HRESULT.
//
// Notes:
//
static __checkReturn HRESULT
FormatUsage(__in const HkStmUnitTest* test)
{
	HRESULT hr = S_OK;

	printf("/test name=%s ", test->TestName);

	PmemLinkedListArgs args;

	hr = readParams(test, &args);

	if(FAILED(hr)) goto exit;

	printf("/param %s=%d ",
		PmemLinkedListArgs::KEY_INSERT_REMOVE_RATIO, args.insertRemoveRatio);
	printf("/param %s=%d ",
		PmemLinkedListArgs::KEY_MAX_VALUE, args.maxValue);
	printf("/param %s=%d ",
		PmemLinkedListArgs::KEY_DURATION_MS, args.durationMs);
	printf("/param %s=%d ",
		PmemLinkedListArgs::KEY_THREAD_COUNT, args.threadCount);

exit:
	return hr;
}

//----------------------------------------------------------------------------
// Function: PmemLinkedListPerfTestEntryFunction
//
// Description:
//
//  This routine runs our test test.
//
// Returns:
//
//  HRESULT.
//
// Notes:
//
__checkReturn HRESULT LlEntryFunction(__in const HkStmUnitTest* test)
{
	HRESULT hr = S_OK;
	PmemLinkedListArgs args;

	buffer = buffer_create();
	EpochGlobalInit(buffer);
	

	// read parameters from command line
	hr = readParams(test, &args);

	if(FAILED(hr)) goto exit;

	hr = runTest(test, &args);

	EpochPrintStats();

exit:
	EpochGlobalShutdown();

	return hr;
}


//---------------------------------------------------------------------
// PmemLinkedList test functions.
//

void PmemLinkedListPerfTestPrintUsage() {
	PrintUsage("Pmem");
}

__checkReturn HRESULT
PmemLinkedListPerfTestFormatUsage(__in const HkStmUnitTest* test) {
	return FormatUsage(test);
}

__checkReturn HRESULT 
PmemLinkedListPerfTestEntryFunction(__in const HkStmUnitTest* test) {
	return LlEntryFunction(test);
}
