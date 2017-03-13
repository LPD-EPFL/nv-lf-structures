#ifndef _LF_COMMON_H_
#define _LF_COMMON_H_

typedef UINT64 skey_t;
typedef UINT64 svalue_t;

#define MIN_KEY 0
#define MAX_KEY _UI64_MAX

#define NODE_PADDING 1

#define CACHE_LINE_SIZE 64
#define PMEM_CACHE_ALIGNED __declspec(align(CACHE_LINE_SIZE)) 


inline void flush_and_try_unflag(PVOID* target) {
	//return;
	PVOID value = *target;
	if (is_marked_ptr_buffer((UINT_PTR)value)) {
		write_data_wait(target, 1);
		InterlockedCompareExchangePointer((volatile PVOID*)target, (PVOID)unmark_ptr_buffer((UINT_PTR)value), value);
	}
}


//links a node and persists it
//marks the link while it is doing the persist
inline PVOID link_and_persist(PVOID* target, PVOID oldvalue, PVOID value) {
	//return InterlockedCompareExchangePointer(target, (PVOID)(UINT_PTR)value, (PVOID)oldvalue);
	PVOID res;
	res = InterlockedCompareExchangePointer(target, (PVOID)mark_ptr_buffer((UINT_PTR)value), (PVOID)oldvalue);
	
	//if cas successful, we updated the link, but it still needs flushing
	if (res != oldvalue) {
		return res; //nothing gets fluhed
	}
	write_data_wait(target, 1);
	InterlockedCompareExchangePointer((volatile PVOID*)target, (PVOID)value, (PVOID)mark_ptr_buffer((UINT_PTR)value));
	return res;
}

#endif
