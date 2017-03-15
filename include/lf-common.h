#ifndef _LF_COMMON_H_
#define _LF_COMMON_H_

//typedef UINT64 skey_t;
//typedef UINT64 svalue_t;

#include <stdint.h>
#include <nv_utils.h>
#include <link-cache.h>

#include "utils.h"

#define MIN_KEY 0
#define MAX_KEY INTPTR_MAX

#define NODE_PADDING 1

#define CACHE_LINE_SIZE 64
#define PMEM_CACHE_ALIGNED ALIGNED(CACHE_LINE_SIZE)

inline void flush_and_try_unflag(PVOID* target) {
	//return;
	PVOID value = *target;
	if (is_marked_ptr_cache((UINT_PTR)value)) {
		write_data_wait(target, 1);
		UNUSED PVOID dummy = CAS_PTR((volatile PVOID*)target, value, (PVOID)unmark_ptr_cache((UINT_PTR)value));
	}
}


//links a node and persists it
//marks the link while it is doing the persist
inline PVOID link_and_persist(PVOID* target, PVOID oldvalue, PVOID value) {
	//return CAS_PTR(target,oldvalue, value);
	PVOID res;
	res = CAS_PTR(target, (PVOID) oldvalue, (PVOID)mark_ptr_cache((UINT_PTR)value));
	
	//if cas successful, we updated the link, but it still needs flushing
	if (res != oldvalue) {
		return res; //nothing gets fluhed
	}
	write_data_wait(target, 1);
	UNUSED PVOID dummy = CAS_PTR((volatile PVOID*)target, (PVOID)mark_ptr_cache((UINT_PTR)value),(PVOID)value);
	return res;
}

#endif
