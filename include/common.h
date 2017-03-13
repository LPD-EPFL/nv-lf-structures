#ifndef _COMMON_H_
#define _COMMON_H_
#include <limits.h>
#include <string.h>

#include <nv_utils.h>

#include "latency.h"
#include "barrier.h"
#include "main_test_loop.h"

#define XSTR(s)                         STR(s)
#define STR(s)                          #s

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 6
#  define STATIC_ASSERT(a, msg)           _Static_assert ((a), msg);
#else 
#  define STATIC_ASSERT(a, msg)           
#endif

#define STRING_LENGTH						8

typedef intptr_t skey_t;
typedef intptr_t svalue_t;

typedef struct strkey_t {
	char key[STRING_LENGTH]; 
} strkey_t;

typedef struct strval_t {
	char val[STRING_LENGTH];
} strval_t;

// static inline char* STR_KEY_MAX() {
// 	char str[] = {CHAR_MIN, '\0'};
// 	return str;
// }

// inline const char* KEY_MIN() {
// 	std::string str = "";
// 	str.push_back(CHAR_MIN);
// 	return str.data();
// }

#define STR_KEY_MIN						""
#define STR_KEY_MAX						"zzz"

static inline int strkey_compare(strkey_t k1, strkey_t k2) {

	//TODO write our own strcmp
	if ( strcmp(k1.key, k2.key) == 0) return 0;

	if ( strcmp(k1.key, STR_KEY_MIN) == 0) return -1;
	if ( strcmp(k2.key, STR_KEY_MIN) == 0 ) return +1;

	if ( strcmp(k1.key, STR_KEY_MAX) == 0 ) return +1;
	if ( strcmp(k2.key, STR_KEY_MAX) == 0 ) return -1;

	return strcmp(k1.key, k2.key);
}

#define KEY_MIN                         INT_MIN
#define KEY_MAX                         (INT_MAX - 2)



#define DEFAULT_DURATION                1000
#define DEFAULT_INITIAL                 1024
#define DEFAULT_NB_THREADS              1
#define DEFAULT_RANGE                   (2 * DEFAULT_INITIAL)
#define DEFAULT_UPDATE                  20

#endif	/*  _COMMON_H_ */
