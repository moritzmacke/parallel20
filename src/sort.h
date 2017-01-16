#ifndef _SORT_H_
#define _SORT_H_

#include "common.h"

typedef int (*FP_COMPARE) (const void *, const void *);
typedef void (*FP_PRINT) (void *item, char *target, size_t maxChars);  

typedef struct _itype {
  size_t size;
  FP_COMPARE compare;
  FP_PRINT print;
} ITYPE;

typedef struct _stats { 
  double total_time;
  int total_threads;
// ...
} STATS;

/* testing.. */
size_t test_partition(void *input, ITYPE *type, size_t length, void *buffer, void *pivot);
void quicksort(void *array, ITYPE *type, size_t length, void *buffer);
void parallelQuicksort(void *array, ITYPE *type, size_t length, size_t chunksize);



/* for testing... */
int set_num_threads(int threads);

/* Sort input sequence, write stats*/
//void executeSort(SEQUENCE *input, STATS **stats);

#endif /* _SORT_H_ */
