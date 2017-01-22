#ifndef _SORT_H_
#define _SORT_H_

#include "common.h"

typedef struct _stats { 
  double total_time;
  int total_threads;
// ...
} STATS;

/* testing.. */

//size_t test_partition(void *input, ITYPE *type, size_t length, void *buffer, void *pivot);
void quicksort(void *array, ITYPE *type, size_t length);
void parallelQuicksort(void *array, ITYPE *type, size_t length, size_t chunksize);


/* Sort input sequence, write stats*/
//void executeSort(SEQUENCE *input, STATS **stats);

#endif /* _SORT_H_ */
