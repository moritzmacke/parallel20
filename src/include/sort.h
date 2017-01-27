#ifndef _SORT_H_
#define _SORT_H_

#include "common.h"


void quicksort_omp(void *array, ITYPE *type, size_t length, uint32_t samples);
void parallelQuicksort(void *array, ITYPE *type, size_t length, size_t chunksize);


#endif /* _SORT_H_ */
