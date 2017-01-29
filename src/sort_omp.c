#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <omp.h>
#include <math.h>

#include "sort.h"

typedef struct _chunk {
  size_t startIndex;
  size_t count_total;
  size_t count_le;
} CHUNK;

/* */

/* Moves middle element to midposition and makes it pivot */
char *selectPivotTask(char **p_left, char **p_right, size_t size, FP_COMPARE compare) {

  //assert(length >= 3);

  char *mid = *p_left + ((*p_right - *p_left) / (2*size)) * size;

  //mid smaller than first?
  if(compare((void *) mid, (void *) *p_left) < 0) {
    swap(mid, *p_left, size);
  }
  //mid now bigger than last?
  if(compare((void *) mid, (void *) *p_right) > 0) {
    swap(mid, *p_right, size);
    //was end smaller than first?
    if(compare((void *) mid, (void *) *p_left) < 0) {
      swap(mid, *p_left, size);
    }
  }

  *p_left += size;
  *p_right -= size;

  return mid;
}


void partition_task(char **p_left, char **p_right, char *pivot, size_t size, FP_COMPARE compare) {

  char *left = *p_left;
  char *right = *p_right;

  do {
    while(compare((void *) left, (void *) pivot) < 0) {
      left += size;
    }
    while(compare((void *) pivot, (void *) right) < 0) {
      right -= size;
    }

    if(left < right) {
      swap(left, right, size);
      //did we swap the pivot?
      if(left == pivot) {
        pivot = right; 
      }
      else if(right == pivot) {
        pivot = left;
      }
      left += size;
      right -= size;
    }
    else if(left == right) {
      //met on same value == pivot
      left += size;
      right -= size;
      break;
    }
  } while(left <= right);

  *p_left = left;
  *p_right = right;
}


void qs_task(char *start, ITYPE *type, size_t length) {
  if(length > 1000/*CUTOFF*/) {

    char *end = &start[(length-1)*type->size];
    char *left = start;
    char *right = end;

    char *pivot = selectPivotTask(&left, &right, type->size, type->compare);

    partition_task(&left, &right, pivot, type->size, type->compare);

    size_t size_low = (right - start)/type->size + 1;
    size_t size_high = (end - left)/type->size + 1;

#pragma omp task untied
    qs_task(start, type, size_low);
#pragma omp task untied
    qs_task(left, type, size_high);
  }
  else {
    qsort(start, length, type->size, type->compare);
  }
}





/* */

size_t *prefix(size_t *counts, uint32_t length) {

    size_t *prefixSums = (size_t *) malloc(length*sizeof(size_t));

    size_t sum = 0;
    uint32_t i;

    for(i=0; i<length-1; i++) {
        prefixSums[i] = sum;
        sum += counts[i];
    }
    prefixSums[i] = sum;

    return prefixSums;
}

size_t parallelPartition(void *input, ITYPE *type, size_t length, void *pivot, int threads) {

  int numThreads;
  size_t *counts;
  size_t split = 0;
  
#pragma omp parallel num_threads(threads) default(none), shared(numThreads, split, counts) firstprivate(type, length, \
        input, pivot)
{
   size_t step = type->size;
  
#pragma omp single
  {
    numThreads = omp_get_num_threads();
    counts = (size_t *) malloc(numThreads*sizeof(size_t));
  }

  if(length > numThreads) { //shouldn't call it anyway otherwise, but so it won't fail...
  
    int tid = omp_get_thread_num();

    size_t itemsPerChunk = (length)/numThreads;  //last chunk larger...
    size_t startIndex = tid*itemsPerChunk;
    size_t chunklen = (tid == numThreads-1) ? length - tid*itemsPerChunk : itemsPerChunk;

    char *buffer = (char *) malloc(chunklen*step);
    char *src = ((char *) input) + startIndex*step;
    char *end = src + chunklen*step;
    char *tgt_le = buffer;
    char *tgt_gt = tgt_le + (chunklen)*step;
    
    size_t countLE = 0;
     
    for(; src < end; src += step) {
      if(type->compare(src, pivot) > 0) {
        tgt_gt -= step;
        copy(src, tgt_gt, step);
      }
      else {
        copy(src, tgt_le, step);
        tgt_le += step;
        countLE++;
      }      
    }

    size_t countGT = chunklen - countLE;
    counts[tid] = countLE;
  
#pragma omp barrier

    size_t *prefixSums = prefix(counts, numThreads+1); //parallel prefix doesn't really pay off with few processors...
    size_t total_le = prefixSums[numThreads];
    size_t le_offset = prefixSums[tid];
    size_t gt_offset = total_le + (startIndex - prefixSums[tid]);

    char *src_le = buffer;
    char *src_gt = tgt_gt;

    tgt_le = ((char *) input) + le_offset*step;
    tgt_gt = ((char *) input) + gt_offset*step;

    memcpy((void *) tgt_le, (void *) src_le, countLE*step);
    memcpy((void *) tgt_gt, (void *) src_gt, countGT*step);

    free(buffer);
    free(prefixSums);

    if(tid == numThreads - 1) {
      split = le_offset + countLE;
    }
  }
  else  {
    #pragma omp single
    { 
      char *start = ((char *) input);
      char *left = start;
      char *right = start + (length-1)*step;

      for(;;) {
		while(left <= right && type->compare(left, pivot) <= 0) {
			left += step;
		}
		while(right >= left && type->compare(right, pivot) > 0) {
			right -= step;
		}
		if(left > right) {
			break;
		}
		else {
			swap(left, right, step);
            left += step;
            right -= step;
		}
	  }
	  split = (left - start)/step;
    }
  }
} //parallel
  
  free(counts);

  return split;
}

static void qs(void *array, ITYPE *type, size_t length, int numThreads, uint32_t numPivots, int32_t rstate) {

  if(length < 2) {
    return;
  }

  size_t left_size;
  int threads;

#pragma omp parallel num_threads(numThreads) default(none) shared(threads)
{
  #pragma omp single
  {
    threads = omp_get_num_threads();
//    printf("Threads in QS: %d requested: %d\n", threads, numThreads);
  }
}

  if(threads < 2) {
  #pragma omp task 
    qs_task((char *) array, type, length);

    return;
  }

  void *pivot = selectPivotRandom(array, type, length, numPivots, &rstate);

  swap((char *) array, (char *) pivot, type->size);
  pivot = array;

  left_size = parallelPartition(((char *) array) + type->size, type, length-1, pivot, threads);

  swap((char *) pivot, ((char *) array) + left_size*type->size , type->size);

  double split = left_size/(double)length;
  int leftp = (int) (split*numThreads);
  if (leftp == 0) leftp++;

#pragma omp parallel sections
{
  #pragma omp section
  {
    qs(array, type, left_size, leftp, numPivots, rstate);
  }

  #pragma omp section
  {
    qs(((char *) array) + (left_size+1)*type->size, type, length - left_size - 1, numThreads-leftp, numPivots, rstate);
  }
}  
  
}


void quicksort_omp(void *array, ITYPE *type, size_t length, uint32_t numPivots) {

    int numThreads;

    #pragma omp parallel
{
    #pragma omp single
        numThreads = omp_get_num_threads();
}

//   printf("%d threads at start\n", numThreads);

   qs(array, type, length, numThreads, numPivots, 612345789);
#pragma omp taskwait
  
}
