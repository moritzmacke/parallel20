#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <omp.h>
#include <math.h>

#include "sort.h"

typedef struct _chunk {
//  int thread_id;
//  int thread_id2;
  size_t startIndex;
  size_t count_total;
  size_t count_le;
//  size_t prefix_le;
//  size_t count_gt;
//  size_t count_eq;
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

//    printf("Qs_task (%lu), thread %d\n", length, omp_get_thread_num());

    char *end = &start[(length-1)*type->size];
    char *left = start;
    char *right = end;

    char *pivot = selectPivotTask(&left, &right, type->size, type->compare);

//    printf("Qs_task (%lu) Pivot: ", length);
//    print_val(pivot, type);
//    printf("\n");
//    print_vals(start, type, length);

    partition_task(&left, &right, pivot, type->size, type->compare);

//    printf("Qs_task partition\n");
//    print_vals(start, type,length);

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

//  printf("Pivot: %d\n", *((int *) pivot));

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
//    printf("%d threads in partition, length: %lu\n", numThreads, length);
    counts = (size_t *) malloc(numThreads*sizeof(size_t));
//    chunks = (CHUNK *) malloc((numThreads+1)*sizeof(CHUNK));
//    chunks[numThreads].startIndex = length;
//    chunks[numThreads].count_total = 0;
  }

  if(length > numThreads) { //shouldn't call it anyway otherwise, but so it won't fail...
  
    int tid = omp_get_thread_num();


//    printf("[%f] Thread %d start.\n", omp_get_wtime(), tid);

    size_t itemsPerChunk = (length)/numThreads;  //last chunk larger...
    size_t startIndex = tid*itemsPerChunk;
    size_t chunklen = (tid == numThreads-1) ? length - tid*itemsPerChunk : itemsPerChunk;

//    printf("Thread %d has [%lu:%lu] \n", tid, c->startIndex, c->startIndex + chunklen);

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

//    printf("[%f] Thread %d pass1 done.\n", omp_get_wtime(), tid);
  
#pragma omp barrier

//    printf("[%f] Thread %d pass2 start.\n", omp_get_wtime(), tid);

    size_t * prefixSums = prefix(counts, numThreads+1); //parallel prefix doesn't really pay off with few processors...

    size_t total_le = prefixSums[numThreads];

    size_t le_offset = prefixSums[tid];
    size_t gt_offset = total_le + (startIndex - prefixSums[tid]);


  
//    printf("LE: [%lu:%lu], GT: [%lu:%lu]\n", le_offset, le_offset+c->count_le, gt_offset, gt_offset + (c->count_total - c->count_le));


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
    
//    printf("[%f] Thread %d done.\n", omp_get_wtime(), tid);
  }
  else  {
    #pragma omp single
    { 
      char *start = ((char *) input);

      char *left = start;
      char *right = start + (length-1)*step;

//      printf("Start single partition, pivot %d\n", *((int *)pivot));
//      print_ints0((int *) input, length);

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

//        printf("End single partition, split: %lu\n", split);
//        print_ints0((int *) input, length);
    }
  }
} //parallel
  
  free(counts);

  return split;
}


int *selectPivot(void *array, ITYPE *type, size_t length) {
  void *first = array;
  void *mid = (void *) (((char *)array) + (length/2)*type->size);
  void *last = (void *) (((char *)array) + (length-1)*type->size);
  void *pivot = first;

//  printf("Pivot candidates: %d, %d, %d\n", *((int *) first) , *((int *) mid), *((int *) last));

  if(type->compare(first, last) > 0) {
    //first > last
    if(type->compare(first, mid) > 0) {
      //first > last, mid
      if(type->compare(mid, last) > 0) {
        //first > mid > last
        pivot = mid;
      }
      else {
        //first > last >= mid
        pivot = last;
      }
    }
    else {
      //mid >= first > last
    }
  }
  else {
    //last >= first
    if(type->compare(first, mid) > 0) {
      //last >= first > mid
    }
    else {
      //last, mid >= first
      if(type->compare(last, mid) > 0) {
        //last > mid >= first
        pivot = mid;
      }
      else {
        //mid >= last >= first
        pivot = last;
      }
    }
  }

//  printf("Chose: %d\n", *((int *) pivot));

  return pivot;
}

static void qs(void *array, ITYPE *type, size_t length, int numThreads, int32_t rstate) {


  if(length < 2) {
    return;
  }

  size_t left_size;
  int threads;

#pragma omp parallel num_threads(numThreads)
{
  #pragma omp single
  {
    threads = omp_get_num_threads();
    printf("Threads in QS: %d requested: %d\n", threads, numThreads);
  }
}

  if(threads < 2) {
//    printf("T:%f Thread %d start seq sort (%lu).\n", omp_get_wtime(), omp_get_thread_num(), length);
//    qsort(array, length, type->size, type->compare);
  #pragma omp task 
    qs_task((char *) array, type, length);
//    printf("t:%f Thread %d seq sort done (%lu).\n", omp_get_wtime(), omp_get_thread_num(), length);
    return;
  }


//  void *pivot = selectPivot(array, type, length);
  void *pivot = selectPivotRandom(array, type, length, 9, &rstate);


  swap((char *) array, (char *) pivot, type->size);
  pivot = array;

//  printf("Before   : ");
//  print_ints0((int *) array, length);
  left_size = parallelPartition(((char *) array) + type->size, type, length-1, pivot, threads);
//  printf("Split %3lu: ", left_size);
//  print_ints0((int *) array, length);

  swap((char *) pivot, ((char *) array) + left_size*type->size , type->size);


  double split = left_size/(double)length;
  int leftp = (int) (split*numThreads);
  if (leftp == 0) leftp++;

  printf("Split: %.2f (%d/%d)\n", split, leftp, numThreads-leftp);


#pragma omp parallel sections
{
  #pragma omp section
  {
    qs(array, type, left_size, leftp, rstate);
  }

  #pragma omp section
  {
    qs(((char *) array) + (left_size+1)*type->size, type, length - left_size -1, numThreads-leftp, rstate);
  }
}


 
  
  
}


void quicksort(void *array, ITYPE *type, size_t length) {

    int numThreads;

    #pragma omp parallel
{
    #pragma omp single
        numThreads = omp_get_num_threads();
}

//   printf("%d threads at start\n", numThreads);

   qs(array, type, length, numThreads, 612345789);
#pragma omp taskwait
  
}
