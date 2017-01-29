#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef SEQUENTIAL
#define cilk_spawn
#define cilk_sync
#else
// cilkplus libraries
#include <cilk/cilk.h>
#include <cilk/cilk_api.h>
#endif


#include "sort.h"

typedef struct _chunk {
  size_t startIndex;
  size_t count_le;
  size_t prefix_le;
  size_t count_total;
} CHUNK;


//
void parallelPartitionPass1(char *array, char *swaps, ITYPE *type, int start, int size, CHUNK chunks[], int range, void *pivot) {
  
  if(range > 1) {
    cilk_spawn parallelPartitionPass1(array, swaps, type, start, size/2, chunks, range/2, pivot);
    cilk_spawn parallelPartitionPass1(array, swaps, type, start + size/2, size-size/2, &chunks[range/2], range-range/2, pivot);
    
    cilk_sync;

    chunks[range].prefix_le += chunks[range/2].prefix_le;
  }
  else {
    size_t count_le = 0;
    size_t step = type->size;

    char *src = &array[start*step];
    char *end = &src[size*step];
    char *t_le = &swaps[start*step];
    char *t_gt = &t_le[(size-1)*step];

    //partition to swap array
    while(src < end) {
      if(type->compare(src, pivot) <= 0) {
        copy(src, t_le, step);
        t_le += step;
        count_le++;
      }
      else {
        copy(src, t_gt, step);
        t_gt -= step;
      }
      src += step;
    }
    
    CHUNK *c = &chunks[0];

    //init chunk..
    c->startIndex = start;
    c->count_total = size;
    c->count_le = count_le;
    chunks[1].prefix_le = count_le;

  }
}

//
void parallelPartitionPass2(char *array, char *swaps, ITYPE *type, size_t total_le, CHUNK *chunks, int range, size_t left_sum_le) {

  if(range > 1) {
    size_t mid_sum_le = chunks[range/2].prefix_le + left_sum_le;

    cilk_spawn parallelPartitionPass2(array, swaps, type, total_le, chunks, range/2, left_sum_le);
    cilk_spawn parallelPartitionPass2(array, swaps, type, total_le, &chunks[range/2], range-(range/2), mid_sum_le);
  }
  else {
    //do partition
    CHUNK *c = &chunks[0];
  
    size_t num_le = c->count_le;
    size_t num_gt = c->count_total-num_le;
    size_t off_le = left_sum_le; //le before this chunk...
    size_t off_gt = c->startIndex - off_le;

    size_t step = type->size;

    char *s_le = &swaps[c->startIndex*step];
    char *s_gt = &s_le[num_le*step];
    char *t_le = &array[off_le*step];
    char *t_gt = &array[(total_le + off_gt)*step];

    copy(s_le, t_le, num_le*step);
    copy(s_gt, t_gt, num_gt*step);
  }
}

size_t parallelPartition(void *array, void *swaps, ITYPE *type, size_t length, size_t num_chunks, void *pivot) {

  CHUNK *chunks = malloc((num_chunks+1)*sizeof(CHUNK));

  //init first pf
  chunks[0].prefix_le = 0;

  parallelPartitionPass1((char *) array, (char *) swaps, type, 0, length, chunks, num_chunks, pivot);

  chunks[num_chunks].startIndex = length;
  chunks[num_chunks].count_total = 0;

  size_t total_le = chunks[num_chunks].prefix_le;   //complete for last block

  if(total_le < length && total_le > 0) {	//all le or all gt?
    parallelPartitionPass2((char *) array, (char *) swaps, type, total_le, chunks, num_chunks, 0);
  }

  free(chunks);

  return total_le;

}

void qs(char *array, char *swaps, ITYPE *type, size_t length, size_t chunk_size, int32_t rstate) {

  size_t num_chunks = (length + chunk_size - 1)/chunk_size;

  if(num_chunks > 1) {

    void *pivot = selectPivotRandom(array, type, length, 7, &rstate);
    swap(&array[0],(char *) pivot, type->size); //swap pivot to beginning
    pivot = (void *) &array[0];

    size_t size_left = parallelPartition(&array[type->size], &swaps[type->size], type, length-1, num_chunks, pivot);
    size_t size_right = length - size_left - 1;

//    printf("Split at %lu\n", size_left);

    swap((char *) pivot,(char *) &array[size_left*type->size], type->size); //swap pivot in place

    cilk_spawn qs(array, swaps, type, size_left, chunk_size, rstate);
    cilk_spawn qs(&array[(size_left+1)*type->size], &swaps[(size_left+1)*type->size], type, size_right, chunk_size, rstate);
  }
  else if(num_chunks != 0) {
    qsort(array, length, type->size, type->compare);
  }

}

void parallelQuicksort(void *array, ITYPE *type, size_t length, size_t chunk_size) {

  size_t num_chunks = (length + chunk_size - 1)/chunk_size;
  
  if(num_chunks > 1) {
    char *swaps = (char *) malloc(length*type->size);

    qs(array, swaps, type, length, chunk_size, 612345789);

    free(swaps);
  }
  else if(num_chunks != 0) {
    qsort(array, length, type->size, type->compare);
  }
}
