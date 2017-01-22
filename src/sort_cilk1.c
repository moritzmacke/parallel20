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
  char *baseArray;
  size_t startIndex;
  size_t count_le;
//  size_t count_lt;
//  size_t count_gt;
  size_t prefix_le;
//  size_t prefix_lt;
//  size_t prefix_gt;
  size_t count_total;
  size_t item_size;
} CHUNK;

//
void parallelPartitionPass1(char *array, ITYPE *type, int start, int size, CHUNK chunks[], int range, void *pivot) {
  
  if(range > 1) {
    cilk_spawn parallelPartitionPass1(array, type, start, size/2, chunks, range/2, pivot);
    cilk_spawn parallelPartitionPass1(array, type, start + size/2, size-size/2, &chunks[range/2], range-range/2, pivot);
    
    cilk_sync;

    chunks[range].prefix_le += chunks[range/2].prefix_le;
    
  }
  else {

    size_t step = type->size;
    size_t count_le = 0;
    char *src = &array[start*step];
    char *end = &src[size*step];
    while(src < end) {
      count_le += (type->compare(src,pivot) <= 0);
      src += step;
    }
    
    CHUNK *c = &chunks[0];

    //init chunk..
    c->baseArray = array;
    c->startIndex = start;
    c->count_total = size;
    c->count_le = count_le;
//    c->prefix_le = count_le;
    chunks[1].prefix_le = count_le;

  }
}

//
void parallelPartitionPass2(char *out, ITYPE *type, size_t total_le, CHUNK *chunks, int range, size_t left_sum_le, void *pivot) {

  if(range > 1) {
    size_t mid_sum_le = chunks[range/2].prefix_le + left_sum_le;

    cilk_spawn parallelPartitionPass2(out, type, total_le, chunks, range/2, left_sum_le, pivot);
    cilk_spawn parallelPartitionPass2(out, type, total_le, &chunks[range/2], range-(range/2), mid_sum_le, pivot);
  }
  else {
    //do partition
    CHUNK *c = &chunks[0];

    size_t step = type->size;
  
    char *src = &c->baseArray[c->startIndex*step];
    char *end = &src[c->count_total*step];
    size_t off_le = left_sum_le; //le before this chunk...
    size_t num_gt = c->startIndex - off_le;
    char *t_le = &out[off_le*step];
    char *t_gt = &out[(total_le + num_gt)*step];

    while(src < end) {
      if(type->compare(src,pivot) <= 0) {
        copy(src, t_le, step);
        t_le += step;
      }
      else {
        copy(src, t_gt, step);
        t_gt += step;
      }
      src += step;
    }
  }
}

size_t parallelPartition(void *in, void *out, ITYPE *type, size_t length, size_t num_chunks, int *pivot) {

//  printf("before partition\n");
//  print_vals0(in, type, length);

  CHUNK *chunks = malloc((num_chunks+1)*sizeof(CHUNK));

  //init first pf
  chunks[0].prefix_le = 0;

  parallelPartitionPass1((char *) in, type, 0, length, chunks, num_chunks, pivot);

  chunks[num_chunks].startIndex = length;
  chunks[num_chunks].count_total = 0;

  size_t total_le = chunks[num_chunks].prefix_le;
//  printf("total_le: %lu\n", total_le);
  
	if(total_le < length && total_le > 0) {	//all le or all gt?
    parallelPartitionPass2((char *) out, type, total_le, chunks, num_chunks, 0, pivot);
  }

  free(chunks);

//  print_chunks(chunks, num_chunks);

//  printf("after partition\n");
//  print_vals0(out, type, length);

  return total_le;
}

void qs(char *a, char *b, ITYPE *type, size_t length, size_t chunk_size, int32_t rstate, int depth) {

//    printf("qs start\n");

  size_t num_chunks = (length + chunk_size - 1)/chunk_size;
  
//  printf("size: %lu, %lu chunks\n", size, num_chunks);


  if(num_chunks > 1) {

    void *pivot = selectPivotRandom(a, type, length, 7, &rstate);//selectPivot(a, type, length);
    swap(&a[0],(char *) pivot, type->size); //swap pivot to beginning
    pivot = (void *) &a[0];

//    printf("\nPivot: %d\n", a[0]);

    size_t size_left = parallelPartition(&a[type->size], &b[type->size], type, length-1, num_chunks, pivot);
    size_t size_right = length - size_left - 1;

//    printf("Split at %lu\n", size_left);


    //move pivot in place...
    copy(&b[size_left*type->size], &b[0], type->size);
    copy(&a[0], &b[size_left*type->size], type->size);
    copy(&a[0], &a[size_left*type->size], type->size);


    cilk_spawn qs(b, a, type, size_left, chunk_size, rstate, depth+1);
    cilk_spawn qs(&b[(size_left+1)*type->size], &a[(size_left+1)*type->size], type, size_right, chunk_size, rstate, depth+1);


  }
  else if(num_chunks != 0) {
//    printf("QS sequential\n");
    
    if(depth%2 == 1) {
      //move to original array...

      size_t step = type->size;

      char *src = a;
      char *end = &a[length*step];
      char *tl = b;
      char *tr = &b[(length-1)*step];
      void *pivot = selectPivotRandom(a, type, length, 3, &rstate);
      while(src < end) {
        if(type->compare(src, pivot) <= 0) {
          copy(src, tl, step);
          tl += step;
        }
        else {
          copy(src, tr, step);
          tr -= step;
        }
        src += step;
      }

      size_t size_left = (tl - b)/step;

      qsort(b, size_left , type->size, type->compare);
      qsort(tl, length-size_left, type->size, type->compare);

    }
    else {
      qsort(a, length, type->size, type->compare);
    }
  }

}

void parallelQuicksort(void *array, ITYPE *type, size_t length, size_t chunk_size) {
    
    char *swapArray = malloc(length*type->size);

    qs((char *) array, swapArray, type, length, chunk_size, 612345789, 0);    

    free(swapArray);

}
