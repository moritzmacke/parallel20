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
  size_t item_size;
} CHUNK;

void swap(char *a, char *b, size_t size) {

  while(size > 0) {
    char tmp = *a;
    *a++ = *b;
    *b++ = tmp;
    size--;
  }
}

void copy(char *a, char *b, size_t size) {
  char *e = &a[size];
  while(a < e) {
    *b++ = *a++;
  }
}

void print_vals0(char *array, ITYPE *type, size_t length) {
  char buf[32];

  if(length <= 100 && length > 0) {
    int i;
    for(i=0; (i+1)<length; i++) {
      type->print(&array[i*type->size], buf, sizeof(buf));
      printf("%s, ", buf);
    }
    type->print(&array[i*type->size], buf, sizeof(buf));
    printf("%s\n", buf);
  }
}



//
void parallelPartitionPass1(char *array, char *swaps, ITYPE *type, int start, int size, CHUNK chunks[], int range, void *pivot) {
  
  if(range > 1) {
    cilk_spawn parallelPartitionPass1(array, swaps, type, start, size/2, chunks, range/2, pivot);
    cilk_spawn parallelPartitionPass1(array, swaps, type, start + size/2, size-size/2, &chunks[range/2], range-range/2, pivot);
    
    cilk_sync;

    chunks[range-1].prefix_le += chunks[range/2 - 1].prefix_le;


    
  }
  else {

    size_t count_le = 0;
    size_t step = type->size;

    char *src = &array[start*step];
    char *end = &src[size*step];
    char *t_le = &swaps[start*step];
    char *t_gt = &t_le[(size-1)*step];
    while(src < end) {
      if(type->compare(src, pivot) <= 0) {
//        printf("Move le: %d to %lu\n", *src, t_le);
        copy(src, t_le, step);
        t_le += step;

        count_le++;
      }
      else {
//        printf("Move gt: %d to %lu\n", *src, t_gt);
        copy(src, t_gt, step);
        t_gt -= step;
      }
      src += step;
    }
    
    CHUNK *c = &chunks[0];

    //init chunk..
//    c->baseArray = items;
    c->startIndex = start;
    c->count_total = size;
    c->count_le = count_le;
    c->prefix_le = count_le;


  }
}

//
void parallelPartitionPass2(char *array, char *swaps, ITYPE *type, size_t total_le, CHUNK *chunks, int range, size_t left_sum_le) {

  if(range > 1) {
    chunks[range/2 - 1].prefix_le += left_sum_le;

    cilk_spawn parallelPartitionPass2(array, swaps, type, total_le, chunks, range/2, left_sum_le);
    cilk_spawn parallelPartitionPass2(array, swaps, type, total_le, &chunks[range/2], range-(range/2), chunks[range/2 - 1].prefix_le);
  }
  else {
    //do partition
    CHUNK *c = &chunks[0];
  
    size_t num_le = c->count_le;
    size_t num_gt = c->count_total-num_le;
    size_t off_le = c->prefix_le - num_le; //le before this chunk...
    size_t off_gt = c->startIndex - off_le;

    size_t step = type->size;

    char *s_le = &swaps[c->startIndex*step];
    char *s_gt = &s_le[num_le*step];


    char *t_le = &array[off_le*step];
    char *t_gt = &array[(total_le + off_gt)*step];

//    printf("move %lu le from %lu to %lu\n", num_le, c->startIndex, off_le);
//    memcpy((void *) t_le, (void *) s_le, num_le*step);
    copy(s_le, t_le, num_le*step);
//    printf("move %lu gt from %lu to %lu\n", num_gt, c->startIndex + num_le, off_gt);
//    memcpy((void *) t_gt, (void *) s_gt, num_gt*step);
    copy(s_gt, t_gt, num_gt*step);

  }
}

void parallelPartition(void *array, void *swaps, ITYPE *type, size_t size, CHUNK chunks[], size_t num_chunks, void *pivot) {

//  printf("Before Partition: \n");
//  print_vals0(array, type, size);

  parallelPartitionPass1((char *) array, (char *) swaps, type, 0, size, chunks, num_chunks, pivot);

  size_t total_le = chunks[num_chunks-1].prefix_le;  

  parallelPartitionPass2((char *) array, (char *) swaps, type, total_le, chunks, num_chunks, 0);

//  printf("After Partition: \n");
//  print_vals0(array, type, size);

//  print_chunks(chunks, num_chunks);

}

void *selectPivot(void *array, ITYPE *type, size_t length) {
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

void qs(char *array, char *swaps, ITYPE *type, size_t length, size_t chunk_size) {

  size_t num_chunks = (length + chunk_size - 1)/chunk_size;
  
//  printf("length: %lu, %lu chunks\n", length, num_chunks);

//  sleep(1);

  if(num_chunks > 1) {

//    printf("\n");
//    print_ints(items, length);

    void *pivot = selectPivot(array, type, length);
    swap(&array[0],(char *) pivot, type->size); //swap pivot to beginning
    pivot = (void *) &array[0];

    //printf("Pivot: %d\n", *pivot);

    CHUNK *chunks = (CHUNK *) malloc(num_chunks*sizeof(CHUNK));

    parallelPartition(&array[type->size], &swaps[type->size], type, length-1, chunks, num_chunks, pivot);

    size_t size_left = chunks[num_chunks-1].prefix_le;
    size_t size_right = length - size_left - 1;

//    printf("Split at %lu\n", size_left);

    free(chunks);

    swap((char *) pivot,(char *) &array[size_left*type->size], type->size); //swap pivot in place

//    print_ints(items, length);


    if(size_left > chunk_size) {
      cilk_spawn qs(array, swaps, type, size_left, chunk_size);
    }
    else {
      qs(array, swaps, type, size_left, chunk_size);
    }

    if(size_right > chunk_size) {
      cilk_spawn qs(&array[(size_left+1)*type->size], &swaps[(size_left+1)*type->size], type, size_right, chunk_size);
    }
    else {
      qs(&array[(size_left+1)*type->size], &swaps[(size_left+1)*type->size], type, size_right, chunk_size);
    }

  }
  else if(num_chunks != 0) {
    qsort(array, length, type->size, type->compare);
  }

}

void parallelQuicksort(void *array, ITYPE *type, size_t length, size_t chunk_size) {

  size_t num_chunks = (length + chunk_size - 1)/chunk_size;
  
//  printf("length: %lu, %lu chunks\n", length, num_chunks);

//  sleep(1);

  if(num_chunks > 1) {

    char *swaps = (char *) malloc(length*type->size);

    qs(array, swaps, type, length, chunk_size);

    free(swaps);


  }
  else if(num_chunks != 0) {
    qsort(array, length, type->size, type->compare);
  }

}
