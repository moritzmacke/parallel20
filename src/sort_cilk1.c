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
void parallelPartitionPass1(char *array, ITYPE *type, int start, int size, CHUNK chunks[], int range, void *pivot) {
  
  if(range > 1) {
    cilk_spawn parallelPartitionPass1(array, type, start, size/2, chunks, range/2, pivot);
    cilk_spawn parallelPartitionPass1(array, type, start + size/2, size-size/2, &chunks[range/2], range-range/2, pivot);
    
    cilk_sync;

    chunks[range-1].prefix_le += chunks[range/2 - 1].prefix_le;
    
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
    c->prefix_le = count_le;


  }
}

//
void parallelPartitionPass2(char *out, ITYPE *type, size_t total_le, CHUNK *chunks, int range, size_t left_sum_le, void *pivot) {

  if(range > 1) {
    chunks[range/2 - 1].prefix_le += left_sum_le;

    cilk_spawn parallelPartitionPass2(out, type, total_le, chunks, range/2, left_sum_le, pivot);
    cilk_spawn parallelPartitionPass2(out, type, total_le, &chunks[range/2], range-(range/2), chunks[range/2 - 1].prefix_le, pivot);
  }
  else {
    //do partition
    CHUNK *c = &chunks[0];

    size_t step = type->size;
  
    char *src = &c->baseArray[c->startIndex*step];
    char *end = &src[c->count_total*step];
    size_t off_le = c->prefix_le - c->count_le; //le before this chunk...
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

void parallelPartition(void *in, void *out, ITYPE *type, size_t length, CHUNK chunks[], size_t num_chunks, int *pivot) {

//  printf("before partition\n");
//  print_vals0(in, type, length);

  parallelPartitionPass1((char *) in, type, 0, length, chunks, num_chunks, pivot);

  size_t total_le = chunks[num_chunks-1].prefix_le;
//  printf("total_le: %lu\n", total_le);
  

  parallelPartitionPass2((char *) out, type, total_le, chunks, num_chunks, 0, pivot);

//  print_chunks(chunks, num_chunks);

//  printf("after partition\n");
//  print_vals0(out, type, length);

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

void qs(char *a, char *b, ITYPE *type, size_t length, size_t chunk_size, int depth) {

//    printf("qs start\n");

  size_t num_chunks = (length + chunk_size - 1)/chunk_size;
  
//  printf("size: %lu, %lu chunks\n", size, num_chunks);


  if(num_chunks > 1) {

    void *pivot = selectPivot(a, type, length);
    swap(&a[0],(char *) pivot, type->size); //swap pivot to beginning
    pivot = (void *) &a[0];

    CHUNK *chunks = (CHUNK *) malloc(num_chunks*sizeof(CHUNK));

//    printf("\nPivot: %d\n", a[0]);

    parallelPartition(&a[type->size], &b[type->size], type, length-1, chunks, num_chunks, pivot);

    size_t size_left = chunks[num_chunks-1].prefix_le;
    size_t size_right = length - size_left - 1;

//    printf("Split at %lu\n", size_left);

    free(chunks);


    //move pivot in place...
    copy(&b[size_left*type->size], &b[0], type->size);
//    b[0] = b[size_left];
    copy(&a[0], &b[size_left*type->size], type->size);
//    b[size_left] = a[0];
    copy(&a[0], &a[size_left*type->size], type->size);
//    a[size_left] = a[0];


//    print_ints(b, size);

    //if(size_left > 0) {
      //cilk_spawn parallelQuicksort(b, a, size_left, chunk_size, depth+1); //balance..., check size before, choose which?
      if(size_left > chunk_size) {
        cilk_spawn qs(b, a, type, size_left, chunk_size, depth+1);
      }
      else {
        qs(b, a, type, size_left, chunk_size, depth+1);
      }
    //}
    //if(size_right > 0) {
      if(size_right > chunk_size) {
        cilk_spawn qs(&b[(size_left+1)*type->size], &a[(size_left+1)*type->size], type, size_right, chunk_size, depth+1);
      }
      else {
        qs(&b[(size_left+1)*type->size], &a[(size_left+1)*type->size], type, size_right, chunk_size, depth+1);
      }


    //}

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
      void *pivot = selectPivot(a, type, length);
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
/*
      for(int i=0; i<length; i++) {
        b[i] = a[i];
      }
      qsort(b, length, sizeof(int), compare_ints);*/
    }
    else {
      qsort(a, length, type->size, type->compare);
    }
  }

}

void parallelQuicksort(void *array, ITYPE *type, size_t length, size_t chunk_size) {
    
    char *swapArray = malloc(length*type->size);

    qs((char *) array, swapArray, type, length, chunk_size, 0);    

    free(swapArray);

}
