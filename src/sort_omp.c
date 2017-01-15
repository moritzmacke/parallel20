#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <omp.h>

#include "sort.h"

typedef struct _chunk {
  int thread_id;
  int thread_id2;
  size_t startIndex;
  size_t count_total;
  size_t count_le;
//  size_t count_gt;
//  size_t count_eq;
} CHUNK;

void copy(char *a, char *b, size_t size) {
  char *e = &a[size];
  while(a < e) {
    *b++ = *a++;
  }
}

void swap(char *a, char *b, size_t size) {

  while(size > 0) {
    char tmp = *a;
    *a++ = *b;
    *b++ = tmp;
    size--;
  }
}

static int num_threads = 1;

int set_num_threads(int threads) {
  if (threads>0) {
    num_threads = (threads > omp_get_max_threads()) ? omp_get_max_threads() : threads;
  }

  omp_set_num_threads(num_threads);

  return num_threads;
}


void print_ints0(int items[], int len) {
  if(len <= 100 && len > 0) {
    int i;
    for(i=0; (i+1)<len; i++) {
      printf("%d, ",items[i]);
    }
    printf("%d\n",items[i]);
  }
}

size_t test_partition(void *input, ITYPE *type, size_t length, void *buffer, void *pivot) {

//  printf("Pivot: %d\n", *((int *) pivot));

  int numThreads;
  size_t step = type->size;
  
  CHUNK *chunks;
//  size_t chunks;// = (item_count+CHUNK_SIZE-1)/CHUNK_SIZE;
  size_t itemsPerChunk;

  size_t split = 0;
  
#pragma omp parallel
{
  
#pragma omp single
  {
    numThreads = omp_get_num_threads();
//    printf("%d threads in partition, length: %lu\n", numThreads, length);
    itemsPerChunk = (length)/numThreads;  //last chunk larger...
    chunks = (CHUNK *) malloc(numThreads*sizeof(CHUNK));
  }

  if(length > numThreads) { //shouldn't call it anyway otherwise, but so it won't fail...
  
    int tid = omp_get_thread_num();

    CHUNK *c = &chunks[tid];

    c->startIndex = tid*itemsPerChunk;

    size_t chunklen = (tid == numThreads-1) ? length - tid*itemsPerChunk : itemsPerChunk;
    c->count_total = chunklen;

//    printf("Thread %d has [%lu:%lu] \n", tid, c->startIndex, c->startIndex + chunklen);

    char *src = ((char *) input) + c->startIndex*step;
    char *end = src + chunklen*step;

    char *tgt_le = ((char *) buffer) + tid*itemsPerChunk*step;
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
  
    c->count_le = countLE;
    c->thread_id = tid;
  
#pragma omp barrier
  
    size_t le_offset = 0;
    size_t gt_offset = 0;

    for(int i=0; i<numThreads; i++) {
      //add sums..
      gt_offset += chunks[i].count_le;
      if(i<tid) {
        le_offset += chunks[i].count_le;
        gt_offset += chunks[i].count_total - chunks[i].count_le;
      }
    }
    
//    printf("LE: [%lu:%lu], GT: [%lu:%lu]\n", le_offset, le_offset+c->count_le, gt_offset, gt_offset + (c->count_total - c->count_le));


    char *src_le = ((char *) buffer) + c->startIndex*step;
    char *src_gt = tgt_gt;

    tgt_le = ((char *) input) + le_offset*step;
    tgt_gt = ((char *) input) + gt_offset*step;

    memcpy((void *) tgt_le, (void *) src_le, countLE*step);
    memcpy((void *) tgt_gt, (void *) src_gt, countGT*step);

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

//#pragma omp single
  
  /*
  for(i=0; i<threads; i++) {   
    printf("Chunk %lu [%lu->%lu] by %i/%i: %lu-%lu-%lu\n", i, chunkstats[i].start, chunkstats[i].end, chunkstats[i].thread_id, chunkstats[i].thread_id2, chunkstats[i].smaller, chunkstats[i].equal, chunkstats[i].bigger);
  }
  */
  
  free(chunks);

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

void quicksort(void *array, ITYPE *type, size_t length, void *buffer) {

  if(length < 2) {
    return;
  }

  if(length < 1000/*CUTOFF*/) {
    qsort(array, length, type->size, type->compare);
    return;
  }

  void *pivot = selectPivot(array, type, length);

  swap((char *) array, (char *) pivot, type->size);
  pivot = array;

//  printf("Before   : ");
//  print_ints0((int *) array, length);
  size_t left_size = test_partition(((char *) array) + type->size, type, length-1, ((char *) buffer) + type->size, pivot);
//  printf("Split %3lu: ", left_size);
//  print_ints0((int *) array, length);

  swap((char *) pivot, ((char *) array) + left_size*type->size , type->size);



//#pragma omp task 
//{
    quicksort(array, type, left_size, buffer);
//}

//#pragma omp task 
//{
    quicksort(((char *) array) + (left_size+1)*type->size, type, length - left_size -1, ((char *) buffer) + (left_size+1)*type->size);
//}
  
  
}
