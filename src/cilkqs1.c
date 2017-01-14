#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <sys/time.h>

#ifdef SEQUENTIAL
#define cilk_spawn
#define cilk_sync
#else
// cilkplus libraries
#include <cilk/cilk.h>
#include <cilk/cilk_api.h>
#endif


typedef struct _chunk {
  int *baseArray;
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


double get_millis() {
  struct timeval now;
  gettimeofday(&now, NULL);  //error checking?
  double t = now.tv_usec/1000.0;
  t += now.tv_sec*1000;
  return t;
}

int compare_ints(const void *a, const void *b) {
  const int *i_a = (const int *) a;
  const int *i_b = (const int *) b;
  
  return *i_a - *i_b;
}

void print_ints(int items[], int len) {
  if(len <= 100 && len > 0) {
    int i;
    for(i=0; (i+1)<len; i++) {
      printf("%d, ",items[i]);
    }
    printf("%d\n",items[i]);
  }
}

int same(void *_a, void *_b, size_t len, size_t *pos) {
  char *a = (char *) _a;
  char *b = (char *) _b;

  while(len > 0 && *a++ == *b++) {
    len--;
  }

  *pos = a - (char *) _a;

  return len == 0;
}

void swap(char *a, char *b, size_t size) {

  while(size > 0) {
    char tmp = *a;
    *a++ = *b;
    *b++ = tmp;
    size--;
  }
}


//
void parallelPartitionPass1(int items[], int start, int size, CHUNK chunks[], int range, int *pivot) {
  
  if(range > 1) {
    cilk_spawn parallelPartitionPass1(items, start, size/2, chunks, range/2, pivot);
    cilk_spawn parallelPartitionPass1(items, start + size/2, size-size/2, &chunks[range/2], range-range/2, pivot);
    
    cilk_sync;

    chunks[range-1].prefix_le += chunks[range/2 - 1].prefix_le;


    
  }
  else {

    size_t count_le = 0;
    int *p = &items[start];
    int *end = &p[size];
    while(p < end) {
      count_le += (*p <= *pivot);
      p++;
    }
    
    CHUNK *c = &chunks[0];

    //init chunk..
    c->baseArray = items;
    c->startIndex = start;
    c->count_total = size;
    c->count_le = count_le;
    c->prefix_le = count_le;


  }
}

//
void parallelPartitionPass2(int out[], size_t total_le, CHUNK *chunks, int range, size_t left_sum_le, int *pivot) {

  if(range > 1) {
    chunks[range/2 - 1].prefix_le += left_sum_le;

    cilk_spawn parallelPartitionPass2(out, total_le, chunks, range/2, left_sum_le, pivot);
    cilk_spawn parallelPartitionPass2(out, total_le, &chunks[range/2], range-(range/2), chunks[range/2 - 1].prefix_le, pivot);
  }
  else {
    //do partition
    CHUNK *c = &chunks[0];
  
    int *src = &c->baseArray[c->startIndex];
    int *end = &src[c->count_total];
    size_t off_le = c->prefix_le - c->count_le; //le before this chunk...
    size_t num_gt = c->startIndex - off_le;
    int *t_le = &out[off_le];
    int *t_gt = &out[total_le + num_gt];

    while(src < end) {
      if(*src <= *pivot) {
        *t_le++ = *src;
      }
      else {
        *t_gt++ = *src;
      }
      src++;
    }
  }
}

void parallelPartition(int in[], int out[], size_t size, CHUNK chunks[], size_t num_chunks, int *pivot) {


  parallelPartitionPass1(in, 0, size, chunks, num_chunks, pivot);

  size_t total_le = chunks[num_chunks-1].prefix_le;
//  printf("total_le: %lu\n", total_le);
  

  parallelPartitionPass2(out, total_le, chunks, num_chunks, 0, pivot);

//  print_chunks(chunks, num_chunks);

}

int *selectPivot(int items[], int length) {
  int mid = length/2;
  int last = length - 1;
  int *pivot = &items[0];

  if(items[0] > items[last]) {
    //first > last
    if(items[0] > items[mid]) {
      //first > last, mid
      if(items[mid] > items[last]) {
        //first > mid > last
        pivot = &items[mid];
      }
      else {
        //first > last >= mid
        pivot = &items[last];
      }
    }
    else {
      //mid >= first > last
    }
  }
  else {
    //last >= first
    if(items[0] > items[mid]) {
      //last >= first > mid
    }
    else {
      //last, mid >= first
      if(items[last] > items[mid]) {
        //last > mid >= first
        pivot = &items[mid];
      }
      else {
        //mid >= last >= first
        pivot = &items[last];
      }
    }
  }

  return pivot;
}

void parallelQuicksort(int a[], int b[], size_t size, size_t chunk_size, int depth) {

  size_t num_chunks = (size + chunk_size - 1)/chunk_size;
  
//  printf("size: %lu, %lu chunks\n", size, num_chunks);

//  sleep(1);

  if(num_chunks > 1) {

    int *pivot = selectPivot(a, size);
    swap((char *) &a[0],(char *) pivot, sizeof(int)); //swap pivot to beginning

    CHUNK *chunks = (CHUNK *) malloc(num_chunks*sizeof(CHUNK));

//    printf("\nPivot: %d\n", a[0]);
//    print_ints(a, size);
//    printf("%lu %lu", a, b);

    parallelPartition(&a[1], &b[1], size-1, chunks, num_chunks, &a[0]);

    size_t size_left = chunks[num_chunks-1].prefix_le;
    size_t size_right = size - size_left - 1;

//    printf("Split at %lu\n", size_left);

    free(chunks);

    //move pivot in place...
    b[0] = b[size_left];
    b[size_left] = a[0];
    a[size_left] = a[0];

//    print_ints(b, size);

    //if(size_left > 0) {
      //cilk_spawn parallelQuicksort(b, a, size_left, chunk_size, depth+1); //balance..., check size before, choose which?
      if(size_left > chunk_size) {
        cilk_spawn parallelQuicksort(b, a, size_left, chunk_size, depth+1);
      }
      else {
        parallelQuicksort(b, a, size_left, chunk_size, depth+1);
      }
    //}
    //if(size_right > 0) {
      if(size_right > chunk_size) {
        cilk_spawn parallelQuicksort(&b[size_left+1], &a[size_left+1], size_right, chunk_size, depth+1);
      }
      else {
        parallelQuicksort(&b[size_left+1], &a[size_left+1], size_right, chunk_size, depth+1);
      }


    //}

  }
  else if(num_chunks != 0) {
    
    if(depth%2 == 1) {
      //move to original array...
      int *src = a;
      int *end = &a[size];
      int *tl = b;
      int *tr = &b[size-1];
      int pivot = a[size/2];
      while(src < end) {
        if(*src <= pivot) {
          *tl++ = *src;
        }
        else {
          *tr-- = *src;
        }
        src++;
      }

      size_t size_left = tl - b;

      qsort(b, size_left , sizeof(int), compare_ints);
      qsort(tl, size-size_left, sizeof(int), compare_ints);
/*
      for(int i=0; i<size; i++) {
        b[i] = a[i];
      }
      qsort(b, size, sizeof(int), compare_ints);*/
    }
    else {
      qsort(a, size, sizeof(int), compare_ints);
    }
  }

}

int main(int argc, char *argv[])
{
  int i, n;
  int *a;

  int seed;

  double start, stop;

  n = 1;
  seed = 0;
  int cutoff = 1000;
  for (i=1; i<argc&&argv[i][0]=='-'; i++) {
    if (argv[i][1]=='n') i++,sscanf(argv[i],"%d",&n);
    if (argv[i][1]=='s') i++,sscanf(argv[i],"%d",&seed);
    if (argv[i][1]=='c') i++,sscanf(argv[i],"%d",&cutoff);
  }

  a = (int*)malloc(n*sizeof(int));

  srand(seed);
  for (i=0; i<n; i++) { a[i] = rand()%n; }


  int *b = (int *) malloc(n*sizeof(int));
  
  int *c = (int *) malloc(n*sizeof(int));
  memcpy(c, a, n*sizeof(int));

#ifndef SEQUENTIAL
//  __cilkrts_set_param("nworkers","");
  // check how many workers are available
  int w = __cilkrts_get_nworkers();  
  printf("Total number of workers: %d\n",w);
#endif

  print_ints(a, n);


  start = get_millis();
  parallelQuicksort(a, b, n, cutoff, 0);
  stop = get_millis();

  printf("ParQSort time %.3f ms\n",stop-start);

  print_ints(a, n);

  start = get_millis();
  qsort(c, n, sizeof(int), compare_ints);
  stop = get_millis();
  printf("SeqQSort time %.3f ms\n",stop-start);
  print_ints(c, n);

  // verify
//  for (i=0; i<n-1; i++) assert(a[i]<=a[i+1]);

  size_t pos = 0;

  if(!same(a, c, sizeof(int)*n, &pos)) {
    pos = pos/sizeof(int);
    printf("Sorting error! At %lu \n", pos);
    printf("%u %u %u\n", pos > 0 ? a[pos-1] : -1, a[pos], a[pos+1]);
    printf("%u %u %u\n", pos > 0 ? c[pos-1] : -1, c[pos], c[pos+1]);
  }

  free(a);
  free(b);
  free(c);

  return 0;
}
