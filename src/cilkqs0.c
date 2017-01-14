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
  size_t startIndex;
  size_t count_le;
  size_t prefix_le;
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
void parallelPartitionPass1(int items[], int swaps[], int start, int size, CHUNK chunks[], int range, int *pivot) {
  
  if(range > 1) {
    cilk_spawn parallelPartitionPass1(items, swaps, start, size/2, chunks, range/2, pivot);
    cilk_spawn parallelPartitionPass1(items, swaps, start + size/2, size-size/2, &chunks[range/2], range-range/2, pivot);
    
    cilk_sync;

    chunks[range-1].prefix_le += chunks[range/2 - 1].prefix_le;


    
  }
  else {

    size_t count_le = 0;
    int *src = &items[start];
    int *end = &src[size];
    int *t_le = &swaps[start];
    int *t_gt = &t_le[size-1];
    while(src < end) {
      if(*src <= *pivot) {
//        printf("Move le: %d to %lu\n", *src, t_le);
        *t_le++ = *src++;
        count_le++;
      }
      else {
//        printf("Move gt: %d to %lu\n", *src, t_gt);
        *t_gt-- = *src++;
      }
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
void parallelPartitionPass2(int items[], int swaps[], size_t total_le, CHUNK *chunks, int range, size_t left_sum_le) {

  if(range > 1) {
    chunks[range/2 - 1].prefix_le += left_sum_le;

    cilk_spawn parallelPartitionPass2(items, swaps, total_le, chunks, range/2, left_sum_le);
    cilk_spawn parallelPartitionPass2(items, swaps, total_le, &chunks[range/2], range-(range/2), chunks[range/2 - 1].prefix_le);
  }
  else {
    //do partition
    CHUNK *c = &chunks[0];
  
    size_t num_le = c->count_le;
    size_t num_gt = c->count_total-num_le;
    size_t off_le = c->prefix_le - num_le; //le before this chunk...
    size_t off_gt = c->startIndex - off_le;

    int *s_le = &swaps[c->startIndex];
    int *s_gt = &s_le[num_le];


    int *t_le = &items[off_le];
    int *t_gt = &items[total_le + off_gt];

//    printf("move %lu le from %lu to %lu\n", num_le, c->startIndex, off_le);
    memcpy((void *) t_le, (void *) s_le, num_le*sizeof(int));
//    printf("move %lu gt from %lu to %lu\n", num_gt, c->startIndex + num_le, off_gt);
    memcpy((void *) t_gt, (void *) s_gt, num_gt*sizeof(int));

  }
}

void parallelPartition(int items[], int swaps[], size_t size, CHUNK chunks[], size_t num_chunks, int *pivot) {


  parallelPartitionPass1(items, swaps, 0, size, chunks, num_chunks, pivot);
//  print_ints(swaps, size);

  size_t total_le = chunks[num_chunks-1].prefix_le;
//  printf("total_le: %lu\n", total_le);
  

  parallelPartitionPass2(items, swaps, total_le, chunks, num_chunks, 0);

//  print_chunks(chunks, num_chunks);

}

int *selectPivot(int items[], int length) {
  int mid = length/2;
  int last = length - 1;
  int *pivot = &items[0];

//  printf("Pivot candidates: %d, %d, %d\n", items[0], items[mid], items[last]);

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

//  printf("Chose: %d\n", *pivot);

  return pivot;
}

void parallelQuicksort(int items[], int swaps[], size_t size, size_t chunk_size) {

  size_t num_chunks = (size + chunk_size - 1)/chunk_size;
  
//  printf("size: %lu, %lu chunks\n", size, num_chunks);

//  sleep(1);

  if(num_chunks > 1) {

//    printf("\n");
//    print_ints(items, size);

    int *pivot = selectPivot(items, size);
    swap((char *) &items[0],(char *) pivot, sizeof(int)); //swap pivot to beginning
    pivot = &items[0];

    //printf("Pivot: %d\n", *pivot);

    CHUNK *chunks = (CHUNK *) malloc(num_chunks*sizeof(CHUNK));

    parallelPartition(&items[1], &swaps[1], size-1, chunks, num_chunks, pivot);

    size_t size_left = chunks[num_chunks-1].prefix_le;
    size_t size_right = size - size_left - 1;

//    printf("Split at %lu\n", size_left);

    free(chunks);

    swap((char *) pivot,(char *) &items[size_left], sizeof(int)); //swap pivot in place

//    print_ints(items, size);

    //if(size_left > 0) {
      //cilk_spawn parallelQuicksort(b, a, size_left, chunk_size, depth+1); //balance..., check size before, choose which?
      if(size_left > chunk_size) {
        cilk_spawn parallelQuicksort(items, swaps, size_left, chunk_size);
      }
      else {
        parallelQuicksort(items, swaps, size_left, chunk_size);
      }
    //}
    //if(size_right > 0) {
      if(size_right > chunk_size) {
        cilk_spawn parallelQuicksort(&items[size_left+1], &swaps[size_left+1], size_right, chunk_size);
      }
      else {
        parallelQuicksort(&items[size_left+1], &swaps[size_left+1], size_right, chunk_size);
      }


    //}

  }
  else if(num_chunks != 0) {
    qsort(items, size, sizeof(int), compare_ints);
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

  parallelQuicksort(a, b, n, cutoff);

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
//    printf("%u %u %u\n", b[pos-1], b[pos], b[pos+1]);
    printf("%u %u %u\n", pos > 0 ? c[pos-1] : -1, c[pos], c[pos+1]);
  }

  free(a);
  free(b);
  free(c);

  return 0;
}
