#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <sys/time.h>

//#include <omp.h>

#include "generate.h"
#include "sort.h"
#include "validate.h"

#define CHUNK_SIZE 2048




struct _opts {
  uint32_t threads;
  uint32_t seed;
  uint32_t pattern;
  uint32_t size;
};


int compare_ints(const void *a, const void *b) {
  const int *i_a = (const int *) a;
  const int *i_b = (const int *) b;
  
  return *i_a - *i_b;
}

int compare_doubles(const void *a, const void *b) {
  const double *da = (const double *) a;
  const double *db = (const double *) b;
  
  return (*da > *db) - (*da < *db);
}

void print_int(void *value, char *target, size_t maxChars) {
  snprintf(target, maxChars, "%u", *((uint32_t *)value));
}

void print_double(void *value, char *target, size_t maxChars) {
  snprintf(target, maxChars, "%f", *((double *)value));
}

void print_vals(char *array, ITYPE *type, size_t length) {
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

/*
void quicksort(int a[], int n)
{
  int i, j;
  int aa;

  if (n<2) return;

  //printf("which: %d %d\n",omp_get_thread_num(),omp_get_num_threads());

  // partition
  int pivot = a[0]; // choose an element non-randomly...
  i = 0; j = n;
  for (;;) {
    while (++i<j&&a[i]<pivot); // has one advantage
    while (a[--j]>pivot);
    if (i>=j) break;
    aa = a[i]; a[i] = a[j]; a[j] = aa;
  }
  // swap pivot
  aa = a[0]; a[0] = a[j]; a[j] = aa;

#pragma omp task untied if (n>1000)
  quicksort(a,j);
#pragma omp task untied if (n>1000)
  quicksort(a+j+1,n-j-1);
  //#pragma omp taskwait
}
*/

//#define MICRO 1000000.0

void print_usage(void);

int get_options(int argc, char *argv[], struct _opts *options);

double get_millis();

int main(int argc, char *argv[])
{
  int n;
  void *a, *copy;

  int threads;

  ITYPE type;

  unsigned seed;

  double start, stop;

  struct _opts prog_options;

  prog_options.threads = 0;
  prog_options.seed = 0;
  prog_options.pattern = 0;
  prog_options.size = 1;

  if(!get_options(argc, argv, &prog_options)) {
    return 1;
  }

  n = prog_options.size;
  seed = prog_options.seed;
  threads = set_num_threads(prog_options.threads);

  printf("Number of threads %d, n=%d, seed=%d\n", threads, n, seed);


  type.size = sizeof(int);
  type.compare = compare_ints;
  type.print = print_int;

  a = generate_random(n, type.size, generate_uint, seed);
//  a = generate_random(prog_options.size, sizeof(int), generate_item, prog_options.seed, prog_options.pattern);

  copy = malloc(n*type.size);
  memcpy(copy, a, n*type.size);

  
/*

#pragma omp parallel
  {
#pragma omp single nowait
    quicksort(a,n);
#pragma omp taskwait
  }
  
*/

  print_vals(a, &type, n);

  start = get_millis();//omp_get_wtime();



  //int *pivot = &a[n/2];
  
  void *buffer = malloc(n*type.size);
  
  
  quicksort(a, &type, n, buffer);


//  size_t split = test_partition(a, &type, n, (char *) buffer, pivot);

  stop = get_millis();//omp_get_wtime();

//  printf("Split at: %lu\n", split);
  print_vals(a, &type, n);


  // verify
//  if(!validPartition(a, copy, n, sizeof(int), pivot, compare_ints)) {
  if(!isSorted(a, &type, n)) {
    printf("Result not sorted correctly.\n");
  }

  printf("Sorting time %.2f ms\n",(stop-start));

  

  free(buffer);
  free(a);
  free(copy);

  return 0;
}

void print_usage(void) {
  printf(" -n SIZE -s SEED -d DISTRIBUTION -t NUM_THREADS\n");
}

int get_options(int argc, char *argv[], struct _opts *options) {

  int success = TRUE;
  int c;


  while ((c = getopt(argc, argv, "n:s:t:p:")) != -1) {
    switch (c) {
      case 'n':
        options->size = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 's':
        options->seed = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 't':
        options->threads = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 'd':
        options->pattern = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case '?':
        success = FALSE;
        if (optopt == 'c') {
          printf("Option -%c requires an argument.\n", optopt);
        }
        else {
          printf("Unknown option '-%c'.\n", optopt);
        }
        print_usage();
        break;
      default:
        break;
    }
  }

  return success;

}

double get_millis() {
  struct timeval now;
  gettimeofday(&now,NULL);  //error checking?
  double t = now.tv_usec/1000.0;
  t += now.tv_sec*1000;
  return t;
}

