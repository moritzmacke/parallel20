#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <sys/time.h>

#include <omp.h>

#include "generate.h"
#include "sort.h"
#include "validate.h"


struct _opts {
  uint32_t threads;
  uint32_t cutoff;
  uint32_t seed;
  uint32_t distribution;
  uint32_t size;
  int useDouble;
  int longValidate;
};


void print_usage(void);

int get_options(int argc, char *argv[], struct _opts *options);

int main(int argc, char *argv[])
{
  int n;
  void *a, *copy;

  ITYPE type;

  unsigned seed;

  double start, stop;

  struct _opts prog_options;

  prog_options.threads = 0;
  prog_options.seed = 0;
  prog_options.distribution = 0;
  prog_options.size = 1;
  prog_options.longValidate = TRUE;

  if(!get_options(argc, argv, &prog_options)) {
    return 1;
  }

  n = prog_options.size;
  seed = prog_options.seed;
  omp_set_num_threads(prog_options.threads);

  printf("Number of threads %d, n=%d, seed=%d\n", prog_options.threads, n, seed);


  type.size = sizeof(int);
  type.compare = compare_ints;
  type.print = print_int;

  a = generate_sequence(n, type.size, generate_uint, RANDOM, seed);
//  a = generate_random(prog_options.size, sizeof(int), generate_item, prog_options.seed, prog_options.pattern);

  copy = malloc(n*type.size);
  memcpy(copy, a, n*type.size);

  print_vals(a, &type, n);



//  void *buffer = malloc(n*type.size);
  
  omp_set_nested(1);  


  start = get_millis();//omp_get_wtime();

  //int *pivot = &a[n/2];

  quicksort(a, &type, n);


//  size_t split = test_partition(a, &type, n, (char *) buffer, pivot);

  stop = get_millis();//omp_get_wtime();

//  printf("Split at: %lu\n", split);
  print_vals(a, &type, n);


  printf("Parallel sort time %.3f ms\n",(stop-start));

  printf("Validating...\n");
  if(prog_options.longValidate) {
    start = get_millis();
    qsort(copy, n, type.size, type.compare);
    stop = get_millis();

    printf("SeqQSort time %.3f ms\n",stop-start);
    print_vals(copy, &type, n);

    size_t pos = 0;

    if(!arraysEqual(a, copy, n, type.size, type.compare, &pos)) {
      pos = pos/type.size;
      printf("Sorting error! At position %lu \n", pos);
//    printf("%u %u %u\n", pos > 0 ? a[pos-1] : -1, a[pos], a[pos+1]);
//    printf("%u %u %u\n", pos > 0 ? c[pos-1] : -1, c[pos], c[pos+1]);
    }
    else {
      printf("OK\n");
    }
  }
  else {
    if(!isSorted(a, &type, n)) {
      printf("Sorting error! \n");
    }
    else {
      printf("OK\n");
    }
  }

//  free(buffer);
  free(a);
  free(copy);

  return 0;
}

void print_usage(void) {
  printf(" -n SIZE -s SEED -g DISTRIBUTION -t NUM_THREADS\n");
}

int get_options(int argc, char *argv[], struct _opts *options) {

  int success = TRUE;
  int c;


  while ((c = getopt(argc, argv, "n:s:t:g:")) != -1) {
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
      case 'g':
        options->distribution = (uint32_t) strtol(optarg, NULL, 0);
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



