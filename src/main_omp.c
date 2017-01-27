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
  uint32_t pivots;
  int useDouble;
  int longValidate;
};


void print_usage(void);

int get_options(int argc, char *argv[], struct _opts *options);

int main(int argc, char *argv[])
{
  size_t n;
  void *a;

  ITYPE type;

  double start, stop;
  double parTime;

  struct _opts opts;

  opts.threads = 0;
  opts.seed = 0;
  opts.distribution = RANDOM;
  opts.size = 1;
  opts.pivots = 11;
  opts.longValidate = FALSE;
  opts.useDouble = FALSE;

  if(!get_options(argc, argv, &opts)) {
    return 1;
  }

  opts.pivots = opts.pivots > 100? 99 : opts.pivots | 0x1;

  n = opts.size;
  omp_set_num_threads(opts.threads);

  int actualThreads;
  #pragma omp parallel default(none) shared(actualThreads)
  {
    #pragma omp single
    actualThreads = omp_get_num_threads();
  }

  printf("Number of threads %d, n=%lu, samples=%d, seed=%d\n", actualThreads, n, opts.pivots, opts.seed);


  if(opts.useDouble) {
    type.size = sizeof(double);
    type.compare = compare_doubles;
    type.print = print_double;
    a = generate_sequence(n, type.size, generate_double, opts.distribution, opts.seed);
  }
  else {
    type.size = sizeof(int);
    type.compare = compare_ints;
    type.print = print_int;
    a = generate_sequence(n, type.size, generate_uint, opts.distribution, opts.seed);
  }

  print_vals(a, &type, n);



//  void *buffer = malloc(n*type.size);
  
  omp_set_nested(1);  


  start = get_millis();//omp_get_wtime();

  //int *pivot = &a[n/2];

  quicksort_omp(a, &type, n, opts.pivots);


//  size_t split = test_partition(a, &type, n, (char *) buffer, pivot);

  stop = get_millis();//omp_get_wtime();

//  printf("Split at: %lu\n", split);
  print_vals(a, &type, n);

  parTime = stop-start;
  printf("Parallel sort time %.3f ms\n", parTime);

  int ok = 0;
  printf("Validating...\n");
  if(opts.longValidate) {

    void *copy;

    if(opts.useDouble) {
      copy = generate_sequence(n, type.size, generate_double, opts.distribution, opts.seed);
    }
    else {
      copy = generate_sequence(n, type.size, generate_uint, opts.distribution, opts.seed);
    }

    start = get_millis();
    qsort(copy, n, type.size, type.compare);
    stop = get_millis();

    printf("SeqQSort time %.3f ms\n",stop-start);

    print_vals(copy, &type, n);

    size_t pos = 0;

    if(!arraysEqual(a, copy, n, type.size, type.compare, &pos)) {
      pos = pos/type.size;
      printf("Sorting error! At position %lu \n", pos);
    }
    else {
      ok = TRUE;
    }
    free(copy);
  }
  else {
    if(!isSorted(a, &type, n)) {
      printf("Sorting error! \n");
    }
    else {
      ok = TRUE;
    }
  }

  free(a);

  printf("Stats: threads=%d, n=%lu, samples=%d, type=%s, seed=%d, time=%.3f, status=%s\n",actualThreads, n, opts.pivots,  opts.useDouble? "double" : "int", opts.seed, parTime, ok? "OK" : "ERR");

  return 0;
}

void print_usage(void) {
  printf(" -n SIZE -s SEED -g DISTRIBUTION -t NUM_THREADS -p PIVOT_SAMPLES -d (=use doubles) \n");
}

int get_options(int argc, char *argv[], struct _opts *options) {

  int success = TRUE;
  int c;


  while ((c = getopt(argc, argv, "n:s:t:g:p:dv")) != -1) {
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
      case 'p':
        options->pivots = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 'd':
        options->useDouble = TRUE;
        break;
      case 'v':
        options->longValidate = TRUE;
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



