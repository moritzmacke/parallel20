#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <sys/time.h>


#ifdef SEQUENTIAL
#define cilk_spawn
#define cilk_sync
#else
// cilkplus libraries
#include <cilk/cilk.h>
#include <cilk/cilk_api.h>
#endif

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

static void print_usage(void);

static int get_options(int argc, char *argv[], struct _opts *options);

int main(int argc, char *argv[])
{
  size_t n;
  void *a;

  ITYPE type;

  double start, stop;
  double parTime;

  struct _opts opts;

//  opts.threads = 0;
  opts.seed = 0;
  opts.distribution = RANDOM;
  opts.cutoff = 1000;
  opts.size = 100000;
  opts.useDouble = FALSE;
  opts.longValidate = FALSE;

  if(!get_options(argc, argv, &opts)) {
    return 1;
  }

  n = opts.size;

//  printf("Number of threads %d, n=%d, seed=%d\n", threads, n, seed);

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

//  copy = malloc(n*type.size);
//  memcpy(copy, a, n*type.size);


  int w = 0;  

#ifndef SEQUENTIAL
//  __cilkrts_set_param("nworkers","");
  // check how many workers are available
  w = __cilkrts_get_nworkers();  
  printf("Total number of workers: %d\n",w);
#endif

  printf("Length: %lu (%s), Chunksize: %d\n", n, opts.useDouble? "doubles" : "ints", opts.cutoff);
  print_vals(a, &type, n);

  start = get_millis();

  parallelQuicksort(a, &type, n, opts.cutoff);

  stop = get_millis();

  print_vals(a, &type, n);

  parTime = stop-start;
  printf("ParQSort time %.3f ms\n", parTime);

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

  printf("Stats: threads=%d, n=%lu, chunk=%d, type=%s seed=%d, time=%.3f, status=%s\n", w, n, opts.cutoff, opts.useDouble? "double" : "int", opts.seed, parTime, ok? "OK" : "ERR");

  return 0;
}

void print_usage(void) {
  printf(" -n SIZE -s SEED -c CUTOFF -d (=use doubles)\n");
}

int get_options(int argc, char *argv[], struct _opts *options) {

  int success = TRUE;
  int c;


  while ((c = getopt(argc, argv, "n:s:c:g:dv")) != -1) {
    switch (c) {
      case 'n':
        options->size = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 's':
        options->seed = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 'c':
        options->cutoff = (uint32_t) strtol(optarg, NULL, 0);
        break;
      case 'g':
        options->distribution = (uint32_t) strtol(optarg, NULL, 0);
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

